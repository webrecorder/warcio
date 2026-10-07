import sys
import types
import zlib
from io import BytesIO
from contextlib import closing

import pytest

from warcio.archiveiterator import ArchiveIterator
from warcio.bufferedreaders import BufferedReader, DecompressingBufferedReader, gzip_decompressor
from warcio.bufferedreaders import _DecompressorAdapter, _register_decompressor
from warcio.bufferedreaders import _find_brotli, try_brotli_init, _find_zstd, try_zstd_init
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter


BROTLI_BUFF = b'[\xff\xaf\x02\xc0"y\\\xfbZ\x8cB;\xf4%U\x19Z\x92\x99\xb15\xc8\x19\x9e\x9e\n{K\x90\xb9<\x98\xc8\t@\xf3\xe6\xd9M\xe4me\x1b\'\x87\x13_\xa6\xe90\x96{<\x15\xd8S\x1c'
BROTLI_TEXT = b'The quick brown fox jumps over the lazy dog' * 4096
ZSTD_FRAME = b'(\xb5/\xfd\x00XI\x00\x00ABC\n1234\n'


# Fake brotli implementations, mirroring the API of each library
#=================================================================
class GoogleBrotliDecompressor(object):
    # Brotli: only process(), no flush() or unused_data
    def process(self, data):
        return data.upper()


class BrotliCFFIDecompressor(object):
    # brotlicffi: decompress() and flush(), no unused_data
    def decompress(self, data):
        return data.upper()

    def process(self, data):
        return data.upper()

    def flush(self):
        return b''


class BrotlipyDecompressor(object):
    # brotlipy: decompress() and flush(), no unused_data
    def decompress(self, data):
        return data.upper()

    def flush(self):
        return b''


# Fake zstd implementations, mirroring the API of each library
#=================================================================
class StdlibZstdDecompressor(object):
    # compression.zstd: decompress(), eof and unused_data, no flush()
    eof = False
    unused_data = b''

    def decompress(self, data):
        return data.upper()


class BackportsZstdDecompressor(StdlibZstdDecompressor):
    # backports.zstd: same API as compression.zstd
    pass


class ZstandardDecompressionObj(StdlibZstdDecompressor):
    # zstandard: ZstdDecompressor().decompressobj() also has flush()
    def flush(self):
        return b''


class ZstandardDecompressor(object):
    def decompressobj(self):
        return ZstandardDecompressionObj()


def fake_zstd_modules(monkeypatch, stdlib, backports, zstandard):
    # mock 'from compression import zstd', 'from backports import zstd' and 'import zstandard'
    for package, zstd_cls in (('compression', stdlib and StdlibZstdDecompressor),
                              ('backports', backports and BackportsZstdDecompressor)):
        if zstd_cls:
            zstd = types.ModuleType(package + '.zstd')
            zstd.ZstdDecompressor = zstd_cls
            package_module = types.ModuleType(package)
            package_module.zstd = zstd
        else:
            zstd = package_module = None

        monkeypatch.setitem(sys.modules, package, package_module)
        monkeypatch.setitem(sys.modules, package + '.zstd', zstd)

    zstandard_module = None
    if zstandard:
        zstandard_module = types.ModuleType('zstandard')
        zstandard_module.ZstdDecompressor = ZstandardDecompressor

    monkeypatch.setitem(sys.modules, 'zstandard', zstandard_module)


def fake_module(name, decompressor_cls):
    if not decompressor_cls:
        # importing a module set to None in sys.modules raises ImportError
        return None

    module = types.ModuleType(name)
    module.Decompressor = decompressor_cls
    return module


@pytest.fixture
def decompressors(monkeypatch):
    # restore the registered decompressors after each test
    monkeypatch.setattr(BufferedReader, 'DECOMPRESSORS', dict(BufferedReader.DECOMPRESSORS))
    return BufferedReader.DECOMPRESSORS


def compress(data):
    compressobj = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS + 16)
    return compressobj.compress(data) + compressobj.flush()


def unwrap(decompressor):
    return getattr(decompressor, '_decompressor', decompressor)


# Brotli implementation detection
#=================================================================
@pytest.mark.parametrize('brotli_cls, brotlicffi_cls, expected_cls', [
    (GoogleBrotliDecompressor, BrotliCFFIDecompressor, GoogleBrotliDecompressor),
    (GoogleBrotliDecompressor, None, GoogleBrotliDecompressor),
    (None, BrotliCFFIDecompressor, BrotliCFFIDecompressor),
    (BrotlipyDecompressor, BrotliCFFIDecompressor, BrotliCFFIDecompressor),
    (BrotlipyDecompressor, None, BrotlipyDecompressor),
])
def test_find_brotli_preference(monkeypatch, brotli_cls, brotlicffi_cls, expected_cls):
    monkeypatch.setitem(sys.modules, 'brotli', fake_module('brotli', brotli_cls))
    monkeypatch.setitem(sys.modules, 'brotlicffi', fake_module('brotlicffi', brotlicffi_cls))

    assert isinstance(unwrap(_find_brotli()()), expected_cls)


def test_brotli_not_installed(monkeypatch, decompressors):
    monkeypatch.setitem(sys.modules, 'brotli', None)
    monkeypatch.setitem(sys.modules, 'brotlicffi', None)
    decompressors.pop('br', None)

    assert _find_brotli() is None

    try_brotli_init()
    assert 'br' not in decompressors

    with pytest.raises(Exception, match='Decompression type not supported: br'):
        DecompressingBufferedReader(BytesIO(b'ABC'), decomp_type='br')


@pytest.mark.parametrize('brotli_cls', [GoogleBrotliDecompressor, BrotlipyDecompressor])
def test_brotli_read_and_close(monkeypatch, decompressors, brotli_cls):
    monkeypatch.setitem(sys.modules, 'brotli', fake_module('brotli', brotli_cls))
    monkeypatch.setitem(sys.modules, 'brotlicffi', None)
    try_brotli_init()

    x = DecompressingBufferedReader(BytesIO(b'abc\ndef'), decomp_type='br', block_size=3)
    assert x.readline() == b'ABC\n'
    assert x.read() == b'DEF'
    x.close()


# Zstd implementation detection
#=================================================================
@pytest.mark.parametrize('stdlib, backports, zstandard, expected_cls', [
    (True, True, True, StdlibZstdDecompressor),
    (False, True, True, BackportsZstdDecompressor),
    (False, False, True, ZstandardDecompressionObj),
])
def test_find_zstd_preference(monkeypatch, stdlib, backports, zstandard, expected_cls):
    fake_zstd_modules(monkeypatch, stdlib, backports, zstandard)

    decompressor = _find_zstd()()
    assert isinstance(decompressor, _DecompressorAdapter)
    assert type(unwrap(decompressor)) is expected_cls


def test_zstd_not_installed(monkeypatch, decompressors):
    fake_zstd_modules(monkeypatch, False, False, False)
    decompressors.pop('zstd', None)

    assert _find_zstd() is None

    try_zstd_init()
    assert 'zstd' not in decompressors

    with pytest.raises(Exception, match='Decompression type not supported: zstd'):
        DecompressingBufferedReader(BytesIO(b'ABC'), decomp_type='zstd')


@pytest.mark.parametrize('stdlib, backports, zstandard', [
    (True, False, False),
    (False, True, False),
    (False, False, True),
])
def test_zstd_read_and_close(monkeypatch, decompressors, stdlib, backports, zstandard):
    fake_zstd_modules(monkeypatch, stdlib, backports, zstandard)
    try_zstd_init()

    x = DecompressingBufferedReader(BytesIO(b'abc\ndef'), decomp_type='zstd', block_size=3)
    assert x.readline() == b'ABC\n'
    assert x.read() == b'DEF'
    x.close()


# Decompressor adapter and registration
#=================================================================
def test_adapter_keeps_data_after_eof():
    class OneFrameDecompressor(object):
        eof = False
        unused_data = b''

        def decompress(self, data):
            if self.eof:
                raise EOFError('Already at the end of a frame')

            self.eof = True
            return data

    decompressor = _DecompressorAdapter(OneFrameDecompressor())

    assert decompressor.decompress(b'abc') == b'abc'
    assert decompressor.decompress(b'def') == b''
    assert decompressor.decompress(b'ghi') == b''
    assert decompressor.unused_data == b'defghi'


def test_register_wraps_minimal_decompressor(decompressors):
    class MinimalDecompressor(object):
        def decompress(self, data):
            return data[::-1]

    _register_decompressor('X-Reverse', MinimalDecompressor)
    decompressor = decompressors['x-reverse']()

    assert decompressor.decompress(b'abc') == b'cba'
    assert decompressor.flush() == b''
    assert not decompressor.unused_data

    with closing(DecompressingBufferedReader(BytesIO(b'abc'), decomp_type='X-Reverse')) as x:
        assert x.read() == b'cba'


def test_register_keeps_complete_decompressor(decompressors):
    class CompleteDecompressor(object):
        unused_data = b''

        def decompress(self, data):
            return data

        def flush(self):
            return b''

    _register_decompressor('x-complete', CompleteDecompressor)

    assert isinstance(decompressors['x-complete'](), CompleteDecompressor)


def test_register_decompressor(decompressors):
    class ReverseDecompressor(object):
        def decompress(self, data):
            return data[::-1]

    BufferedReader.register_decompressor('X-Reverse', ReverseDecompressor)

    assert 'x-reverse' in BufferedReader.get_supported_decompressors()

    with closing(DecompressingBufferedReader(BytesIO(b'abc'), decomp_type='x-reverse')) as x:
        assert x.read() == b'cba'


def test_register_decompressor_replaces_existing(decompressors):
    class UpperDecompressor(object):
        def decompress(self, data):
            return data.upper()

    BufferedReader.register_decompressor('br', UpperDecompressor)

    with closing(DecompressingBufferedReader(BytesIO(b'abc'), decomp_type='br')) as x:
        assert x.read() == b'ABC'


# Content-Encoding of WARC response records
#=================================================================
def response_record(payload, content_encoding):
    output = BytesIO()
    writer = WARCWriter(output, gzip=False)
    http_headers = StatusAndHeaders('200 OK', [('Content-Type', 'text/plain'),
                                               ('Content-Encoding', content_encoding)],
                                    protocol='HTTP/1.1')

    record = writer.create_warc_record('http://example.com/', 'response',
                                       payload=BytesIO(payload),
                                       http_headers=http_headers)
    writer.write_record(record)
    output.seek(0)
    return next(ArchiveIterator(output))


@pytest.mark.skipif('br' not in BufferedReader.DECOMPRESSORS, reason='brotli not available')
def test_content_stream_brotli():
    record = response_record(BROTLI_BUFF, 'br')

    assert record.content_stream().read() == BROTLI_TEXT


def test_content_stream_unsupported_encoding(decompressors):
    decompressors.pop('br', None)
    record = response_record(BROTLI_BUFF, 'br')

    # returned as is, still compressed
    assert record.content_stream().read() == BROTLI_BUFF


@pytest.mark.skipif('zstd' not in BufferedReader.DECOMPRESSORS, reason='zstd not available')
def test_content_stream_zstd():
    record = response_record(ZSTD_FRAME, 'zstd')

    assert record.content_stream().read() == b'ABC\n1234\n'


def test_content_stream_registered_decompressor(decompressors):
    record = response_record(compress(b'ABC\n1234\n'), 'x-gzip')

    # unsupported, returned as is
    assert record.content_stream().read() == compress(b'ABC\n1234\n')

    BufferedReader.register_decompressor('x-gzip', gzip_decompressor)
    record = response_record(compress(b'ABC\n1234\n'), 'x-gzip')

    assert record.content_stream().read() == b'ABC\n1234\n'
