import sys
import types
from io import BytesIO
from contextlib import closing

import pytest

from warcio.archiveiterator import ArchiveIterator
from warcio.bufferedreaders import BufferedReader, DecompressingBufferedReader
from warcio.bufferedreaders import _find_brotli, _register_decompressor, try_brotli_init
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter


BROTLI_BUFF = b'[\xff\xaf\x02\xc0"y\\\xfbZ\x8cB;\xf4%U\x19Z\x92\x99\xb15\xc8\x19\x9e\x9e\n{K\x90\xb9<\x98\xc8\t@\xf3\xe6\xd9M\xe4me\x1b\'\x87\x13_\xa6\xe90\x96{<\x15\xd8S\x1c'
BROTLI_TEXT = b'The quick brown fox jumps over the lazy dog' * 4096


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


# Decompressor registration
#=================================================================
def test_register_wraps_minimal_decompressor(decompressors):
    class MinimalDecompressor(object):
        def decompress(self, data):
            return data[::-1]

    _register_decompressor('X-Reverse', MinimalDecompressor)
    decompressor = decompressors['x-reverse']()

    assert decompressor.decompress(b'abc') == b'cba'
    assert decompressor.flush() == b''
    assert decompressor.unused_data is None

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
