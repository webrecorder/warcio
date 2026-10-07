r"""
# DecompressingBufferedReader Tests
#=================================================================

# decompress with on the fly compression, default gzip compression
>>> print_str(DecompressingBufferedReader(BytesIO(compress('ABC\n1234\n'))).read())
'ABC\n1234\n'

# decompress with on the fly compression, default 'inflate' compression
>>> print_str(DecompressingBufferedReader(BytesIO(compress_alt('ABC\n1234\n')), decomp_type='deflate').read())
'ABC\n1234\n'

# error: invalid compress type
>>> DecompressingBufferedReader(BytesIO(compress('ABC')), decomp_type = 'bzip2').read()
Traceback (most recent call last):
Exception: Decompression type not supported: bzip2

# invalid output when reading compressed data as not compressed
>>> DecompressingBufferedReader(BytesIO(compress('ABC')), decomp_type = None).read() != b'ABC'
True


# test very small block size
>>> dbr = DecompressingBufferedReader(BytesIO(b'ABCDEFG\nHIJKLMN\nOPQR\nXYZ'), block_size = 3)
>>> print_str(dbr.readline()); print_str(dbr.readline(4)); print_str(dbr.readline()); print_str(dbr.readline()); print_str(dbr.readline(2)); print_str(dbr.readline()); print_str(dbr.readline())
'ABCDEFG\n'
'HIJK'
'LMN\n'
'OPQR\n'
'XY'
'Z'
''

# test zero length reads
>>> x = DecompressingBufferedReader(LimitReader(BytesIO(b'\r\n'), 1))
>>> print_str(x.readline(0)); print_str(x.read(0))
''
''

# Chunk-Decoding Buffered Reader Tests
#=================================================================

Properly formatted chunked data:
>>> c = ChunkedDataReader(BytesIO(b"4\r\n1234\r\n0\r\n\r\n"));
>>> print_str(c.read() + c.read(1) + c.read() + c.read())
'1234'

Non-chunked data:
>>> print_str(ChunkedDataReader(BytesIO(b"xyz123!@#")).read())
'xyz123!@#'

Non-chunked data, numbers only:
>>> print_str(ChunkedDataReader(BytesIO(b"ABCDE" * 10)).read())
'ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDE'

Non-chunked data, numbers new line, large:
>>> print_str(ChunkedDataReader(BytesIO(b"ABCDE" * 10 + b'\r\n')).read())
'ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDE\r\n'

Non-chunked, compressed data, specify decomp_type
>>> print_str(ChunkedDataReader(BytesIO(compress('ABCDEF')), decomp_type='gzip').read())
'ABCDEF'

Non-chunked, compressed data, specify compression separately
>>> c = ChunkedDataReader(BytesIO(compress('ABCDEF'))); c.set_decomp('gzip'); print_str(c.read())
'ABCDEF'

Non-chunked, compressed data, wrap in DecompressingBufferedReader
>>> print_str(DecompressingBufferedReader(ChunkedDataReader(BytesIO(compress('\nABCDEF\nGHIJ')))).read())
'\nABCDEF\nGHIJ'

Chunked compressed data
Split compressed stream into 10-byte chunk and a remainder chunk
>>> b = compress('ABCDEFGHIJKLMNOP')
>>> l = len(b)
>>> in_ = format(10, 'x').encode('utf-8') + b"\r\n" + b[:10] + b"\r\n" + format(l - 10, 'x').encode('utf-8') + b"\r\n" + b[10:] + b"\r\n0\r\n\r\n"
>>> c = ChunkedDataReader(BytesIO(in_), decomp_type='gzip')
>>> print_str(c.read())
'ABCDEFGHIJKLMNOP'

Starts like chunked data, but isn't:
>>> c = ChunkedDataReader(BytesIO(b"1\r\nxyz123!@#"));
>>> print_str(c.read() + c.read())
'1\r\nx123!@#'

Chunked data cut off part way through:
>>> c = ChunkedDataReader(BytesIO(b"4\r\n1234\r\n4\r\n12"));
>>> print_str(c.read() + c.read())
'123412'

Zero-Length chunk:
>>> print_str(ChunkedDataReader(BytesIO(b"0\r\n\r\n")).read())
''

"""

from io import BytesIO
from warcio.bufferedreaders import ChunkedDataReader, ChunkedDataException
from warcio.bufferedreaders import DecompressingBufferedReader
from warcio.limitreader import LimitReader

from contextlib import closing

import six

import zlib
import pytest


def compress(buff):
    buff = buff.encode('utf-8')
    compressobj = zlib.compressobj(6, zlib.DEFLATED, zlib.MAX_WBITS + 16)
    compressed = compressobj.compress(buff)
    compressed += compressobj.flush()

    return compressed

# plain "inflate"
def compress_alt(buff):
    buff = buff.encode('utf-8')
    compressobj = zlib.compressobj(6, zlib.DEFLATED)
    compressed = compressobj.compress(buff)
    compressed += compressobj.flush()
    # drop gzip headers/tail
    compressed = compressed[2:-4]

    return compressed

# Brotli
@pytest.mark.skipif('br' not in DecompressingBufferedReader.DECOMPRESSORS, reason='brotli not available')
def test_brotli():
    brotli_buff = b'[\xff\xaf\x02\xc0"y\\\xfbZ\x8cB;\xf4%U\x19Z\x92\x99\xb15\xc8\x19\x9e\x9e\n{K\x90\xb9<\x98\xc8\t@\xf3\xe6\xd9M\xe4me\x1b\'\x87\x13_\xa6\xe90\x96{<\x15\xd8S\x1c'

    with closing(DecompressingBufferedReader(BytesIO(brotli_buff), decomp_type='br')) as x:
        assert x.read() == b'The quick brown fox jumps over the lazy dog' * 4096


@pytest.mark.skipif('br' not in DecompressingBufferedReader.DECOMPRESSORS, reason='brotli not available')
def test_brotli_very_small_chunk():
    brotli_buff = b'[\xff\xaf\x02\xc0"y\\\xfbZ\x8cB;\xf4%U\x19Z\x92\x99\xb15\xc8\x19\x9e\x9e\n{K\x90\xb9<\x98\xc8\t@\xf3\xe6\xd9M\xe4me\x1b\'\x87\x13_\xa6\xe90\x96{<\x15\xd8S\x1c'

    # read 3 bytes at time, will need to read() multiple types before decompressor has enough to return something
    with closing(DecompressingBufferedReader(BytesIO(brotli_buff), decomp_type='br', block_size=3)) as x:
        assert x.read() == b'The quick brown fox jumps over the lazy dog' * 4096


@pytest.mark.skipif('br' not in DecompressingBufferedReader.DECOMPRESSORS, reason='brotli not available')
def test_brotli_chunked():
    brotli_buff = b'[\xff\xaf\x02\xc0"y\\\xfbZ\x8cB;\xf4%U\x19Z\x92\x99\xb15\xc8\x19\x9e\x9e\n{K\x90\xb9<\x98\xc8\t@\xf3\xe6\xd9M\xe4me\x1b\'\x87\x13_\xa6\xe90\x96{<\x15\xd8S\x1c'
    chunked = b''.join(format(len(chunk), 'x').encode('utf-8') + b'\r\n' + chunk + b'\r\n'
                       for chunk in (brotli_buff[:10], brotli_buff[10:]))
    chunked += b'0\r\n\r\n'

    with closing(ChunkedDataReader(BytesIO(chunked), decomp_type='br')) as x:
        assert x.read() == b'The quick brown fox jumps over the lazy dog' * 4096


# Zstandard
# frames compressed in streaming mode, without the content size in the frame header
ZSTD_BUFF = b'(\xb5/\xfd\x00X\xac\x01\x00\xb4\x02The quick brown fox jumps over the lazy dog\x01\x00\x93\xfew\xb9*\x03M\x00\x00\x08k\x01\x00\xfc/\x1d\x08\x01'
ZSTD_FRAME_1 = b'(\xb5/\xfd\x00XI\x00\x00ABC\n1234\n'
ZSTD_FRAME_2 = b'(\xb5/\xfd\x00X!\x00\x00XYZ\n'

zstd_available = pytest.mark.skipif('zstd' not in DecompressingBufferedReader.DECOMPRESSORS, reason='zstd not available')


@zstd_available
def test_zstd():
    with closing(DecompressingBufferedReader(BytesIO(ZSTD_BUFF), decomp_type='zstd')) as x:
        assert x.read() == b'The quick brown fox jumps over the lazy dog' * 4096


@zstd_available
def test_zstd_very_small_chunk():
    with closing(DecompressingBufferedReader(BytesIO(ZSTD_BUFF), decomp_type='zstd', block_size=3)) as x:
        assert x.read() == b'The quick brown fox jumps over the lazy dog' * 4096


@zstd_available
def test_zstd_chunked():
    chunked = b''.join(format(len(chunk), 'x').encode('utf-8') + b'\r\n' + chunk + b'\r\n'
                       for chunk in (ZSTD_BUFF[:10], ZSTD_BUFF[10:]))
    chunked += b'0\r\n\r\n'

    with closing(ChunkedDataReader(BytesIO(chunked), decomp_type='zstd')) as x:
        assert x.read() == b'The quick brown fox jumps over the lazy dog' * 4096


@zstd_available
@pytest.mark.parametrize('block_size', [3, len(ZSTD_FRAME_1), 1024])
def test_zstd_multiple_frames(block_size):
    # like gzip members, frames after the first one are read with read_next_member()
    x = DecompressingBufferedReader(BytesIO(ZSTD_FRAME_1 + ZSTD_FRAME_2), decomp_type='zstd', block_size=block_size)
    assert x.readline() == b'ABC\n'
    assert x.read() == b'1234\n'
    assert x.read_next_member()
    assert x.read() == b'XYZ\n'
    x.close()


@zstd_available
@pytest.mark.parametrize('block_size', [3, len(ZSTD_FRAME_1), 1024])
def test_zstd_multiple_frames_read_all_members(block_size):
    # as with gzip members, a read() may stop at the end of a frame
    with closing(DecompressingBufferedReader(BytesIO(ZSTD_FRAME_1 + ZSTD_FRAME_2), decomp_type='zstd',
                                             block_size=block_size, read_all_members=True)) as x:
        assert b''.join(iter(x.read, b'')) == b'ABC\n1234\nXYZ\n'


@zstd_available
def test_zstd_not_compressed():
    # if decompression fails on first block, data is assumed to not be compressed
    with closing(DecompressingBufferedReader(BytesIO(b'ABC\n1234\n'), decomp_type='zstd')) as x:
        assert x.read() == b'ABC\n1234\n'


# Compression
def test_compress_mix():
    x = DecompressingBufferedReader(BytesIO(compress('ABC') + b'123'), decomp_type = 'gzip')
    b = x.read()
    assert b == b'ABC'
    x.read_next_member()
    assert x.read() == b'123'


# Errors
def test_compress_invalid():
    result = compress('ABCDEFG' * 1)
    # cut-off part of the block
    result = result[:-2] + b'xyz'

    x = DecompressingBufferedReader(BytesIO(result), block_size=16)
    b = x.read(3)
    assert b == b'ABC'

    assert b'DE' == x.read()



def test_err_chunk_cut_off():
    # Chunked data cut off with exceptions
    c = ChunkedDataReader(BytesIO(b"4\r\n1234\r\n4\r\n12"), raise_exceptions=True)
    with pytest.raises(ChunkedDataException):
        c.read() + c.read()
    #ChunkedDataException: Ran out of data before end of chunk



def print_str(string):
    return string.decode('utf-8') if six.PY3 else string



if __name__ == "__main__":
    import doctest
    doctest.testmod()
