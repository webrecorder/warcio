from . import get_test_file
from warcio.recompressor import Recompressor, StreamRecompressor

import gzip
import pytest

def test_recompress_chunked(capsys,tmp_path):
    test_file = get_test_file('example-resource.warc.gz')
    tmp_file = tmp_path / "output.warc.gz"
    recompressor = Recompressor(test_file, str(tmp_file), verbose=True)
    recompressor.recompress()
    out, err = capsys.readouterr()
    assert len(out) > 0
    assert "Records successfully read and compressed" in out
    assert "3 records read and recompressed to file" in out
    assert "No Errors Found!" in out

def test_recompress_non_chunked(capsys,tmp_path):
    test_file = get_test_file('example-bad-non-chunked.warc.gz')
    tmp_file = tmp_path / "output.warc.gz"
    recompressor = Recompressor(test_file, str(tmp_file), verbose=True)
    recompressor.recompress()
    out, err = capsys.readouterr()
    assert len(out) > 0
    assert "ERROR: non-chunked gzip file detected" in out
    assert "Records successfully read and compressed" in out
    assert "6 records read and recompressed to file" in out
    assert "Compression Errors Found and Fixed!" in out

def test_recompress_chunked_decompressed_stream(tmp_path):
    """Open a stream with befor feeding it to the _load_and_write_stream method as stream."""
    test_file = get_test_file('example-resource.warc.gz')
    tmp_file = tmp_path / "output.warc.gz"
    recompressor = Recompressor(test_file, str(tmp_file), verbose=True)
    with open(test_file, "rb") as input, open(tmp_file, "wb") as output:
        count = recompressor._load_and_write_stream(input, output)
    assert count == 3

def test_recompress_non_chunked_decompressed_stream_fails(tmp_path):
    """Open a stream of a non chunked gzip befor feeding it to the _load_and_write_stream method as stream. Expect it to fail."""
    test_file = get_test_file('example-bad-non-chunked.warc.gz')
    tmp_file = tmp_path / "output.warc.gz"
    recompressor = Recompressor(test_file, str(tmp_file), verbose=True)
    with open(test_file, "rb") as input, open(tmp_file, "wb") as output:
        with pytest.raises(Exception):
            recompressor._load_and_write_stream(input, output)

def test_recompress_non_chunked_decompressed_stream(tmp_path):
    """Uncompress a stream with gzip befor feeding it to the _load_and_write_stream method as stream."""
    test_file = get_test_file('example-bad-non-chunked.warc.gz')
    tmp_file = tmp_path / "output.warc.gz"
    recompressor = Recompressor(test_file, str(tmp_file), verbose=True)
    with gzip.open(test_file, "rb") as input, open(tmp_file, "wb") as output:
        count = recompressor._load_and_write_stream(input, output)
    assert count == 6

def test_stream_recompress_non_chunked_decompressed_stream(tmp_path):
    """Uncompress a badly chunked stream with gzip befor feeding it to the StreamRecompressor's recompress method as stream."""
    test_file = get_test_file('example-bad-non-chunked.warc.gz')
    tmp_file = tmp_path / "output.warc.gz"
    with gzip.open(test_file, "rb") as input, open(tmp_file, "wb") as output:
        recompressor = StreamRecompressor(input, output, verbose=True)
        count = recompressor.recompress()
    assert count == 6

def test_stream_recompress_chunked_compressed_stream(tmp_path):
    """Open a chunked compressed stream, feeding it to the StreamRecompressor's recompress method as stream."""
    test_file = get_test_file('example-resource.warc.gz')
    tmp_file = tmp_path / "output.warc.gz"
    with open(test_file, "rb") as input, open(tmp_file, "wb") as output:
        recompressor = StreamRecompressor(input, output, verbose=True)
        count = recompressor.recompress()
    assert count == 3

def test_stream_decompress_recompress_non_chunked_compressed_stream(tmp_path):
    """Open a badly chunked stream and feed it to the StreamRecompressor's decompress_recompress method as stream."""
    test_file = get_test_file('example-bad-non-chunked.warc.gz')
    tmp_file = tmp_path / "output.warc.gz"
    with open(test_file, "rb") as input, open(tmp_file, "wb") as output:
        recompressor = StreamRecompressor(input, output, verbose=True)
        count = recompressor.decompress_recompress()
    assert count == 6

def test_stream_decompress_recompress_chunked_stream(tmp_path):
    """Open a chunked compressed stream and feed it to the StreamRecompressor's decompress_recompress method as stream."""
    test_file = get_test_file('example-resource.warc.gz')
    tmp_file = tmp_path / "output.warc.gz"
    with open(test_file, "rb") as input, open(tmp_file, "wb") as output:
        recompressor = StreamRecompressor(input, output, verbose=True)
        count = recompressor.decompress_recompress()
    assert count == 3
