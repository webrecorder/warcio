from warcio.cli import main

from . import get_test_file

from contextlib import contextmanager
from io import BytesIO

from warcio.recordloader import ArchiveLoadFailed

import pytest
import sys
import tempfile
import os


def test_index():
    files = ['example.warc.gz', 'example.warc', 'example.arc.gz', 'example.arc']
    files = [get_test_file(filename) for filename in files]

    args = ['index', '-f', 'length,offset,warc-type,warc-target-uri,warc-filename,http:content-type']
    args.extend(files)

    expected = b"""\
{"length": "353", "offset": "0", "warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"length": "431", "offset": "353", "warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"length": "1228", "offset": "784", "warc-type": "response", "warc-target-uri": "http://example.com/", "http:content-type": "text/html"}
{"length": "526", "offset": "2012", "warc-type": "request", "warc-target-uri": "http://example.com/"}
{"length": "585", "offset": "2538", "warc-type": "revisit", "warc-target-uri": "http://example.com/", "http:content-type": "text/html"}
{"length": "526", "offset": "3123", "warc-type": "request", "warc-target-uri": "http://example.com/"}
{"length": "484", "offset": "0", "warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"length": "705", "offset": "488", "warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"length": "1365", "offset": "1197", "warc-type": "response", "warc-target-uri": "http://example.com/", "http:content-type": "text/html"}
{"length": "800", "offset": "2566", "warc-type": "request", "warc-target-uri": "http://example.com/"}
{"length": "942", "offset": "3370", "warc-type": "revisit", "warc-target-uri": "http://example.com/", "http:content-type": "text/html"}
{"length": "800", "offset": "4316", "warc-type": "request", "warc-target-uri": "http://example.com/"}
{"length": "171", "offset": "0", "warc-type": "warcinfo", "warc-filename": "live-web-example.arc.gz"}
{"length": "856", "offset": "171", "warc-type": "response", "warc-target-uri": "http://example.com/", "http:content-type": "text/html"}
{"length": "150", "offset": "0", "warc-type": "warcinfo", "warc-filename": "live-web-example.arc.gz"}
{"length": "1656", "offset": "151", "warc-type": "response", "warc-target-uri": "http://example.com/", "http:content-type": "text/html"}
"""
    with patch_stdout() as buff:
        res = main(args=args)
        assert buff.getvalue() == expected


def test_index_2():
    files = ['example.warc.gz']
    files = [get_test_file(filename) for filename in files]

    args = ['index', '-f', 'offset,length,http:status,warc-type,filename']
    args.extend(files)

    expected = b"""\
{"offset": "0", "length": "353", "warc-type": "warcinfo", "filename": "example.warc.gz"}
{"offset": "353", "length": "431", "warc-type": "warcinfo", "filename": "example.warc.gz"}
{"offset": "784", "length": "1228", "http:status": "200", "warc-type": "response", "filename": "example.warc.gz"}
{"offset": "2012", "length": "526", "warc-type": "request", "filename": "example.warc.gz"}
{"offset": "2538", "length": "585", "http:status": "200", "warc-type": "revisit", "filename": "example.warc.gz"}
{"offset": "3123", "length": "526", "warc-type": "request", "filename": "example.warc.gz"}
"""
    with patch_stdout() as buff:
        res = main(args=args)
        assert buff.getvalue() == expected

def test_recompress():
    with named_temp() as temp:
        test_file = get_test_file('example-bad-non-chunked.warc.gz')

        with patch_stdout() as buff:
            with pytest.raises(ArchiveLoadFailed):
                main(args=['index', test_file, '-f', 'warc-type'])

        # recompress!
        main(args=['recompress', test_file, temp.name])

        expected = """\
{"warc-type": "warcinfo"}
{"warc-type": "warcinfo"}
{"warc-type": "response"}
{"warc-type": "request"}
{"warc-type": "revisit"}
{"warc-type": "request"}
"""

        with patch_stdout() as buff:
            main(args=['index', temp.name, '-f', 'warc-type'])
            assert buff.getvalue().decode('utf-8') == expected


def test_recompress_arc2warc():
    with named_temp() as temp:
        test_file = get_test_file('example.arc.gz')

        # recompress!
        main(args=['recompress', test_file, temp.name])

        expected = """\
{"warc-type": "warcinfo"}
{"warc-type": "response", "warc-block-digest": "sha1:PEWDX5GTH66WU74WBPGFECIYBMPMP3FP", "warc-payload-digest": "sha1:B2LTWWPUOYAH7UIPQ7ZUPQ4VMBSVC36A"}
"""

        with patch_stdout() as buff:
            main(args=['index', temp.name, '-f', 'warc-type,warc-block-digest,warc-payload-digest'])
            assert buff.getvalue().decode('utf-8') == expected


def test_recompress_bad_file():
    with named_temp() as temp:
        temp.write(b'abcdefg-not-a-warc\n')
        temp.seek(0)
        with named_temp() as temp2:
            with pytest.raises(ArchiveLoadFailed):
                main(args=['recompress', temp.name, temp2.name])


@contextmanager
def patch_stdout():
    buff = BytesIO()
    if hasattr(sys.stdout, 'buffer'):
        orig = sys.stdout.buffer
        sys.stdout.buffer = buff
        yield buff
        sys.stdout.buffer = orig
    else:
        orig = sys.stdout
        sys.stdout = buff
        yield buff
        sys.stdout = orig


# due to NamedTemporaryFile issue on Windows
# see: https://bugs.python.org/issue14243#msg157925
@contextmanager
def named_temp():
    f = tempfile.NamedTemporaryFile(delete=False)
    try:
        yield f

    finally:
        try:
            os.unlink(f.name)
        except OSError:
            pass

