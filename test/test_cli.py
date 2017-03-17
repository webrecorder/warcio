from warcio.cli import main

from . import get_test_file

from contextlib import contextmanager
from io import BytesIO

from warcio.recordloader import ArchiveLoadFailed

import pytest
import sys
import tempfile


def test_index():
    files = ['example.warc.gz', 'example.warc', 'example.arc.gz', 'example.arc']
    files = [get_test_file(filename) for filename in files]

    args = ['index', '-f', 'warc-type,warc-target-uri,warc-filename']
    args.extend(files)

    expected = b"""\
{"offset": 0, "warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"offset": 353, "warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"offset": 784, "warc-type": "response", "warc-target-uri": "http://example.com/"}
{"offset": 2012, "warc-type": "request", "warc-target-uri": "http://example.com/"}
{"offset": 2538, "warc-type": "revisit", "warc-target-uri": "http://example.com/"}
{"offset": 3123, "warc-type": "request", "warc-target-uri": "http://example.com/"}
{"offset": 0, "warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"offset": 488, "warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"offset": 1197, "warc-type": "response", "warc-target-uri": "http://example.com/"}
{"offset": 2566, "warc-type": "request", "warc-target-uri": "http://example.com/"}
{"offset": 3370, "warc-type": "revisit", "warc-target-uri": "http://example.com/"}
{"offset": 4316, "warc-type": "request", "warc-target-uri": "http://example.com/"}
{"offset": 0, "warc-type": "warcinfo", "warc-filename": "live-web-example.arc.gz"}
{"offset": 171, "warc-type": "response", "warc-target-uri": "http://example.com/"}
{"offset": 0, "warc-type": "warcinfo", "warc-filename": "live-web-example.arc.gz"}
{"offset": 151, "warc-type": "response", "warc-target-uri": "http://example.com/"}
"""

    with patch_stdout() as buff:
        res = main(args=args)
        assert buff.getvalue() == expected


def test_recompress():
    with tempfile.NamedTemporaryFile() as temp:
        test_file = get_test_file('example-bad-non-chunked.warc.gz')

        with patch_stdout() as buff:
            with pytest.raises(ArchiveLoadFailed):
                main(args=['index', test_file, '-f', 'warc-type'])

        # recompress!
        main(args=['recompress', test_file, temp.name])

        expected = """\
{"offset": 0, "warc-type": "warcinfo"}
{"offset": 353, "warc-type": "warcinfo"}
{"offset": 784, "warc-type": "response"}
{"offset": 2012, "warc-type": "request"}
{"offset": 2583, "warc-type": "revisit"}
{"offset": 2965, "warc-type": "request"}
"""

        with patch_stdout() as buff:
            main(args=['index', temp.name, '-f', 'warc-type'])
            assert buff.getvalue().decode('utf-8') == expected


def test_recompress_bad_file():
    with tempfile.NamedTemporaryFile() as temp:
        temp.write(b'abcdefg-not-a-warc\n')
        temp.seek(0)
        with tempfile.NamedTemporaryFile() as temp2:
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


