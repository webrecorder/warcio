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

    expected = """\
{"warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"warc-type": "response", "warc-target-uri": "http://example.com/"}
{"warc-type": "request", "warc-target-uri": "http://example.com/"}
{"warc-type": "revisit", "warc-target-uri": "http://example.com/"}
{"warc-type": "request", "warc-target-uri": "http://example.com/"}
{"warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"warc-type": "warcinfo", "warc-filename": "temp-20170306040353.warc.gz"}
{"warc-type": "response", "warc-target-uri": "http://example.com/"}
{"warc-type": "request", "warc-target-uri": "http://example.com/"}
{"warc-type": "revisit", "warc-target-uri": "http://example.com/"}
{"warc-type": "request", "warc-target-uri": "http://example.com/"}
{"warc-type": "warcinfo", "warc-filename": "live-web-example.arc.gz"}
{"warc-type": "response", "warc-target-uri": "http://example.com/"}
{"warc-type": "warcinfo", "warc-filename": "live-web-example.arc.gz"}
{"warc-type": "response", "warc-target-uri": "http://example.com/"}
"""

    with patch_stdout() as buff:
        res = main(args=args)
        assert buff.getvalue().decode('utf-8') == expected


def test_recompress():
    with tempfile.NamedTemporaryFile() as temp:
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


