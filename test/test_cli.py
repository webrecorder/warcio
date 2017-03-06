from warcio.cli import main

from . import get_test_file

from contextlib import contextmanager
from io import BytesIO
import sys



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


