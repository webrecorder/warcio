from warcio.cli import main
from warcio import ArchiveIterator
from warcio.warcwriter import BufferWARCWriter

from . import get_test_file
import os

SKIP = ['example-trunc.warc',
        'example-iana.org-chunked.warc',
        'example-wrong-chunks.warc.gz',
        'example-bad-non-chunked.warc.gz',
        'example-digest.warc'
       ]


def pytest_generate_tests(metafunc):
    if 'test_filename' in metafunc.fixturenames:
        files = [filename for filename in os.listdir(get_test_file('.'))
                 if filename not in SKIP and filename.endswith(('.warc', '.warc.gz', '.arc', '.arc.gz'))]

        metafunc.parametrize('test_filename', files)


class TestExamplesDigest(object):
    def check_helper(self, args, expected_exit_value, capsys):
        exit_value = None
        try:
            main(args=args)
        except SystemExit as e:
            exit_value = e.code
        finally:
            assert exit_value == expected_exit_value

        return capsys.readouterr()[0]  # list for py33 support

    def test_check_invalid(self, capsys):
        filenames = [get_test_file('example-digest.warc')]

        args = ['check'] + filenames
        value = self.check_helper(args, 1, capsys)
        assert value.count('payload digest failed') == 1
        assert value.count('WARC-Record-ID') == 1

        args = ['check', '-v'] + filenames
        value = self.check_helper(args, 1, capsys)
        assert value.count('payload digest failed') == 1
        assert value.count('digest pass') == 3
        assert value.count('WARC-Record-ID') == 4

    def test_check_valid(self, capsys):
        filenames = [get_test_file('example.warc'), get_test_file('example.warc.gz')]

        args = ['check'] + filenames
        expected = ''
        assert self.check_helper(args, 0, capsys) == expected

        args = ['check', '-v'] + filenames
        value = self.check_helper(args, 0, capsys)
        # two digests per file (payload and block)
        assert value.count('digest pass') == 4
        assert value.count('WARC-Record-ID') == 12

    def test_check_valid_chunked(self, capsys):
        filenames = [get_test_file('example-iana.org-chunked.warc')]

        args = ['check'] + filenames
        expected = ''
        assert self.check_helper(args, 0, capsys) == expected

        args = ['check', '-v'] + filenames
        value = self.check_helper(args, 0, capsys)
        # two digests per file (payload and block)
        assert value.count('no digest to check') == 1
        assert value.count('digest pass') == 2
        assert value.count('WARC-Record-ID') == 3

    def test_check_no_invalid_files(self, test_filename, capsys):
        args = ['check', '-v', get_test_file(test_filename)]
        value = self.check_helper(args, 0, capsys)
        assert value.count('digest failed') == 0

        # if ARC file, no digests to check, so no passing results
        if test_filename.endswith(('.arc', '.arc.gz')):
            assert value.count('digest pass') == 0
