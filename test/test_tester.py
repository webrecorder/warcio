from warcio.cli import main
from warcio.utils import to_native_str
import warcio.tester

from . import get_test_file
from .test_cli import patch_stdout


file_map = {}


def map_test_file(filename):
    file_map[filename] = get_test_file(filename)
    return file_map[filename]


def helper(args, expected_exit_value):
    with patch_stdout() as buff:
        exit_value = None
        try:
            main(args=args)
        except SystemExit as e:
            exit_value = e.code
        finally:
            assert exit_value == expected_exit_value

        return to_native_str(buff.getvalue())


def remove_before_test_data(s):
    ret = ''
    for line in s.splitlines(True):
        for filename, value in file_map.items():
            if value in line:
                line = line.replace(value, 'test/data/' + filename)
        ret += line
    return ret


def run_one(f):
    args = ['test']
    args.append(f)

    with open(f+'.test', 'r') as expectedf:
        expected = expectedf.read()

    value = helper(args, 0)
    print(remove_before_test_data(value))

    actual = remove_before_test_data(value)

    assert actual == expected


def test_torture():
    files = ['standard-torture-validate-record.warc',
             'standard-torture-validate-field.warc']
    [run_one(map_test_file(filename)) for filename in files]


def test_arc():
    files = ['does-not-exist.arc']
    files = [map_test_file(filename) for filename in files]

    args = ['test']
    args.extend(files)

    expected = """\
test/data/does-not-exist.arc
"""

    value = helper(args, 0)
    assert remove_before_test_data(value) == expected


def test_digests():
    # needed for test coverage
    files = ['example-digest-bad.warc', 'example.warc']
    [run_one(map_test_file(filename)) for filename in files]


def test_leftovers():
    commentary = warcio.tester.Commentary('id', 'type')
    assert not commentary.has_comments()

    # hard to test because invalid WARC Content-Length raises in archiveiterator
    warcio.tester.validate_content_length('Content-Length', 'not-an-integer', None, '1.0', commentary, None)

    # hard to test because warcio raises for unknown WARC version
    warcio.tester.validate_profile('blah', 'blah', None, '999', commentary, None)

    expected = '''\
error: Must be an integer: Content-Length not-an-integer
'''

    assert '\n'.join(commentary.comments())+'\n' == expected
