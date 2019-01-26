from warcio.cli import main

from . import get_test_file
from .test_cli import patch_stdout


def helper(args, expected_exit_value):
    with patch_stdout() as buff:
        exit_value = None
        try:
            main(args=args)
        except SystemExit as e:
            exit_value = e.code
        finally:
            assert exit_value == expected_exit_value

        return buff.getvalue()


def remove_before_test_data(s):
    ret = b''
    for line in s.splitlines(True):
        if b'/test/data/' in line:
            line = b'test/data/' + line.split(b'/test/data/', 1)[1]
        ret += line
    return ret


def test_torture_missing():
    files = ['standard-torture-missing.warc']
    files = [get_test_file(filename) for filename in files]

    args = ['test']
    args.extend(files)

    expected = b"""\
test/data/standard-torture-missing.warc
  WARC-Record-ID None
    WARC-Type warcinfo
    digest not present
    error: missing required header Content-Type
    error: missing required header WARC-Date
    error: missing required header WARC-Record-ID
    recommendation: warcinfo Content-Type of application/warc-fields, saw none
"""

    value = helper(args, 0)
    assert remove_before_test_data(value) == expected


def test_torture_validate_record():
    files = ['standard-torture-validate-record.warc']
    files = [get_test_file(filename) for filename in files]

    args = ['test']
    args.extend(files)

    expected = b"""\
test/data/standard-torture-validate-record.warc
  WARC-Record-ID None
    WARC-Type warcinfo
    digest not present
    error: missing required header WARC-Date
    error: missing required header WARC-Record-ID
    error: warc-fields contains invalid utf-8: 'utf-8' codec can't decode byte 0xc3 in position 57: invalid continuation byte
    comment: The first line of warc-fields cannot start with whitespace
    comment: warc-fields lines must end with \\r\\n: test: lines should end with \\r\\n
    comment: Missing field-name : in warc-fields line: no colon
    comment: invalid warc-fields name: token cannot have a space
  WARC-Record-ID None
    WARC-Type warcinfo
    digest not present
    error: missing required header WARC-Date
    error: missing required header WARC-Record-ID
    comment: warc-fields body present but empty
  WARC-Record-ID None
    WARC-Type response
    digest not present
    error: missing required header WARC-Date
    error: missing required header WARC-Record-ID
    error: responses for http/https should have Content-Type of application/http; msgtype=response or application/http, saw text/plain
    error: WARC-IP-Address should be used for http and https responses
  WARC-Record-ID None
    WARC-Type resource
    digest not present
    error: missing required header WARC-Date
    error: missing required header WARC-Record-ID
  WARC-Record-ID None
    WARC-Type resource
    digest not present
    error: missing required header WARC-Date
    error: missing required header WARC-Record-ID
  WARC-Record-ID None
    WARC-Type request
    digest not present
    error: missing required header WARC-Date
    error: missing required header WARC-Record-ID
    error: requests for http/https should have Content-Type of application/http; msgtype=request or application/http, saw text/plain
    error: WARC-IP-Address should be used for http and https requests
  WARC-Record-ID None
    WARC-Type metadata
    digest not present
    error: missing required header WARC-Date
    error: missing required header WARC-Record-ID
    comment: warc-fields body present but empty
  WARC-Record-ID None
    WARC-Type revisit
    digest not present
    error: missing required header Content-Type
    error: missing required header WARC-Date
    error: missing required header WARC-Record-ID
    error: missing required header WARC-Target-URI
    comment: extension seen warc-profile none
    comment: no revisit details validation done due to unknown profile
  WARC-Record-ID None
    WARC-Type revisit
    digest not present
    error: missing required header Content-Type
    error: missing required header WARC-Date
    error: missing required header WARC-Record-ID
    error: missing required header WARC-Target-URI
    error: missing required header WARC-Payload-Digest
    recommendation: missing recommended header WARC-Refers-To
    recommendation: missing recommended header WARC-Refers-To-Date
    recommendation: missing recommended header WARC-Refers-To-Target-URI
  WARC-Record-ID None
    WARC-Type revisit
    digest not present
    error: missing required header Content-Type
    error: missing required header WARC-Date
    error: missing required header WARC-Record-ID
    error: missing required header WARC-Target-URI
    recommendation: missing recommended header WARC-Refers-To
    recommendation: missing recommended header WARC-Refers-To-Date
    comment: extension seen warc-profile http://netpreserve.org/warc/1.0/revisit/server-not-modified
  WARC-Record-ID None
    WARC-Type continuation
    digest not present
    error: missing required header WARC-Date
    error: missing required header WARC-Record-ID
    error: missing required header WARC-Segment-Origin-ID
    error: missing required header WARC-Target-URI
    error: continuation record must have WARC-Segment-Number > 1, saw 1
    comment: warcio test continuation code has not been tested, expect bugs
"""

    value = helper(args, 0)
    print(remove_before_test_data(value).decode())
    assert remove_before_test_data(value) == expected
