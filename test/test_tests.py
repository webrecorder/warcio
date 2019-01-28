from warcio.cli import main
from warcio.utils import to_native_str
import warcio.tester

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

        return to_native_str(buff.getvalue())


def remove_before_test_data(s):
    ret = ''
    for line in s.splitlines(True):
        if '/test/data/' in line:
            line = 'test/data/' + line.split('/test/data/', 1)[1]
        if '\\test\\data\\' in line:
            line = 'test/data/' + line.split('\\test\\data\\', 1)[1]
        ret += line
    return ret


def test_torture_validate_record():
    files = ['standard-torture-validate-record.warc']
    files = [get_test_file(filename) for filename in files]

    args = ['test']
    args.extend(files)

    expected = """\
test/data/standard-torture-validate-record.warc
  WARC-Record-ID None
    WARC-Type warcinfo
    digest not present
    error: uri must be within <>: WARC-Refers-To probhibited
    error: missing required header: WARC-Date
    error: missing required header: WARC-Record-ID
    error: field not allowed in record type: warcinfo WARC-Refers-To
    error: warc-fields contains invalid utf-8: 'utf-8' codec can't decode byte 0xc3 in position 57: invalid continuation byte
    comment: The first line of warc-fields cannot start with whitespace
    comment: warc-fields lines must end with \\r\\n: test: lines should end with \\r\\n
    comment: Missing colon in warc-fields line: no colon
    comment: Invalid warc-fields name: token cannot have a space
  WARC-Record-ID <uri:uuid:test-empty-warc-fields>
    WARC-Type warcinfo
    digest not present
    error: missing required header: WARC-Date
    comment: warc-fields body present but empty
  WARC-Record-ID <uri:uuid:test-warcinfo-non-recommended-content-type>
    WARC-Type warcinfo
    digest not present
    error: missing required header: WARC-Date
    recommendation: warcinfo Content-Type recommended to be application/warc-fields: not-application/warc-fields
  WARC-Record-ID <uri:uuid:test-response-content-type>
    WARC-Type response
    digest not present
    error: missing required header: WARC-Date
    error: responses for http/https should have Content-Type of application/http; msgtype=response or application/http: text/plain
    error: WARC-IP-Address should be used for http and https responses
  WARC-Record-ID <uri:uuid:test-resource-dns-content-type>
    WARC-Type resource
    digest not present
    error: missing required header: WARC-Date
    error: resource records for dns shall have Content-Type of text/dns: text/plain
  WARC-Record-ID <uri:uuid:test-resource-dns-empty>
    WARC-Type resource
    digest not present
    error: missing required header: WARC-Date
    comment: unknown field, no validation performed: WARC-Test-TODO add another with valid block
  WARC-Record-ID <uri:uuid:test-resource-not-dns>
    WARC-Type resource
    digest not present
    error: missing required header: Content-Type
    error: missing required header: WARC-Date
  WARC-Record-ID <uri:uuid:test-request-content-type>
    WARC-Type request
    digest not present
    error: missing required header: WARC-Date
    error: requests for http/https should have Content-Type of application/http; msgtype=request or application/http: text/plain
    error: WARC-IP-Address should be used for http and https requests
  WARC-Record-ID <uri:uuid:test-request-content-type-with-ip>
    WARC-Type request
    digest not present
    error: missing required header: WARC-Date
    error: requests for http/https should have Content-Type of application/http; msgtype=request or application/http: text/plain
  WARC-Record-ID <uri:uuid:test-metadata-warc-fields-empty>
    WARC-Type metadata
    digest not present
    error: missing required header: WARC-Date
    comment: warc-fields body present but empty
  WARC-Record-ID <uri:uuid:test-metadata-not-warc-fields>
    WARC-Type metadata
    digest not present
    error: missing required header: WARC-Date
  WARC-Record-ID <uri:uuid:test-revisit-profile-unknown>
    WARC-Type revisit
    digest not present
    error: missing required header: Content-Type
    error: missing required header: WARC-Date
    error: missing required header: WARC-Target-URI
    comment: extension seen: WARC-Profile none
    comment: no revisit details validation done due to unknown profile: none
  WARC-Record-ID <uri:uuid:test-revisit-profile-future>
    WARC-Type revisit
    digest not present
    error: missing required header: Content-Type
    error: missing required header: WARC-Date
    error: missing required header: WARC-Target-URI
    error: missing required header: WARC-Payload-Digest
    recommendation: missing recommended header: WARC-Refers-To
    recommendation: missing recommended header: WARC-Refers-To-Date
    recommendation: missing recommended header: WARC-Refers-To-Target-URI
    comment: extension seen: WARC-Profile http://netpreserve.org/warc/1.1/revisit/identical-payload-digest
  WARC-Record-ID <uri:uuid:test-revisit-profile-good>
    WARC-Type revisit
    digest not present
    error: missing required header: Content-Type
    error: missing required header: WARC-Date
    error: missing required header: WARC-Target-URI
    recommendation: missing recommended header: WARC-Refers-To
    recommendation: missing recommended header: WARC-Refers-To-Date
  WARC-Record-ID <uri:uuid:test-conversion>
    WARC-Type conversion
    digest not present
    error: missing required header: WARC-Date
    error: missing required header: WARC-Target-URI
  WARC-Record-ID <uri:uuid:test-continuation-segment-1>
    WARC-Type continuation
    digest not present
    error: missing required header: WARC-Date
    error: missing required header: WARC-Segment-Origin-ID
    error: missing required header: WARC-Target-URI
    error: continuation record must have WARC-Segment-Number > 1: 1
    comment: warcio test continuation code has not been tested, expect bugs
  WARC-Record-ID <uri:uuid:test-continuation-segment-valid>
    WARC-Type continuation
    digest not present
    error: missing required header: WARC-Date
    error: missing required header: WARC-Segment-Origin-ID
    error: missing required header: WARC-Target-URI
    comment: warcio test continuation code has not been tested, expect bugs
"""

    value = helper(args, 0)
    print(remove_before_test_data(value))

    actual = remove_before_test_data(value)

    assert actual == expected


def test_torture_validate_field():
    files = ['standard-torture-validate-field.warc']
    files = [get_test_file(filename) for filename in files]

    args = ['test']
    args.extend(files)

    expected = """\
test/data/standard-torture-validate-field.warc
  WARC-Record-ID <urn:uuid:torture-validate-field>
    WARC-Type does-not-exist
    unknown hash algorithm name in block digest
    error: uri must not be within <>: WARC-Target-URI <http://example.com/>
    error: invalid uri scheme, bad character: WARC-Target-URI <http://example.com/>
    error: duplicate field seen: WARC-Target-URI example.com
    error: invalid uri, no scheme: WARC-Target-URI example.com
    error: duplicate field seen: WARC-Target-URI ex ample.com
    error: invalid uri, no scheme: WARC-Target-URI ex ample.com
    error: invalid uri, contains whitespace: WARC-Target-URI ex ample.com
    error: invalid uri scheme, bad character: WARC-Target-URI ex ample.com
    error: duplicate field seen: WARC-Target-URI h<>ttp://example.com/
    error: invalid uri scheme, bad character: WARC-Target-URI h<>ttp://example.com/
    error: duplicate field seen: WARC-Type CAPITALIZED
    error: uri must be within <>: WARC-Concurrent-To http://example.com/
    error: duplicate field seen: WARC-Date 2017-03-06T04:03:53.Z
    error: WARC 1.0 time may not have fractional seconds: WARC-Date 2017-03-06T04:03:53.Z
    error: must contain a /: Content-Type asdf
    error: invalid subtype: Content-Type asdf
    error: duplicate field seen: Content-Type has space/asdf
    error: invalid type: Content-Type has space/asdf
    error: duplicate field seen: Content-Type asdf/has space
    error: invalid subtype: Content-Type asdf/has space
    error: duplicate field seen: Content-Type asdf/has space;asdf
    error: invalid subtype: Content-Type asdf/has space;asdf
    error: missing algorithm: WARC-Block-Digest asdf
    error: duplicate field seen: WARC-Block-Digest has space:asdf
    error: invalid algorithm: WARC-Block-Digest has space:asdf
    error: duplicate field seen: WARC-Block-Digest sha1:&$*^&*^#*&^
    error: invalid ip: WARC-IP-Address 1.2.3.4.5
    error: uri must be within <>: WARC-Warcinfo-ID asdf:asdf
    error: duplicate field seen: WARC-Profile http://netpreserve.org/warc/1.0/revisit/identical-payload-digest
    error: must contain a /: WARC-Identified-Payload-Type asdf
    error: invalid subtype: WARC-Identified-Payload-Type asdf
    error: uri must be within <>: WARC-Segment-Origin-ID http://example.com
    error: must be an integer: WARC-Segment-Number not-an-integer
    error: duplicate field seen: WARC-Segment-Number 0
    error: must be 1 or greater: WARC-Segment-Number 0
    error: non-continuation records must always have WARC-Segment-Number: 1: WARC-Segment-Number 0
    error: duplicate field seen: WARC-Segment-Number 1
    error: duplicate field seen: WARC-Segment-Number 2
    error: non-continuation records must always have WARC-Segment-Number: 1: WARC-Segment-Number 2
    error: duplicate field seen: WARC-Segment-Total-Length not-an-integer
    error: must be an integer: WARC-Segment-Total-Length not-an-integer
    comment: unknown WARC-Type: WARC-Type does-not-exist
    comment: WARC-Type is not lower-case: WARC-Type CAPITALIZED
    comment: unknown WARC-Type: WARC-Type CAPITALIZED
    comment: unknown digest algorithm: WARC-Block-Digest asdf
    comment: Invalid-looking digest value: WARC-Block-Digest sha1:&$*^&*^#*&^
    comment: extension seen: WARC-Truncated invalid
    comment: extension seen: WARC-Profile asdf
    comment: field was introduced after this warc version: 1.0 WARC-Refers-To-Target-URI http://example.com
    comment: field was introduced after this warc version: 1.0 WARC-Refers-To-Date not-a-date
    comment: unknown field, no validation performed: WARC-Unknown-Field asdf
  WARC-Record-ID None
    WARC-Type invalid
    digest not present
    error: duplicate field seen: WARC-Date 2017-03-06T04:03:53.Z
    error: fractional seconds must have 1-9 digits: WARC-Date 2017-03-06T04:03:53.Z
    error: duplicate field seen: WARC-Date 2017-03-06T04:03:53.0Z
    comment: unknown WARC-Type: WARC-Type invalid
  WARC-Record-ID None
    WARC-Type request
    digest not present
    error: segmented records must have both WARC-Segment-Number and WARC-Segment-Origin-ID
    error: missing required header: Content-Type
    error: missing required header: WARC-Date
    error: missing required header: WARC-Record-ID
    error: missing required header: WARC-Target-URI
    recommendation: do not segment WARC-Type request
"""

    value = helper(args, 0)
    actual = remove_before_test_data(value)

    print(actual)
    assert actual == expected


def test_arc():
    files = ['does-not-exist.arc']
    files = [get_test_file(filename) for filename in files]

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
    files = [get_test_file(filename) for filename in files]

    args = ['test']
    args.extend(files)

    expected = """\
test/data/example-digest-bad.warc
  WARC-Record-ID <urn:uuid:a9c5c23a-0221-11e7-8fe3-0242ac120007>
    WARC-Type request
    payload digest failed: sha1:1112H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ
    error: WARC-IP-Address should be used for http and https requests
  WARC-Record-ID <urn:uuid:a9c5c23a-0221-11e7-8fe3-0242ac120007>
    WARC-Type request
    digest pass
    error: WARC-IP-Address should be used for http and https requests
  WARC-Record-ID <urn:uuid:a9c5c23a-0221-11e7-8fe3-0242ac120007>
    WARC-Type request
    digest pass
    error: WARC-IP-Address should be used for http and https requests
  WARC-Record-ID <urn:uuid:a9c5c23a-0221-11e7-8fe3-0242ac120007>
    WARC-Type request
    digest pass
    error: WARC-IP-Address should be used for http and https requests
test/data/example.warc
  WARC-Record-ID <urn:uuid:a9c5c23a-0221-11e7-8fe3-0242ac120007>
    WARC-Type request
    digest not present
    error: WARC-IP-Address should be used for http and https requests
  WARC-Record-ID <urn:uuid:e6e395ca-0221-11e7-a18d-0242ac120005>
    WARC-Type revisit
    digest present but not checked
    recommendation: missing recommended header: WARC-Refers-To
    comment: field was introduced after this warc version: 1.0 WARC-Refers-To-Target-URI http://example.com/
    comment: field was introduced after this warc version: 1.0 WARC-Refers-To-Date 2017-03-06T04:02:06Z
  WARC-Record-ID <urn:uuid:e6e41fea-0221-11e7-8fe3-0242ac120007>
    WARC-Type request
    digest not present
    error: WARC-IP-Address should be used for http and https requests
"""

    value = helper(args, 0)
    assert remove_before_test_data(value) == expected


def test_leftovers():
    commentary = warcio.tester.Commentary('id', 'type')
    assert not commentary.has_comments()

    # hard to test because invalid WARC Content-Length raises in archiveiterator
    warcio.tester.validate_content_length('Content-Length', 'not-an-integer', None, '1.0', commentary, None)

    # hard to test because warcio checks the WARC version
    warcio.tester.validate_profile('blah', 'blah', None, '999', commentary, None)

    expected = '''\
error: must be an integer: Content-Length not-an-integer
comment: no profile check because unknown warc version: blah blah
'''

    assert '\n'.join(commentary.comments())+'\n' == expected
