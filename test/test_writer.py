from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import BufferWARCWriter
from warcio.recordloader import ArcWarcRecordLoader
from warcio.archiveiterator import ArchiveIterator
from warcio.bufferedreaders import DecompressingBufferedReader

from . import get_test_file

from io import BytesIO
from collections import OrderedDict
import json

import pytest


# ============================================================================
class FixedTestWARCWriter(BufferWARCWriter):
    @classmethod
    def _make_warc_id(cls, id_=None):
        return '<urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>'

    @classmethod
    def _make_warc_date(cls):
        return '2000-01-01T00:00:00Z'


# ============================================================================
WARCINFO_RECORD = '\
WARC/1.0\r\n\
WARC-Type: warcinfo\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Filename: testfile.warc.gz\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
Content-Type: application/warc-fields\r\n\
Content-Length: 86\r\n\
\r\n\
software: recorder test\r\n\
format: WARC File Format 1.0\r\n\
json-metadata: {"foo": "bar"}\r\n\
\r\n\
\r\n\
'


RESPONSE_RECORD = '\
WARC/1.0\r\n\
WARC-Type: response\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: http://example.com/\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Payload-Digest: sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O\r\n\
WARC-Block-Digest: sha1:OS3OKGCWQIJOAOC3PKXQOQFD52NECQ74\r\n\
Content-Type: application/http; msgtype=response\r\n\
Content-Length: 97\r\n\
\r\n\
HTTP/1.0 200 OK\r\n\
Content-Type: text/plain; charset="UTF-8"\r\n\
Custom-Header: somevalue\r\n\
\r\n\
some\n\
text\r\n\
\r\n\
'


RESPONSE_RECORD_2 = '\
WARC/1.0\r\n\
WARC-Type: response\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: http://example.com/\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Payload-Digest: sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O\r\n\
WARC-Block-Digest: sha1:U6KNJY5MVNU3IMKED7FSO2JKW6MZ3QUX\r\n\
Content-Type: application/http; msgtype=response\r\n\
Content-Length: 145\r\n\
\r\n\
HTTP/1.0 200 OK\r\n\
Content-Type: text/plain; charset="UTF-8"\r\n\
Content-Length: 9\r\n\
Custom-Header: somevalue\r\n\
Content-Encoding: x-unknown\r\n\
\r\n\
some\n\
text\r\n\
\r\n\
'


REQUEST_RECORD = '\
WARC/1.0\r\n\
WARC-Type: request\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: http://example.com/\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Payload-Digest: sha1:3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ\r\n\
WARC-Block-Digest: sha1:ONEHF6PTXPTTHE3333XHTD2X45TZ3DTO\r\n\
Content-Type: application/http; msgtype=request\r\n\
Content-Length: 54\r\n\
\r\n\
GET / HTTP/1.0\r\n\
User-Agent: foo\r\n\
Host: example.com\r\n\
\r\n\
\r\n\
\r\n\
'


REVISIT_RECORD_1 = '\
WARC/1.0\r\n\
WARC-Type: revisit\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: http://example.com/\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Profile: http://netpreserve.org/warc/1.0/revisit/identical-payload-digest\r\n\
WARC-Refers-To-Target-URI: http://example.com/foo\r\n\
WARC-Refers-To-Date: 1999-01-01T00:00:00Z\r\n\
WARC-Payload-Digest: sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O\r\n\
WARC-Block-Digest: sha1:3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ\r\n\
Content-Type: application/http; msgtype=response\r\n\
Content-Length: 0\r\n\
\r\n\
\r\n\
\r\n\
'


REVISIT_RECORD_2 = '\
WARC/1.0\r\n\
WARC-Type: revisit\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: http://example.com/\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Profile: http://netpreserve.org/warc/1.0/revisit/identical-payload-digest\r\n\
WARC-Refers-To-Target-URI: http://example.com/foo\r\n\
WARC-Refers-To-Date: 1999-01-01T00:00:00Z\r\n\
WARC-Payload-Digest: sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O\r\n\
WARC-Block-Digest: sha1:A6J5UTI2QHHCZFCFNHQHCDD3JJFKP53V\r\n\
Content-Type: application/http; msgtype=response\r\n\
Content-Length: 88\r\n\
\r\n\
HTTP/1.0 200 OK\r\n\
Content-Type: text/plain; charset="UTF-8"\r\n\
Custom-Header: somevalue\r\n\
\r\n\
\r\n\
\r\n\
'


RESOURCE_RECORD = '\
WARC/1.0\r\n\
WARC-Type: resource\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: ftp://example.com/\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Payload-Digest: sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O\r\n\
WARC-Block-Digest: sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O\r\n\
Content-Type: text/plain\r\n\
Content-Length: 9\r\n\
\r\n\
some\n\
text\r\n\
\r\n\
'


METADATA_RECORD = '\
WARC/1.0\r\n\
WARC-Type: metadata\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: http://example.com/\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Payload-Digest: sha1:ZOLBLKAQVZE5DXH56XE6EH6AI6ZUGDPT\r\n\
WARC-Block-Digest: sha1:ZOLBLKAQVZE5DXH56XE6EH6AI6ZUGDPT\r\n\
Content-Type: application/json\r\n\
Content-Length: 67\r\n\
\r\n\
{"metadata": {"nested": "obj", "list": [1, 2, 3], "length": "123"}}\r\n\
\r\n\
'


# ============================================================================
# Decorator Setup
# ============================================================================
all_sample_records = {}

def sample_record(name, record_string):
    def decorate(f):
        all_sample_records[name] = (f, record_string)
        return f

    return decorate


# ============================================================================
# Sample Record Functions
# ============================================================================
@sample_record('warcinfo', WARCINFO_RECORD)
def sample_warcinfo(writer):
    params = OrderedDict([('software', 'recorder test'),
                          ('format', 'WARC File Format 1.0'),
                          ('invalid', ''),
                          ('json-metadata', json.dumps({'foo': 'bar'}))])

    return writer.create_warcinfo_record('testfile.warc.gz', params)


# ============================================================================
@sample_record('response', RESPONSE_RECORD)
def sample_response(writer):
    headers_list = [('Content-Type', 'text/plain; charset="UTF-8"'),
                    ('Custom-Header', 'somevalue')
                   ]

    payload = b'some\ntext'

    http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')

    return writer.create_warc_record('http://example.com/', 'response',
                                     payload=BytesIO(payload),
                                     length=len(payload),
                                     http_headers=http_headers)


# ============================================================================
@sample_record('response', RESPONSE_RECORD_2)
def sample_response_2(writer):
    payload = b'some\ntext'

    headers_list = [('Content-Type', 'text/plain; charset="UTF-8"'),
                    ('Content-Length', str(len(payload))),
                    ('Custom-Header', 'somevalue'),
                    ('Content-Encoding', 'x-unknown'),
                   ]

    http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')

    return writer.create_warc_record('http://example.com/', 'response',
                                     payload=BytesIO(payload),
                                     length=len(payload),
                                     http_headers=http_headers)


# ============================================================================
@sample_record('request', REQUEST_RECORD)
def sample_request(writer):
    headers_list = [('User-Agent', 'foo'),
                    ('Host', 'example.com')]

    http_headers = StatusAndHeaders('GET / HTTP/1.0', headers_list)

    return writer.create_warc_record('http://example.com/', 'request',
                                     http_headers=http_headers)


# ============================================================================
@sample_record('resource', RESOURCE_RECORD)
def sample_resource(writer):
    payload = b'some\ntext'

    return writer.create_warc_record('ftp://example.com/', 'resource',
                                      payload=BytesIO(payload),
                                      length=len(payload),
                                      warc_content_type='text/plain')


# ============================================================================
@sample_record('metadata', METADATA_RECORD)
def sample_metadata(writer):

    payload_dict = {"metadata": OrderedDict([("nested", "obj"),
                                             ("list", [1, 2, 3]),
                                             ("length", "123")])}

    payload = json.dumps(payload_dict).encode('utf-8')

    return writer.create_warc_record('http://example.com/', 'metadata',
                                      payload=BytesIO(payload),
                                      length=len(payload),
                                      warc_content_type='application/json')


# ============================================================================
@sample_record('revisit_1', REVISIT_RECORD_1)
def sample_revisit_1(writer):
    return writer.create_revisit_record('http://example.com/',
                                         digest='sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O',
                                         refers_to_uri='http://example.com/foo',
                                         refers_to_date='1999-01-01T00:00:00Z')


# ============================================================================
@sample_record('revisit_2', REVISIT_RECORD_2)
def sample_revisit_2(writer):
    resp = sample_response(writer)

    return writer.create_revisit_record('http://example.com/',
                                        digest='sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O',
                                        refers_to_uri='http://example.com/foo',
                                        refers_to_date='1999-01-01T00:00:00Z',
                                        http_headers=resp.http_headers)


# ============================================================================
# Fixture Setup
# ============================================================================
@pytest.fixture(params=['gzip', 'plain'])
def is_gzip(request):
    return request.param == 'gzip'


@pytest.fixture(params=all_sample_records.keys())
def record_sampler(request):
    return all_sample_records[request.param]


# ============================================================================
class TestWarcWriter(object):
    @classmethod
    def _validate_record_content_len(cls, stream):
        for record in ArchiveIterator(stream, no_record_parse=True):
            assert record.http_headers == None
            assert int(record.rec_headers.get_header('Content-Length')) == record.length
            assert record.length == len(record.raw_stream.read())

    def test_generate_record(self, record_sampler, is_gzip):
        writer = FixedTestWARCWriter(gzip=is_gzip)

        record_maker, record_string = record_sampler
        record = record_maker(writer)

        writer.write_record(record)

        raw_buff = writer.get_contents()

        self._validate_record_content_len(BytesIO(raw_buff))

        stream = DecompressingBufferedReader(writer.get_stream())

        buff = stream.read()

        if is_gzip:
            assert len(buff) > len(raw_buff)
        else:
            assert len(buff) == len(raw_buff)

        assert buff.decode('utf-8') == record_string

        # assert parsing record matches as well
        stream = DecompressingBufferedReader(writer.get_stream())
        parsed_record = ArcWarcRecordLoader().parse_record_stream(stream)
        writer2 = FixedTestWARCWriter(gzip=False)
        writer2.write_record(parsed_record)
        assert writer2.get_contents().decode('utf-8') == record_string

        # verify parts of record
        stream = DecompressingBufferedReader(writer.get_stream())
        parsed_record = ArcWarcRecordLoader().parse_record_stream(stream)

        content_buff = parsed_record.content_stream().read().decode('utf-8')
        assert content_buff in record_string

        rec_type = parsed_record.rec_type
        # verify http_headers
        if parsed_record.http_headers:
            assert rec_type in ('response', 'request', 'revisit')
        else:
            # empty revisit
            if rec_type == 'revisit':
                assert len(content_buff) == 0
            else:
                assert len(content_buff) == parsed_record.length

    def test_warcinfo_record(self, is_gzip):
        writer = FixedTestWARCWriter(gzip=is_gzip)

        record = sample_warcinfo(writer)

        writer.write_record(record)
        reader = DecompressingBufferedReader(writer.get_stream())

        parsed_record = ArcWarcRecordLoader().parse_record_stream(reader)

        assert parsed_record.rec_headers.get_header('WARC-Type') == 'warcinfo'
        assert parsed_record.rec_headers.get_header('Content-Type') == 'application/warc-fields'
        assert parsed_record.rec_headers.get_header('WARC-Filename') == 'testfile.warc.gz'

        buff = parsed_record.content_stream().read().decode('utf-8')

        assert 'json-metadata: {"foo": "bar"}\r\n' in buff
        assert 'format: WARC File Format 1.0\r\n' in buff

    def test_request_response_concur(self, is_gzip):
        writer = BufferWARCWriter(gzip=is_gzip)

        resp = sample_response(writer)

        req = sample_request(writer)

        writer.write_request_response_pair(req, resp)

        stream = writer.get_stream()

        reader = ArchiveIterator(stream)
        resp, req = list(reader)

        resp_id = resp.rec_headers.get_header('WARC-Record-ID')
        req_id = req.rec_headers.get_header('WARC-Record-ID')

        assert resp_id != req_id
        assert resp_id == req.rec_headers.get_header('WARC-Concurrent-To')

    def test_copy_from_stream(self):
        writer = FixedTestWARCWriter(gzip=False)

        stream = BytesIO()

        # strip-off the two empty \r\n\r\n added at the end of uncompressed record
        stream.write(RESPONSE_RECORD[:-4].encode('utf-8'))

        length = stream.tell()
        stream.seek(0)

        record = writer.create_record_from_stream(stream, length)

        writer.write_record(record)

        buff = writer.get_contents()

        assert buff.decode('utf-8') == RESPONSE_RECORD

    def test_arc2warc(self):
        writer = FixedTestWARCWriter(gzip=False)

        with open(get_test_file('example.arc.gz'), 'rb') as fh:
            for record in ArchiveIterator(fh, arc2warc=True):
                writer.write_record(record)

            buff = writer.get_contents()

        self._validate_record_content_len(BytesIO(buff))

        buff = buff.decode('utf-8')

        assert 'WARC-Filename: live-web-example.arc.gz' in buff
        assert 'Content-Type: text/plain' in buff

        assert 'WARC-Target-URI: http://example.com/' in buff

