#!/usr/bin/env python
# -*- coding: utf-8 -*-

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import BufferWARCWriter, GzippingWrapper
from warcio.recordbuilder import RecordBuilder
from warcio.recordloader import ArcWarcRecordLoader
from warcio.archiveiterator import ArchiveIterator
from warcio.bufferedreaders import DecompressingBufferedReader

from . import get_test_file

from io import BytesIO
from collections import OrderedDict
import json
import re

import pytest


# ============================================================================
class FixedTestRecordMixin:
    @classmethod
    def _make_warc_id(cls, id_=None):
        return '<urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>'

    @classmethod
    def _make_warc_date(cls, use_micros=False):
        if not use_micros:
            return '2000-01-01T00:00:00Z'
        else:
            return '2000-01-01T00:00:00.123456Z'

class FixedTestRecordBuilder(FixedTestRecordMixin, RecordBuilder):
    pass

class FixedTestWARCWriter(FixedTestRecordMixin, BufferWARCWriter):
    pass

# ============================================================================
WARCINFO_RECORD = '\
WARC/1.0\r\n\
WARC-Type: warcinfo\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Filename: testfile.warc.gz\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Block-Digest: sha1:GAD6P5BTZPRU57ICXEYUJZGCURZYABID\r\n\
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

RESPONSE_RECORD_UNICODE_HEADERS = '\
WARC/1.0\r\n\
WARC-Type: response\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: http://example.com/\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Payload-Digest: sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O\r\n\
WARC-Block-Digest: sha1:KMUABC6URWIQ7QXCZDQ5FS6WIBBFRORR\r\n\
Content-Type: application/http; msgtype=response\r\n\
Content-Length: 268\r\n\
\r\n\
HTTP/1.0 200 OK\r\n\
Content-Type: text/plain; charset="UTF-8"\r\n\
Content-Disposition: attachment; filename*=UTF-8\'\'%D0%B8%D1%81%D0%BF%D1%8B%D1%82%D0%B0%D0%BD%D0%B8%D0%B5.txt\r\n\
Custom-Header: somevalue\r\n\
Unicode-Header: %F0%9F%93%81%20text%20%F0%9F%97%84%EF%B8%8F\r\n\
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


REQUEST_RECORD_2 = '\
WARC/1.0\r\n\
WARC-Type: request\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: http://example.com/\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Payload-Digest: sha1:R5VZAKIE53UW5VGK43QJIFYS333QM5ZA\r\n\
WARC-Block-Digest: sha1:L7SVBUPPQ6RH3ANJD42G5JL7RHRVZ5DV\r\n\
Content-Type: application/http; msgtype=request\r\n\
Content-Length: 92\r\n\
\r\n\
POST /path HTTP/1.0\r\n\
Content-Type: application/json\r\n\
Content-Length: 17\r\n\
\r\n\
{"some": "value"}\r\n\
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


REVISIT_RECORD_3 = '\
WARC/1.1\r\n\
WARC-Type: revisit\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: http://example.com/\r\n\
WARC-Date: 2000-01-01T00:00:00.123456Z\r\n\
WARC-Profile: http://netpreserve.org/warc/1.1/revisit/identical-payload-digest\r\n\
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


RESOURCE_RECORD_NO_CONTENT_TYPE = '\
WARC/1.0\r\n\
WARC-Type: resource\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: ftp://example.com/\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Payload-Digest: sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O\r\n\
WARC-Block-Digest: sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O\r\n\
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


DNS_RESPONSE_RECORD = '\
WARC/1.0\r\n\
WARC-Type: response\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: dns:google.com\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Payload-Digest: sha1:2AAVJYKKIWK5CF6EWE7PH63EMNLO44TH\r\n\
WARC-Block-Digest: sha1:2AAVJYKKIWK5CF6EWE7PH63EMNLO44TH\r\n\
Content-Type: application/http; msgtype=response\r\n\
Content-Length: 147\r\n\
\r\n\
20170509000739\n\
google.com.     185 IN  A   209.148.113.239\n\
google.com.     185 IN  A   209.148.113.238\n\
google.com.     185 IN  A   209.148.113.250\n\
\r\n\r\n\
'

DNS_RESOURCE_RECORD = '\
WARC/1.0\r\n\
WARC-Type: resource\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: dns:google.com\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Payload-Digest: sha1:2AAVJYKKIWK5CF6EWE7PH63EMNLO44TH\r\n\
WARC-Block-Digest: sha1:2AAVJYKKIWK5CF6EWE7PH63EMNLO44TH\r\n\
Content-Type: application/warc-record\r\n\
Content-Length: 147\r\n\
\r\n\
20170509000739\n\
google.com.     185 IN  A   209.148.113.239\n\
google.com.     185 IN  A   209.148.113.238\n\
google.com.     185 IN  A   209.148.113.250\n\
\r\n\r\n\
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
def sample_warcinfo(builder):
    params = OrderedDict([('software', 'recorder test'),
                          ('format', 'WARC File Format 1.0'),
                          ('invalid', ''),
                          ('json-metadata', json.dumps({'foo': 'bar'}))])

    return builder.create_warcinfo_record('testfile.warc.gz', params)


# ============================================================================
@sample_record('response_1', RESPONSE_RECORD)
def sample_response(builder):
    headers_list = [('Content-Type', 'text/plain; charset="UTF-8"'),
                    ('Custom-Header', 'somevalue')
                   ]

    payload = b'some\ntext'

    http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')

    return builder.create_warc_record('http://example.com/', 'response',
                                     payload=BytesIO(payload),
                                     length=len(payload),
                                     http_headers=http_headers)


# ============================================================================
@sample_record('response_1-buff', RESPONSE_RECORD)
def sample_response_from_buff(builder):
    payload = '\
HTTP/1.0 200 OK\r\n\
Content-Type: text/plain; charset="UTF-8"\r\n\
Custom-Header: somevalue\r\n\
\r\n\
some\ntext'.encode('utf-8')

    return builder.create_warc_record('http://example.com/', 'response',
                                     payload=BytesIO(payload),
                                     length=len(payload))


# ============================================================================
@sample_record('response-unicode-header', RESPONSE_RECORD_UNICODE_HEADERS)
def sample_response_unicode(builder):
    headers_list = [('Content-Type', 'text/plain; charset="UTF-8"'),
                    ('Content-Disposition', u'attachment; filename="испытание.txt"'),
                    ('Custom-Header', 'somevalue'),
                    ('Unicode-Header', '📁 text 🗄️'),
                   ]

    payload = b'some\ntext'

    http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')

    return builder.create_warc_record('http://example.com/', 'response',
                                     payload=BytesIO(payload),
                                     length=len(payload),
                                     http_headers=http_headers)


# ============================================================================
@sample_record('response_2', RESPONSE_RECORD_2)
def sample_response_2(builder):
    payload = b'some\ntext'

    headers_list = [('Content-Type', 'text/plain; charset="UTF-8"'),
                    ('Content-Length', str(len(payload))),
                    ('Custom-Header', 'somevalue'),
                    ('Content-Encoding', 'x-unknown'),
                   ]

    http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')

    return builder.create_warc_record('http://example.com/', 'response',
                                     payload=BytesIO(payload),
                                     length=len(payload),
                                     http_headers=http_headers)


# ============================================================================
@sample_record('response_dns', DNS_RESPONSE_RECORD)
def sample_response_dns(builder):
    payload = b'''\
20170509000739
google.com.     185 IN  A   209.148.113.239
google.com.     185 IN  A   209.148.113.238
google.com.     185 IN  A   209.148.113.250
'''

    return builder.create_warc_record('dns:google.com', 'response',
                                     payload=BytesIO(payload))


# ============================================================================
@sample_record('resource_dns', DNS_RESOURCE_RECORD)
def sample_resource_dns(builder):
    payload = b'''\
20170509000739
google.com.     185 IN  A   209.148.113.239
google.com.     185 IN  A   209.148.113.238
google.com.     185 IN  A   209.148.113.250
'''

    return builder.create_warc_record('dns:google.com', 'resource',
                                     payload=BytesIO(payload))


# ============================================================================
@sample_record('request_1', REQUEST_RECORD)
def sample_request(builder):
    headers_list = [('User-Agent', 'foo'),
                    ('Host', 'example.com')]

    http_headers = StatusAndHeaders('GET / HTTP/1.0', headers_list, is_http_request=True)

    return builder.create_warc_record('http://example.com/', 'request',
                                     http_headers=http_headers)


# ============================================================================
@sample_record('request_2', REQUEST_RECORD_2)
def sample_request_from_buff(builder):
    payload = '\
POST /path HTTP/1.0\r\n\
Content-Type: application/json\r\n\
Content-Length: 17\r\n\
\r\n\
{"some": "value"}'.encode('utf-8')

    return builder.create_warc_record('http://example.com/', 'request',
                                     payload=BytesIO(payload),
                                     length=len(payload))


# ============================================================================
@sample_record('resource', RESOURCE_RECORD)
def sample_resource(builder):
    payload = b'some\ntext'

    return builder.create_warc_record('ftp://example.com/', 'resource',
                                      payload=BytesIO(payload),
                                      length=len(payload),
                                      warc_content_type='text/plain')


# ============================================================================
@sample_record('resource_no_ct', RESOURCE_RECORD_NO_CONTENT_TYPE)
def sample_resource_no_content_type(builder):
    payload = b'some\ntext'

    rec = builder.create_warc_record('ftp://example.com/', 'resource',
                                    payload=BytesIO(payload),
                                    length=len(payload))

    # default content-type added, but removing to match expected string
    assert rec.content_type == 'application/warc-record'

    rec.content_type = None
    return rec


# ============================================================================
@sample_record('metadata', METADATA_RECORD)
def sample_metadata(builder):

    payload_dict = {"metadata": OrderedDict([("nested", "obj"),
                                             ("list", [1, 2, 3]),
                                             ("length", "123")])}

    payload = json.dumps(payload_dict).encode('utf-8')

    return builder.create_warc_record('http://example.com/', 'metadata',
                                      payload=BytesIO(payload),
                                      length=len(payload),
                                      warc_content_type='application/json')


# ============================================================================
@sample_record('revisit_1', REVISIT_RECORD_1)
def sample_revisit_1(builder):
    return builder.create_revisit_record('http://example.com/',
                                         digest='sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O',
                                         refers_to_uri='http://example.com/foo',
                                         refers_to_date='1999-01-01T00:00:00Z')


# ============================================================================
@sample_record('revisit_2', REVISIT_RECORD_2)
def sample_revisit_2(builder):
    resp = sample_response(builder)

    return builder.create_revisit_record('http://example.com/',
                                        digest='sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O',
                                        refers_to_uri='http://example.com/foo',
                                        refers_to_date='1999-01-01T00:00:00Z',
                                        http_headers=resp.http_headers)


# ============================================================================
@sample_record('revisit_warc_1_1', REVISIT_RECORD_3)
def sample_revisit_1_1(builder):
    builder.warc_version = 'WARC/1.1'
    res = builder.create_revisit_record('http://example.com/',
                                         digest='sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O',
                                         refers_to_uri='http://example.com/foo',
                                         refers_to_date='1999-01-01T00:00:00Z')

    builder.warc_version = 'WARC/1.0'
    return res


# ============================================================================
# Fixture Setup
# ============================================================================
@pytest.fixture(params=['gzip', 'plain'])
def is_gzip(request):
    return request.param == 'gzip'

@pytest.fixture(params=['writer', 'builder'])
def builder_factory(request):
    def factory(writer, builder_cls=FixedTestRecordBuilder, **kwargs):
        if request.param == 'writer':
            return writer
        return builder_cls(**kwargs)
    return factory


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

    def test_generate_record(self, record_sampler, is_gzip, builder_factory):
        writer = FixedTestWARCWriter(gzip=is_gzip)

        builder = builder_factory(writer)
        record_maker, record_string = record_sampler
        record = record_maker(builder)

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

        # match original
        assert record.http_headers == parsed_record.http_headers

        if parsed_record.http_headers:
            assert rec_type in ('response', 'request', 'revisit')
        else:
            # empty revisit
            if rec_type == 'revisit':
                assert len(content_buff) == 0
            else:
                assert len(content_buff) == parsed_record.length

    def test_warcinfo_record(self, is_gzip, builder_factory):
        writer = FixedTestWARCWriter(gzip=is_gzip)
        builder = builder_factory(writer)

        record = sample_warcinfo(builder)

        writer.write_record(record)
        reader = DecompressingBufferedReader(writer.get_stream())

        parsed_record = ArcWarcRecordLoader().parse_record_stream(reader)

        assert parsed_record.rec_headers.get_header('WARC-Type') == 'warcinfo'
        assert parsed_record.rec_headers.get_header('Content-Type') == 'application/warc-fields'
        assert parsed_record.rec_headers.get_header('WARC-Filename') == 'testfile.warc.gz'
        assert parsed_record.rec_headers.get_header('WARC-Block-Digest') == 'sha1:GAD6P5BTZPRU57ICXEYUJZGCURZYABID'

        buff = parsed_record.content_stream().read().decode('utf-8')

        assert 'json-metadata: {"foo": "bar"}\r\n' in buff
        assert 'format: WARC File Format 1.0\r\n' in buff

    def test_request_response_concur(self, is_gzip, builder_factory):
        writer = BufferWARCWriter(gzip=is_gzip)
        builder = builder_factory(writer, builder_cls=RecordBuilder)

        resp = sample_response(builder)

        req = sample_request(builder)

        # test explicitly calling ensure_digest with block digest enabled on a record
        writer.ensure_digest(resp, block=True, payload=True)

        writer.write_request_response_pair(req, resp)

        stream = writer.get_stream()

        reader = ArchiveIterator(stream)
        resp, req = list(reader)

        resp_id = resp.rec_headers.get_header('WARC-Record-ID')
        req_id = req.rec_headers.get_header('WARC-Record-ID')

        assert resp_id != req_id
        assert resp_id == req.rec_headers.get_header('WARC-Concurrent-To')

    def test_response_warc_1_1(self, is_gzip, builder_factory):
        writer = BufferWARCWriter(gzip=is_gzip, warc_version='WARC/1.1')

        builder = builder_factory(writer, warc_version='WARC/1.1')
        resp = sample_response(builder)

        writer.write_record(resp)

        stream = writer.get_stream()

        reader = ArchiveIterator(stream)
        recs = list(reader)

        assert len(recs) == 1
        assert recs[0].rec_headers.protocol == 'WARC/1.1'

        # ISO 8601 date with fractional seconds (microseconds)
        assert '.' in recs[0].rec_headers['WARC-Date']
        assert len(recs[0].rec_headers['WARC-Date']) == 27

    def _conv_to_streaming_record(self, record_buff, rec_type):
        # strip-off the two empty \r\n\r\n added at the end of uncompressed record
        record_buff = record_buff[:-4]
        record_buff = re.sub('Content-Length:[^\r\n]+\r\n', '', record_buff, 1)

        # don't remove payload digest for revisit, as it can not be recomputed
        if rec_type != 'revisit':
            record_buff = re.sub('WARC-Payload-Digest:[^\r\n]+\r\n', '', record_buff, 1)
            assert 'WARC-Payload-Digest: ' not in record_buff

        record_buff = re.sub('WARC-Block-Digest:[^\r\n]+\r\n', 'WARC-Block-Digest: sha1:x-invalid\r\n', record_buff, 1)
        assert 'WARC-Block-Digest: sha1:x-invalid' in record_buff

        return record_buff

    def test_read_from_stream_no_content_length(self, record_sampler, is_gzip, builder_factory):
        writer = FixedTestWARCWriter(gzip=is_gzip)
        builder = builder_factory(writer)

        record_maker, record_string = record_sampler
        full_record = record_maker(builder)

        stream = BytesIO()
        record_no_cl = self._conv_to_streaming_record(record_string, full_record.rec_type)

        if is_gzip:
            gzip_stream = GzippingWrapper(stream)
            gzip_stream.write(record_no_cl.encode('utf-8'))
            gzip_stream.flush()
        else:
            stream.write(record_no_cl.encode('utf-8'))

        # parse to verify http headers + payload matches sample record
        # but not rec headers (missing content-length)
        stream.seek(0)
        parsed_record = ArcWarcRecordLoader().parse_record_stream(DecompressingBufferedReader(stream))

        if 'Content-Disposition' not in record_string:
            assert full_record.http_headers == parsed_record.http_headers
        assert full_record.raw_stream.read() == parsed_record.raw_stream.read()
        assert full_record.rec_headers != parsed_record.rec_headers

        # parse and write
        stream.seek(0)
        parsed_record = ArcWarcRecordLoader().parse_record_stream(DecompressingBufferedReader(stream))

        writer.write_record(parsed_record)

        stream = DecompressingBufferedReader(writer.get_stream())
        buff = stream.read()

        # assert written record matches expected response record
        # with content-length, digests computed
        assert buff.decode('utf-8') == record_string

    @pytest.mark.parametrize('filename', ['example.arc.gz', 'example.arc'])
    def test_arc2warc(self, filename, is_gzip):
        writer = FixedTestWARCWriter(gzip=is_gzip)

        def validate_warcinfo(record):
            assert record.rec_headers.get('WARC-Type') == 'warcinfo'
            assert record.rec_headers.get('WARC-Filename') == 'live-web-example.arc.gz'
            assert record.rec_headers.get('Content-Type') == 'text/plain'

        def validate_response(record):
            assert record.rec_headers.get('WARC-Type') == 'response'
            assert record.rec_headers.get('Content-Length') == '1591'
            assert record.length == 1591
            assert record.rec_headers.get('WARC-Target-URI') == 'http://example.com/'
            assert record.rec_headers.get('WARC-Date') == '2014-02-16T05:02:21Z'
            assert record.rec_headers.get('WARC-Block-Digest') == 'sha1:PEWDX5GTH66WU74WBPGFECIYBMPMP3FP'
            assert record.rec_headers.get('WARC-Payload-Digest') == 'sha1:B2LTWWPUOYAH7UIPQ7ZUPQ4VMBSVC36A'

        with open(get_test_file(filename), 'rb') as fh:
            for record in ArchiveIterator(fh, arc2warc=True):
                writer.write_record(record)

                if record.rec_type == 'response':
                    validate_response(record)

                if record.rec_type == 'warcinfo':
                    validate_warcinfo(record)

        raw_buff = writer.get_contents()
        self._validate_record_content_len(BytesIO(raw_buff))

        stream = writer.get_stream()

        records = list(ArchiveIterator(stream, arc2warc=False))
        assert len(records) == 2

        validate_warcinfo(records[0])
        validate_response(records[1])

        validate_warcinfo(records[0])

    def test_utf8_rewrite_content_adjust(self):
        UTF8_PAYLOAD = u'\
HTTP/1.0 200 OK\r\n\
Content-Type: text/plain; charset="UTF-8"\r\n\
Content-Disposition: attachment; filename="испытание.txt"\r\n\
Custom-Header: somevalue\r\n\
Unicode-Header: %F0%9F%93%81%20text%20%F0%9F%97%84%EF%B8%8F\r\n\
\r\n\
some\n\
text'

        content_length = len(UTF8_PAYLOAD.encode('utf-8'))

        UTF8_RECORD = u'\
WARC/1.0\r\n\
WARC-Type: response\r\n\
WARC-Record-ID: <urn:uuid:12345678-feb0-11e6-8f83-68a86d1772ce>\r\n\
WARC-Target-URI: http://example.com/\r\n\
WARC-Date: 2000-01-01T00:00:00Z\r\n\
WARC-Payload-Digest: sha1:B6QJ6BNJ3R4B23XXMRKZKHLPGJY2VE4O\r\n\
WARC-Block-Digest: sha1:KMUABC6URWIQ7QXCZDQ5FS6WIBBFRORR\r\n\
Content-Type: application/http; msgtype=response\r\n\
Content-Length: {0}\r\n\
\r\n\
{1}\r\n\
\r\n\
'.format(content_length, UTF8_PAYLOAD)

        assert(content_length == 226)

        record = ArcWarcRecordLoader().parse_record_stream(BytesIO(UTF8_RECORD.encode('utf-8')))

        writer = BufferWARCWriter(gzip=False)
        writer.write_record(record)

        raw_buff = writer.get_contents()
        assert raw_buff.decode('utf-8') == RESPONSE_RECORD_UNICODE_HEADERS

        for record in ArchiveIterator(writer.get_stream()):
            assert record.length == 268

    def test_identity(self):
        """ read(write(record)) should yield record """
        payload = b'foobar'
        writer = BufferWARCWriter(gzip=True)
        httpHeaders = StatusAndHeaders('GET / HTTP/1.1', {}, is_http_request=True)
        warcHeaders = {'Foo': 'Bar'}
        record = writer.create_warc_record('http://example.com/', 'request',
                payload=BytesIO(payload),
                warc_headers_dict=warcHeaders, http_headers=httpHeaders)

        writer.write_record(record)

        for new_rec in ArchiveIterator(writer.get_stream()):
            assert new_rec.rec_type == record.rec_type
            assert new_rec.rec_headers == record.rec_headers
            assert new_rec.content_type == record.content_type
            assert new_rec.length == record.length
            assert new_rec.http_headers == record.http_headers
            assert new_rec.raw_stream.read() == payload

