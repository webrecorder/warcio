import tempfile
import base64
import hashlib
import datetime
import zlib
import six

from socket import gethostname
from io import BytesIO

from warcio.utils import to_native_str, BUFF_SIZE
from warcio.timeutils import datetime_to_iso_date

from warcio.statusandheaders import StatusAndHeadersParser, StatusAndHeaders

from warcio.recordloader import ArcWarcRecord, ArcWarcRecordLoader


# ============================================================================
class BaseWARCWriter(object):
    WARC_RECORDS = {'warcinfo': 'application/warc-fields',
         'response': 'application/http; msgtype=response',
         'revisit': 'application/http; msgtype=response',
         'request': 'application/http; msgtype=request',
         'metadata': 'application/warc-fields',
        }

    REVISIT_PROFILE = 'http://netpreserve.org/warc/1.0/revisit/identical-payload-digest'

    WARC_1_0 = 'WARC/1.0'
    WARC_1_1 = 'WARC/1.1'

    # default warc version
    WARC_VERSION = WARC_1_0

    NO_PAYLOAD_DIGEST_TYPES = ('warcinfo', 'revisit')
    NO_BLOCK_DIGEST_TYPES = ('warcinfo')

    def __init__(self, gzip=True, *args, **kwargs):
        self.gzip = gzip
        self.hostname = gethostname()

        self.parser = StatusAndHeadersParser([], verify=False)

        self.warc_version = self._parse_warc_version(kwargs.get('warc_version'))
        self.header_filter = kwargs.get('header_filter')

    def _parse_warc_version(self, version):
        if not version:
            return self.WARC_VERSION

        version = str(version)
        if version.startswith('WARC/'):
            return version

        return 'WARC/' + version

    @classmethod
    def _iter_stream(cls, stream):
        while True:
            buf = stream.read(BUFF_SIZE)
            if not buf:
                return

            yield buf

    def ensure_digest(self, record, block=True, payload=True):
        if block:
            if (record.rec_headers.get_header('WARC-Block-Digest') or
                (record.rec_type in self.NO_BLOCK_DIGEST_TYPES)):
                block = False

        if payload:
            if (record.rec_headers.get_header('WARC-Payload-Digest') or
                (record.rec_type in self.NO_PAYLOAD_DIGEST_TYPES)):
                payload = False

        block_digester = self._create_digester() if block else None
        payload_digester = self._create_digester() if payload else None

        has_length = (record.length is not None)

        if not block_digester and not payload_digester and has_length:
            return

        temp_file = None
        try:
            # force buffering if no length is set
            assert(has_length)
            pos = record.raw_stream.tell()
            record.raw_stream.seek(pos)
        except:
            pos = 0
            temp_file = self._create_temp_file()

        if block_digester and record.http_headers and record.http_headers.headers_buff:
            block_digester.update(record.http_headers.headers_buff)

        for buf in self._iter_stream(record.raw_stream):
            if block_digester:
                block_digester.update(buf)

            if payload_digester:
                payload_digester.update(buf)

            if temp_file:
                temp_file.write(buf)

        if temp_file:
            record.payload_length = temp_file.tell()
            temp_file.seek(0)
            record._orig_stream = record.raw_stream
            record.raw_stream = temp_file
        else:
            record.raw_stream.seek(pos)

        if payload_digester:
            record.rec_headers.add_header('WARC-Payload-Digest', str(payload_digester))

        if block_digester:
            record.rec_headers.add_header('WARC-Block-Digest', str(block_digester))

    def _create_digester(self):
        return Digester('sha1')

    def write_request_response_pair(self, req, resp, params=None):
        url = resp.rec_headers.get_header('WARC-Target-URI')
        dt = resp.rec_headers.get_header('WARC-Date')

        req.rec_headers.replace_header('WARC-Target-URI', url)
        req.rec_headers.replace_header('WARC-Date', dt)

        resp_id = resp.rec_headers.get_header('WARC-Record-ID')
        if resp_id:
            req.rec_headers.add_header('WARC-Concurrent-To', resp_id)

        self._do_write_req_resp(req, resp, params)

    def write_record(self, record, params=None):  #pragma: no cover
        raise NotImplemented()

    def _do_write_req_resp(self, req, resp, params):  #pragma: no cover
        raise NotImplemented()

    def create_warcinfo_record(self, filename, info):
        warc_headers = StatusAndHeaders(self.warc_version, [])
        warc_headers.add_header('WARC-Type', 'warcinfo')
        warc_headers.add_header('WARC-Record-ID', self._make_warc_id())
        if filename:
            warc_headers.add_header('WARC-Filename', filename)
        warc_headers.add_header('WARC-Date', self.curr_warc_date())

        warcinfo = BytesIO()
        for name, value in six.iteritems(info):
            if not value:
                continue

            line = name + ': ' + str(value) + '\r\n'
            warcinfo.write(line.encode('utf-8'))

        length = warcinfo.tell()
        warcinfo.seek(0)

        return self.create_warc_record('', 'warcinfo',
                                       warc_headers=warc_headers,
                                       payload=warcinfo,
                                       length=length)

    def create_revisit_record(self, uri, digest, refers_to_uri, refers_to_date,
                              http_headers=None, warc_headers_dict={}):

        assert digest, 'Digest can not be empty'

        record = self.create_warc_record(uri, 'revisit', http_headers=http_headers,
                                                         warc_headers_dict=warc_headers_dict)

        record.rec_headers.add_header('WARC-Profile', self.REVISIT_PROFILE)

        record.rec_headers.add_header('WARC-Refers-To-Target-URI', refers_to_uri)
        record.rec_headers.add_header('WARC-Refers-To-Date', refers_to_date)

        record.rec_headers.add_header('WARC-Payload-Digest', digest)

        return record

    def create_warc_record(self, uri, record_type,
                           payload=None,
                           length=None,
                           warc_content_type='',
                           warc_headers_dict={},
                           warc_headers=None,
                           http_headers=None):

        if payload and not http_headers:
            loader = ArcWarcRecordLoader()
            http_headers = loader.load_http_headers(record_type, uri, payload, length)
            if http_headers and length is not None:
                length -= payload.tell()

        if not payload:
            payload = BytesIO()
            length = 0

        if not warc_headers:
            warc_headers = self._init_warc_headers(uri, record_type, warc_headers_dict)

        # compute Content-Type
        if not warc_content_type:
            warc_content_type = warc_headers.get_header('Content-Type')

            if not warc_content_type:
                warc_content_type = self.WARC_RECORDS.get(record_type,
                                                'application/warc-record')

        record = ArcWarcRecord('warc', record_type, warc_headers, payload,
                               http_headers, warc_content_type, length)

        record.payload_length = length

        self.ensure_digest(record, block=False, payload=True)

        return record

    def _init_warc_headers(self, uri, record_type, warc_headers_dict):
        warc_headers = StatusAndHeaders(self.warc_version, list(warc_headers_dict.items()))
        warc_headers.replace_header('WARC-Type', record_type)
        if not warc_headers.get_header('WARC-Record-ID'):
            warc_headers.add_header('WARC-Record-ID', self._make_warc_id())

        if uri:
            warc_headers.replace_header('WARC-Target-URI', uri)

        if not warc_headers.get_header('WARC-Date'):
            warc_headers.add_header('WARC-Date', self.curr_warc_date())

        return warc_headers

    def _set_header_buff(self, record):
        # HTTP headers %-encoded as ascii (see to_ascii_bytes for more info)
        headers_buff = record.http_headers.to_ascii_bytes(self.header_filter)
        record.http_headers.headers_buff = headers_buff

    def _write_warc_record(self, out, record):
        if self.gzip:
            out = GzippingWrapper(out)

        if record.http_headers:
            self._set_header_buff(record)

        # Content-Length is None/unknown
        # Fix record by: buffering and recomputing all digests and length
        # (since no length, can't trust existing digests)
        # Also remove content-type for consistent header ordering
        if record.length is None:
            record.rec_headers.remove_header('WARC-Block-Digest')
            if record.rec_type != 'revisit':
                record.rec_headers.remove_header('WARC-Payload-Digest')
            record.rec_headers.remove_header('Content-Type')

            self.ensure_digest(record, block=True, payload=True)

            record.length = record.payload_length

        # ensure digests are set
        else:
            self.ensure_digest(record, block=True, payload=True)

        if record.content_type != None:
            # ensure proper content type
            record.rec_headers.replace_header('Content-Type', record.content_type)

        if record.rec_type == 'revisit':
            http_headers_only = True
        else:
            http_headers_only = False

        # compute Content-Length
        if record.http_headers and record.payload_length >= 0:
            actual_len = 0

            if record.http_headers:
                actual_len = len(record.http_headers.headers_buff)

            if not http_headers_only:
                actual_len += record.payload_length

            record.length = actual_len

        record.rec_headers.replace_header('Content-Length', str(record.length))

        # write record headers -- encoded as utf-8
        # WARC headers can be utf-8 per spec
        out.write(record.rec_headers.to_bytes(encoding='utf-8'))

        # write headers buffer, if any
        if record.http_headers:
            out.write(record.http_headers.headers_buff)

        if not http_headers_only:
            try:
                for buf in self._iter_stream(record.raw_stream):
                    out.write(buf)
            finally:
                if hasattr(record, '_orig_stream'):
                    record.raw_stream.close()
                    record.raw_stream = record._orig_stream

        # add two lines
        out.write(b'\r\n\r\n')

        out.flush()

    def curr_warc_date(self):
        use_micros = (self.warc_version >= self.WARC_1_1)
        return self._make_warc_date(use_micros=use_micros)

    @classmethod
    def _make_warc_id(cls):
        return StatusAndHeadersParser.make_warc_id()

    @classmethod
    def _make_warc_date(cls, use_micros=False):
        return datetime_to_iso_date(datetime.datetime.utcnow(), use_micros=use_micros)

    @classmethod
    def _create_temp_file(cls):
        return tempfile.SpooledTemporaryFile(max_size=512*1024)


# ============================================================================
class GzippingWrapper(object):
    def __init__(self, out):
        self.compressor = zlib.compressobj(9, zlib.DEFLATED, zlib.MAX_WBITS + 16)
        self.out = out

    def write(self, buff):
        #if isinstance(buff, str):
        #    buff = buff.encode('utf-8')
        buff = self.compressor.compress(buff)
        self.out.write(buff)

    def flush(self):
        buff = self.compressor.flush()
        self.out.write(buff)
        self.out.flush()


# ============================================================================
class Digester(object):
    def __init__(self, type_='sha1'):
        self.type_ = type_
        self.digester = hashlib.new(type_)

    def update(self, buff):
        self.digester.update(buff)

    def __str__(self):
        return self.type_ + ':' + to_native_str(base64.b32encode(self.digester.digest()))


# ============================================================================
class WARCWriter(BaseWARCWriter):
    def __init__(self, filebuf, *args, **kwargs):
        super(WARCWriter, self).__init__(*args, **kwargs)
        self.out = filebuf

    def write_record(self, record, params=None):
        self._write_warc_record(self.out, record)

    def _do_write_req_resp(self, req, resp, params):
        self._write_warc_record(self.out, resp)
        self._write_warc_record(self.out, req)


# ============================================================================
class BufferWARCWriter(WARCWriter):
    def __init__(self, *args, **kwargs):
        out = self._create_temp_file()
        super(BufferWARCWriter, self).__init__(out, *args, **kwargs)

    def get_contents(self):
        pos = self.out.tell()
        self.out.seek(0)
        buff = self.out.read()
        self.out.seek(pos)
        return buff

    def get_stream(self):
        self.out.seek(0)
        return self.out


