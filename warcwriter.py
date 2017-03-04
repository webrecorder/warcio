import tempfile
import uuid
import base64
import hashlib
import datetime
import zlib
import six

from socket import gethostname
from io import BytesIO

from pywb.warclib.utils import to_native_str, BUFF_SIZE
from pywb.warclib.timeutils import datetime_to_iso_date

from pywb.warclib.statusandheaders import StatusAndHeadersParser, StatusAndHeaders

from pywb.warclib.recordloader import ArcWarcRecord


# ============================================================================
class BaseWARCWriter(object):
    WARC_RECORDS = {'warcinfo': 'application/warc-fields',
         'response': 'application/http; msgtype=response',
         'revisit': 'application/http; msgtype=response',
         'request': 'application/http; msgtype=request',
         'metadata': 'application/warc-fields',
        }

    REVISIT_PROFILE = 'http://netpreserve.org/warc/1.0/revisit/identical-payload-digest'

    WARC_VERSION = 'WARC/1.0'

    def __init__(self, gzip=True, *args, **kwargs):
        self.gzip = gzip
        self.hostname = gethostname()

        self.parser = StatusAndHeadersParser([], verify=False)
        self.warc_version = kwargs.get('warc_version', self.WARC_VERSION)

    @classmethod
    def _iter_stream(cls, stream):
        while True:
            buf = stream.read(BUFF_SIZE)
            if not buf:
                return

            yield buf

    def ensure_digest(self, record, block=True, payload=True):
        if block and record.rec_headers.get_header('WARC-Block-Digest'):
            block = False

        if payload and record.rec_headers.get_header('WARC-Payload-Digest'):
            payload = False

        block_digester = self._create_digester() if block else None
        payload_digester = self._create_digester() if payload else None

        if not block_digester and not payload_digester:
            return

        if hasattr(record.stream, 'seek'):
            temp_file = None
            pos = record.stream.tell()
        else:
            temp_file = self._create_temp_file()
            pos = 0

        if block_digester and record.status_headers and record.status_headers.headers_buff:
            block_digester.update(record.status_headers.headers_buff)

        for buf in self._iter_stream(record.stream):
            if block_digester:
                block_digester.update(buf)

            if payload_digester:
                payload_digester.update(buf)

            if temp_file:
                temp_file.write(buf)

        if temp_file:
            record.payload_length = temp_file.tell()
            temp_file.seek(0)
            record._orig_stream = record.stream
            record.stream = temp_file
        else:
            record.stream.seek(pos)

        if block_digester:
            record.rec_headers.add_header('WARC-Block-Digest', str(block_digester))

        if payload_digester:
            record.rec_headers.add_header('WARC-Payload-Digest', str(payload_digester))

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
        warc_headers.add_header('WARC-Date', self._make_warc_date())

        warcinfo = BytesIO()
        for n, v in six.iteritems(info):
            self._header(warcinfo, n, v)

        length = warcinfo.tell()
        warcinfo.seek(0)

        return self.create_warc_record('', 'warcinfo',
                                       warc_headers=warc_headers,
                                       payload=warcinfo,
                                       length=length)

    def create_revisit_record(self, uri, digest, refers_to_uri, refers_to_date,
                              status_headers=None):

        record = self.create_warc_record(uri, 'revisit', status_headers=status_headers)

        record.rec_headers.add_header('WARC-Profile', self.REVISIT_PROFILE)

        record.rec_headers.add_header('WARC-Refers-To-Target-URI', refers_to_uri)
        record.rec_headers.add_header('WARC-Refers-To-Date', refers_to_date)

        record.rec_headers.add_header('WARC-Payload-Digest', digest)

        return record

    def create_record_from_stream(self, record_stream, length):
        warc_headers = self.parser.parse(record_stream)

        return self.create_warc_record('', warc_headers.get_header('WARC-Type'),
                                       payload=record_stream,
                                       length=length,
                                       warc_headers=warc_headers)

    def create_warc_record(self, uri, record_type,
                           payload=None,
                           length=0,
                           warc_content_type='',
                           warc_headers_dict={},
                           warc_headers=None,
                           status_headers=None):

        if payload and not status_headers and record_type in ('response', 'request', 'revisit'):
            status_headers = self.parser.parse(payload)
            length -= payload.tell()

        if not payload:
            payload = BytesIO()
            length = 0

        if not warc_headers:
            warc_headers = self._init_warc_headers(uri, record_type, warc_headers_dict)

        record = ArcWarcRecord('warc', record_type, warc_headers, payload,
                               status_headers, warc_content_type, length)

        record.payload_length = length

        if record_type not in ('warcinfo', 'revisit'):
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
            warc_headers.add_header('WARC-Date', self._make_warc_date())

        return warc_headers

    def _set_header_buff(self, record):
        record.status_headers.headers_buff = record.status_headers.to_bytes()

    def _write_warc_record(self, out, record, adjust_cl=True):
        if self.gzip:
            out = GzippingWrapper(out)

        if record.status_headers:
            self._set_header_buff(record)

        # ensure digests are set
        if record.rec_type != 'warcinfo':
            self.ensure_digest(record, block=True, payload=False)

        # compute Content-Type
        content_type = record.rec_headers.get_header('Content-Type')

        if not content_type:
            content_type = record.content_type

            if not content_type:
                content_type = self.WARC_RECORDS.get(record.rec_headers.get_header('WARC-Type'))

            if content_type:
                record.rec_headers.replace_header('Content-Type', content_type)
                #self._header(out, 'Content-Type', content_type)

        if record.rec_type == 'revisit':
            http_headers_only = True
        else:
            http_headers_only = False

        # compute Content-Length
        if record.status_headers and record.payload_length >= 0:
            actual_len = 0

            if record.status_headers:
                actual_len = len(record.status_headers.headers_buff)

            if not http_headers_only:
                actual_len += record.payload_length

            record.length = actual_len

        record.rec_headers.replace_header('Content-Length', str(record.length))

        # write record headers
        out.write(record.rec_headers.to_bytes())

        # write headers buffer, if any
        if record.status_headers:
            out.write(record.status_headers.headers_buff)

        if not http_headers_only:
            try:
                for buf in self._iter_stream(record.stream):
                    out.write(buf)
            finally:
                if hasattr(record, '_orig_stream'):
                    record.stream.close()
                    record.stream = record._orig_stream

        # add two lines
        self._line(out, b'\r\n')

        # add three lines (1 for end of header, 2 for end of record)
        #self._line(out, b'Content-Length: 0\r\n\r\n')

        out.flush()

    def _header(self, out, name, value):
        if not value:
            return

        self._line(out, (name + ': ' + str(value)).encode('latin-1'))

    def _line(self, out, line):
        out.write(line + b'\r\n')

    @classmethod
    def _make_warc_id(cls, id_=None):
        if not id_:
            id_ = uuid.uuid1()
        return '<urn:uuid:{0}>'.format(id_)

    @classmethod
    def _make_warc_date(cls):
        return datetime_to_iso_date(datetime.datetime.utcnow())

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


