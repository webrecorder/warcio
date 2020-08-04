import datetime
import six
import tempfile

from io import BytesIO

from warcio.recordloader import ArcWarcRecord, ArcWarcRecordLoader
from warcio.statusandheaders import StatusAndHeadersParser, StatusAndHeaders
from warcio.timeutils import datetime_to_iso_date
from warcio.utils import to_native_str, BUFF_SIZE, Digester

#=================================================================
class RecordBuilder(object):
    REVISIT_PROFILE = 'http://netpreserve.org/warc/1.0/revisit/identical-payload-digest'
    REVISIT_PROFILE_1_1 = 'http://netpreserve.org/warc/1.1/revisit/identical-payload-digest'

    WARC_1_0 = 'WARC/1.0'
    WARC_1_1 = 'WARC/1.1'

    # default warc version
    WARC_VERSION = WARC_1_0

    WARC_RECORDS = {'warcinfo': 'application/warc-fields',
         'response': 'application/http; msgtype=response',
         'revisit': 'application/http; msgtype=response',
         'request': 'application/http; msgtype=request',
         'metadata': 'application/warc-fields',
        }

    NO_PAYLOAD_DIGEST_TYPES = ('warcinfo', 'revisit')


    def __init__(self, warc_version=None, header_filter=None):
        self.warc_version = self._parse_warc_version(warc_version)

        self.header_filter = header_filter

    def create_warcinfo_record(self, filename, info):
        warc_headers = StatusAndHeaders('', [], protocol=self.warc_version)
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
                              http_headers=None, warc_headers_dict=None):

        assert digest, 'Digest can not be empty'
        if warc_headers_dict is None:
            warc_headers_dict = dict()

        record = self.create_warc_record(uri, 'revisit', http_headers=http_headers,
                                                         warc_headers_dict=warc_headers_dict)

        revisit_profile = self.REVISIT_PROFILE_1_1 if self.warc_version == self.WARC_1_1 else self.REVISIT_PROFILE
        record.rec_headers.add_header('WARC-Profile', revisit_profile)

        record.rec_headers.add_header('WARC-Refers-To-Target-URI', refers_to_uri)
        record.rec_headers.add_header('WARC-Refers-To-Date', refers_to_date)

        record.rec_headers.add_header('WARC-Payload-Digest', digest)

        return record

    def create_warc_record(self, uri, record_type,
                           payload=None,
                           length=None,
                           warc_content_type='',
                           warc_headers_dict=None,
                           warc_headers=None,
                           http_headers=None):
        if warc_headers_dict is None:
            warc_headers_dict = dict()

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
        warc_headers = StatusAndHeaders('', list(warc_headers_dict.items()), protocol=self.warc_version)
        warc_headers.replace_header('WARC-Type', record_type)
        if not warc_headers.get_header('WARC-Record-ID'):
            warc_headers.add_header('WARC-Record-ID', self._make_warc_id())

        if uri:
            warc_headers.replace_header('WARC-Target-URI', uri)

        if not warc_headers.get_header('WARC-Date'):
            warc_headers.add_header('WARC-Date', self.curr_warc_date())

        return warc_headers

    def curr_warc_date(self):
        use_micros = (self.warc_version >= self.WARC_1_1)
        return self._make_warc_date(use_micros=use_micros)

    def _parse_warc_version(self, version):
        if not version:
            return self.WARC_VERSION

        version = str(version)
        if version.startswith('WARC/'):
            return version

        return 'WARC/' + version

    @classmethod
    def _make_warc_id(cls):
        return StatusAndHeadersParser.make_warc_id()

    @classmethod
    def _make_warc_date(cls, use_micros=False):
        return datetime_to_iso_date(datetime.datetime.utcnow(), use_micros=use_micros)

    def ensure_digest(self, record, block=True, payload=True):
        if block:
            if record.rec_headers.get_header('WARC-Block-Digest'):
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

        if block_digester and record.http_headers:
            if not record.http_headers.headers_buff:
                record.http_headers.compute_headers_buffer(self.header_filter)
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

    @staticmethod
    def _iter_stream(stream):
        while True:
            buf = stream.read(BUFF_SIZE)
            if not buf:
                return

            yield buf

    @staticmethod
    def _create_digester():
        return Digester('sha1')

    @staticmethod
    def _create_temp_file():
        return tempfile.SpooledTemporaryFile(max_size=512*1024)
