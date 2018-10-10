import threading

from io import BytesIO

from six.moves import http_client as httplib

from contextlib import contextmanager

from array import array

from warcio.utils import to_native_str, BUFF_SIZE, open
from warcio.warcwriter import WARCWriter, BufferWARCWriter

from tempfile import SpooledTemporaryFile


# ============================================================================
orig_connection = httplib.HTTPConnection


# ============================================================================
class RecordingStream(object):
    def __init__(self, fp, recorder):
        self.fp = fp
        self.recorder = recorder

        self.recorder.set_remote_ip(self._get_remote_ip())

    def _get_remote_ip(self):
        try:
            fp = self.fp
            # for python 3, need to get 'raw' fp
            if hasattr(fp, 'raw'):  #pragma: no cover
                fp = fp.raw

            socket = fp._sock

            # wrapped ssl socket
            if hasattr(socket, 'socket'):
                socket = socket.socket

            return socket.getpeername()[0]

        except Exception:  #pragma: no cover
            return None

    # Used in PY2 Only
    def read(self, amt=None):  #pragma: no cover
        buff = self.fp.read(amt)
        self.recorder.write_response(buff)
        return buff

    # Used in PY3 Only
    def readinto(self, buff):  #pragma: no cover
        res = self.fp.readinto(buff)
        self.recorder.write_response(buff)
        return res

    def readline(self, maxlen=-1):
        line = self.fp.readline(maxlen)
        self.recorder.write_response(line)
        return line

    def close(self):
        self.recorder.done()
        return self.fp.close()

    def flush(self):
        return self.fp.flush()


# ============================================================================
class RecordingHTTPResponse(httplib.HTTPResponse):
    def __init__(self, recorder, *args, **kwargs):
        httplib.HTTPResponse.__init__(self, *args, **kwargs)
        self.fp = RecordingStream(self.fp, recorder)


# ============================================================================
class RecordingHTTPConnection(httplib.HTTPConnection):
    local = threading.local()

    def __init__(self, *args, **kwargs):
        orig_connection.__init__(self, *args, **kwargs)
        if hasattr(self.local, 'recorder'):
            self.recorder = self.local.recorder
        else:
            self.recorder = None

        def make_recording_response(*args, **kwargs):
            return RecordingHTTPResponse(self.recorder, *args, **kwargs)

        if self.recorder:
            self.response_class = make_recording_response

    def send(self, data):
        if not self.recorder:
            orig_connection.send(self, data)
            return

        def send_request(buff):
            if not self.recorder.url:
                url = self._extract_url(buff)
                self.recorder.url = url

            orig_connection.send(self, buff)
            self.recorder.write_request(buff)

        # if sending request body as stream
        if hasattr(data, 'read') and not isinstance(data, array):
            while True:
                buff = data.read(BUFF_SIZE)
                if not buff:
                    break

                send_request(buff)
        else:
            send_request(data)

    def request(self, *args, **kwargs):
        if self.recorder:
            self.recorder.start()
        return orig_connection.request(self, *args, **kwargs)

    def _extract_url(self, data):
        buff = BytesIO(data)
        line = to_native_str(buff.readline(), 'latin-1')

        path = line.split(' ', 2)[1]

        scheme = 'https' if self.default_port == 443 else 'http'
        url = scheme + '://' + self.host
        if self.port != self.default_port:
            url += ':' + str(self.port)

        url += path
        return url


# ============================================================================
class RequestRecorder(object):
    def __init__(self, writer, filter_func=None):
        self.writer = writer
        self.filter_func = filter_func
        self.request_out = None
        self.response_out = None
        self.url = None
        self.lock = threading.Lock()
        self.warc_headers = {}

    def start(self):
        self.request_out = self._create_buffer()
        self.response_out = self._create_buffer()
        self.url = None

    def _create_buffer(self):
        return SpooledTemporaryFile(BUFF_SIZE)

    def set_remote_ip(self, remote_ip):
        if remote_ip:
            self.warc_headers['WARC-IP-Address'] = remote_ip

    def write_request(self, buff):
        self.request_out.write(buff)

    def write_response(self, buff):
        self.response_out.write(buff)

    def _create_record(self, out, record_type):
        length = out.tell()
        out.seek(0)
        return self.writer.create_warc_record(
                warc_headers_dict=self.warc_headers,
                uri=self.url,
                record_type=record_type,
                payload=out,
                length=length)

    def done(self):
        try:
            request = self._create_record(self.request_out, 'request')
            response = self._create_record(self.response_out, 'response')

            if self.filter_func:
                request, response = self.filter_func(request, response, self)
                if not request or not response:
                    return

            with self.lock:
                self.writer.write_request_response_pair(request, response)
        finally:
            self.request_out.close()
            self.response_out.close()


# ============================================================================
httplib.HTTPConnection = RecordingHTTPConnection
# ============================================================================

@contextmanager
def capture_http(warc_writer=None, filter_func=None, append=True,
                **kwargs):
    out = None
    if warc_writer == None:
        if 'gzip' not in kwargs:
            kwargs['gzip'] = False

        warc_writer = BufferWARCWriter(**kwargs)

    if isinstance(warc_writer, str):
        out = open(warc_writer, 'ab' if append else 'xb')
        warc_writer = WARCWriter(out, **kwargs)

    try:
        recorder = RequestRecorder(warc_writer, filter_func)
        RecordingHTTPConnection.local.recorder = recorder
        yield warc_writer

    finally:
        RecordingHTTPConnection.local.recorder = None
        if out:
            out.close()


