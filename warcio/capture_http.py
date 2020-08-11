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
        if self.fp:
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
            self.recorder.extract_url(buff, self.host, self.port, self.default_port)

            orig_connection.send(self, buff)
            self.recorder.write_request(buff)

        # if sending request body as stream
        # (supported via httplib but seems unused via higher-level apis)
        if hasattr(data, 'read') and not isinstance(data, array):  #pragma: no cover
            while True:
                buff = data.read(BUFF_SIZE)
                if not buff:
                    break

                send_request(buff)
        else:
            send_request(data)

    def _tunnel(self, *args, **kwargs):
        if self.recorder:
            self.recorder.start_tunnel()

        return orig_connection._tunnel(self, *args, **kwargs)

    def putrequest(self, *args, **kwargs):
        if self.recorder:
            self.recorder.start()
        return orig_connection.putrequest(self, *args, **kwargs)


# ============================================================================
class RequestRecorder(object):
    def __init__(self, writer, filter_func=None, record_ip=True):
        self.writer = writer
        self.filter_func = filter_func
        self.request_out = None
        self.response_out = None
        self.url = None
        self.connect_host = self.connect_port = None
        self.started_req = False
        self.first_line_read = False
        self.lock = threading.Lock()
        self.warc_headers = {}
        self.record_ip = record_ip

    def start_tunnel(self):
        self.connect_host = self.connect_port = None
        self.started_req = False
        self.first_line_read = False

    def start(self):
        self.request_out = self._create_buffer()
        self.response_out = self._create_buffer()
        self.url = None
        self.started_req = True
        self.first_line_read = False

    def _create_buffer(self):
        return SpooledTemporaryFile(BUFF_SIZE)

    def set_remote_ip(self, remote_ip):
        if self.record_ip and remote_ip:  #pragma: no cover
            self.warc_headers['WARC-IP-Address'] = remote_ip

    def write_request(self, buff):
        if self.started_req:
            self.request_out.write(buff)

    def write_response(self, buff):
        if self.started_req:
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
        if not self.started_req:
            return

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

    def extract_url(self, data, host, port, default_port):
        if self.first_line_read:
            return

        self.first_line_read = True
        buff = BytesIO(data)
        line = to_native_str(buff.readline(), 'latin-1')

        parts = line.split(' ', 2)
        verb = parts[0]
        path = parts[1]

        if verb == "CONNECT":
            parts = path.split(":", 1)
            self.connect_host = parts[0]
            self.connect_port = int(parts[1]) if len(parts) > 1 else default_port
            self.warc_headers['WARC-Proxy-Host'] = "https://{0}:{1}".format(host, port)
            return

        if self.connect_host:
            host = self.connect_host

        if self.connect_port:
            port = self.connect_port

        if path.startswith(('http:', 'https:')):
            self.warc_headers['WARC-Proxy-Host'] = "http://{0}:{1}".format(host, port)
            self.url = path
            return

        scheme = 'https' if default_port == 443 else 'http'
        self.url = scheme + '://' + host
        if port != default_port:
            self.url += ':' + str(port)

        self.url += path


# ============================================================================
httplib.HTTPConnection = RecordingHTTPConnection
# ============================================================================

@contextmanager
def capture_http(warc_writer=None, filter_func=None, append=True,
                record_ip=True, **kwargs):
    out = None
    if warc_writer == None:
        if 'gzip' not in kwargs:
            kwargs['gzip'] = False

        warc_writer = BufferWARCWriter(**kwargs)

    if isinstance(warc_writer, str):
        out = open(warc_writer, 'ab' if append else 'xb')
        warc_writer = WARCWriter(out, **kwargs)

    try:
        recorder = RequestRecorder(warc_writer, filter_func, record_ip=record_ip)
        RecordingHTTPConnection.local.recorder = recorder
        yield warc_writer

    finally:
        RecordingHTTPConnection.local.recorder = None
        if out:
            out.close()


