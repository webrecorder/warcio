import threading
from wsgiref.simple_server import make_server
from io import BytesIO
import time

# must be imported before 'requests'
from warcio.capture_http import capture_http
from pytest import raises
import requests

import json
import os
import tempfile

from warcio.archiveiterator import ArchiveIterator
from warcio.utils import BUFF_SIZE
from warcio.warcwriter import BufferWARCWriter, WARCWriter

# ==================================================================



# ==================================================================
class TestCaptureHttpBin(object):
    @classmethod
    def setup_class(cls):
        from httpbin import app as httpbin_app

        cls.temp_dir = tempfile.mkdtemp('warctest')

        server = make_server('localhost', 0, httpbin_app)
        addr, cls.port = server.socket.getsockname()

        def run():
            try:
                server.serve_forever()
            except  Exception as e:
                print(e)

        thread = threading.Thread(target=run)
        thread.daemon = True
        thread.start()
        time.sleep(0.1)

    @classmethod
    def teardown_class(cls):
        os.rmdir(cls.temp_dir)

    def test_get_no_capture(self):
        url = 'http://localhost:{0}/get?foo=bar'.format(self.port)
        res = requests.get(url, headers={'Host': 'httpbin.org'})

        assert res.json()['args'] == {'foo': 'bar'}

    def test_get(self):
        url = 'http://localhost:{0}/get?foo=bar'.format(self.port)
        with capture_http() as warc_writer:
            res = requests.get(url, headers={'Host': 'httpbin.org'})

        assert res.json()['args'] == {'foo': 'bar'}

        ai = ArchiveIterator(warc_writer.get_stream())
        response = next(ai)
        assert response.rec_type == 'response'
        assert response.rec_headers['WARC-Target-URI'] == url
        assert response.rec_headers['WARC-IP-Address'] == '127.0.0.1'
        assert res.json() == json.loads(response.content_stream().read().decode('utf-8'))

        request = next(ai)
        assert request.rec_type == 'request'
        assert request.rec_headers['WARC-Target-URI'] == url
        assert request.rec_headers['WARC-IP-Address'] == '127.0.0.1'

    def test_post_cache_to_file(self):
        warc_writer = BufferWARCWriter(gzip=False)

        random_bytes = os.urandom(BUFF_SIZE * 2)
        request_data = {"data": str(random_bytes)}

        url = 'http://localhost:{0}/anything'.format(self.port)
        with capture_http(warc_writer):
            res = requests.post(
                url,
                headers={'Host': 'httpbin.org'},
                json=request_data
            )

        assert res.json()["json"] == request_data

        ai = ArchiveIterator(warc_writer.get_stream())
        response = next(ai)
        assert response.rec_type == 'response'
        assert response.rec_headers['WARC-Target-URI'] == url
        assert response.rec_headers['WARC-IP-Address'] == '127.0.0.1'
        assert request_data == json.loads(response.content_stream().read().decode('utf-8'))["json"]

        request = next(ai)
        assert request.rec_type == 'request'
        assert request.rec_headers['WARC-Target-URI'] == url
        assert request.rec_headers['WARC-IP-Address'] == '127.0.0.1'

    def test_post_json(self):
        warc_writer = BufferWARCWriter(gzip=False)

        with capture_http(warc_writer):
            res = requests.post('http://localhost:{0}/post'.format(self.port),
                                headers={'Host': 'httpbin.org'},
                                json={'some': {'data': 'posted'}})

        assert res.json()['json'] == {'some': {'data': 'posted'}}

        # response
        ai = ArchiveIterator(warc_writer.get_stream())
        response = next(ai)
        assert response.rec_type == 'response'

        assert res.json() == json.loads(response.content_stream().read().decode('utf-8'))

        # request
        request = next(ai)
        assert request.rec_type == 'request'
        assert request.http_headers['Content-Type'] == 'application/json'

        data = request.content_stream().read().decode('utf-8')
        assert data == '{"some": {"data": "posted"}}'

    def test_post_stream(self):
        warc_writer = BufferWARCWriter(gzip=False)

        def nop_filter(request, response, recorder):
            assert request
            assert response
            return request, response

        postbuff = BytesIO(b'somedatatopost')

        url = 'http://localhost:{0}/post'.format(self.port)

        with capture_http(warc_writer, nop_filter):
            res = requests.post(url, data=postbuff)

        # response
        ai = ArchiveIterator(warc_writer.get_stream())
        response = next(ai)
        assert response.rec_type == 'response'
        assert response.rec_headers['WARC-Target-URI'] == url
        assert response.rec_headers['WARC-IP-Address'] == '127.0.0.1'

        assert res.json() == json.loads(response.content_stream().read().decode('utf-8'))

        # request
        request = next(ai)
        assert request.rec_type == 'request'
        assert request.rec_headers['WARC-Target-URI'] == url
        assert request.rec_headers['WARC-IP-Address'] == '127.0.0.1'

        data = request.content_stream().read().decode('utf-8')
        assert data == 'somedatatopost'

    def test_post_chunked(self):
        warc_writer = BufferWARCWriter(gzip=False)

        def nop_filter(request, response, recorder):
            assert request
            assert response
            return request, response

        def gen():
            return iter([b'some', b'data', b'to', b'post'])

        #url = 'http://localhost:{0}/post'.format(self.port)
        url = 'https://httpbin.org/post'

        with capture_http(warc_writer, nop_filter, record_ip=False):
            res = requests.post(url, data=gen(), headers={'Content-Type': 'application/json'})

        # response
        ai = ArchiveIterator(warc_writer.get_stream())
        response = next(ai)
        assert response.rec_type == 'response'
        assert response.rec_headers['WARC-Target-URI'] == url
        assert 'WARC-IP-Address' not in response.rec_headers

        assert res.json() == json.loads(response.content_stream().read().decode('utf-8'))

        # request
        request = next(ai)
        assert request.rec_type == 'request'
        assert request.rec_headers['WARC-Target-URI'] == url
        assert 'WARC-IP-Address' not in response.rec_headers

        data = request.content_stream().read().decode('utf-8')
        assert data == 'somedatatopost'

    def test_skip_filter(self):
        warc_writer = BufferWARCWriter(gzip=False)

        def skip_filter(request, response, recorder):
            assert request
            assert response
            return None, None

        with capture_http(warc_writer, skip_filter):
            res = requests.get('http://localhost:{0}/get?foo=bar'.format(self.port),
                               headers={'Host': 'httpbin.org'})

        assert res.json()['args'] == {'foo': 'bar'}

        # skipped, nothing written
        assert warc_writer.get_contents() == b''

    def test_capture_to_temp_file_append(self):
        full_path = os.path.join(self.temp_dir, 'example.warc.gz')

        url = 'http://localhost:{0}/get?foo=bar'.format(self.port)

        with capture_http(full_path):
            res = requests.get(url)

        with capture_http(full_path):
            res = requests.get(url)

        with open(full_path, 'rb') as stream:
            # response
            ai = ArchiveIterator(stream)
            response = next(ai)
            assert response.rec_type == 'response'
            assert response.rec_headers['WARC-Target-URI'] == url

            # request
            request = next(ai)
            assert request.rec_type == 'request'
            assert request.rec_headers['WARC-Target-URI'] == url

            response = next(ai)
            assert response.rec_type == 'response'
            assert response.rec_headers['WARC-Target-URI'] == url

            # request
            request = next(ai)
            assert request.rec_type == 'request'
            assert request.rec_headers['WARC-Target-URI'] == url

        os.remove(full_path)

    def test_error_capture_to_temp_file_no_append_no_overwrite(self):
        full_path = os.path.join(self.temp_dir, 'example2.warc.gz')

        url = 'http://localhost:{0}/get?foo=bar'.format(self.port)

        with capture_http(full_path, append=False):
            res = requests.get(url)

        with raises(OSError):
            with capture_http(full_path, append=False):
                res = requests.get(url)

        os.remove(full_path)

    def test_warc_1_1(self):
        full_path = os.path.join(self.temp_dir, 'example3.warc')

        url = 'http://localhost:{0}/get?foo=bar'.format(self.port)

        with capture_http(full_path, append=False, warc_version='1.1', gzip=False):
            res = requests.get(url)

        with open(full_path, 'rb') as stream:
            # response
            ai = ArchiveIterator(stream)
            response = next(ai)
            assert response.rec_headers.protocol == 'WARC/1.1'
            warc_date = response.rec_headers['WARC-Date']

            # ISO 8601 date with fractional seconds (microseconds)
            assert '.' in warc_date
            assert len(warc_date) == 27

        os.remove(full_path)

    def test_remote(self):
        with capture_http(warc_version='1.1', gzip=True) as writer:
            requests.get('http://example.com/')
            requests.get('https://google.com/')

        expected = [('http://example.com/', 'response', True),
                    ('http://example.com/', 'request', True),
                    ('https://google.com/', 'response', True),
                    ('https://google.com/', 'request', True),
                    ('https://www.google.com/', 'response', True),
                    ('https://www.google.com/', 'request', True)
                   ]

        actual = [
                  (record.rec_headers['WARC-Target-URI'],
                   record.rec_type,
                   'WARC-IP-Address' in record.rec_headers)

                  for record in ArchiveIterator(writer.get_stream())
                 ]

        assert actual == expected

