import threading
from wsgiref.simple_server import make_server
from io import BytesIO
import time

# must be imported before 'requests'
from warcio.record_http import record_http
import requests

import json
import os
import tempfile

from warcio.archiveiterator import ArchiveIterator
from warcio.utils import BUFF_SIZE
from warcio.warcwriter import BufferWARCWriter, WARCWriter


# ==================================================================
class TestRecordHttpBin(object):
    @classmethod
    def setup_class(cls):
        from httpbin import app as httpbin_app

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

    def test_get_no_record(self):
        url = 'http://localhost:{0}/get?foo=bar'.format(self.port)
        res = requests.get(url, headers={'Host': 'httpbin.org'})

        assert res.json()['args'] == {'foo': 'bar'}

    def test_get(self):
        warc_writer = BufferWARCWriter(gzip=False)

        url = 'http://localhost:{0}/get?foo=bar'.format(self.port)
        with record_http(warc_writer):
            res = requests.get(url, headers={'Host': 'httpbin.org'})

        assert res.json()['args'] == {'foo': 'bar'}

        ai = ArchiveIterator(warc_writer.get_stream())
        response = next(ai)
        assert response.rec_type == 'response'
        assert response.rec_headers['WARC-Target-URI'] == url
        assert res.json() == json.loads(response.content_stream().read().decode('utf-8'))

        request = next(ai)
        assert request.rec_type == 'request'
        assert request.rec_headers['WARC-Target-URI'] == url

    def test_get_cache_to_file(self):
        warc_writer = BufferWARCWriter(gzip=False)

        url = 'http://localhost:{0}/bytes/{1}'.format(self.port, BUFF_SIZE * 2)
        with record_http(warc_writer):
            res = requests.get(url, headers={'Host': 'httpbin.org'})

        assert len(res.content) == BUFF_SIZE * 2

        ai = ArchiveIterator(warc_writer.get_stream())
        response = next(ai)
        assert response.rec_type == 'response'
        assert response.rec_headers['WARC-Target-URI'] == url
        assert res.content == response.content_stream().read()

        request = next(ai)
        assert request.rec_type == 'request'
        assert request.rec_headers['WARC-Target-URI'] == url

    def test_post_json(self):
        warc_writer = BufferWARCWriter(gzip=False)

        with record_http(warc_writer):
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

        def nop_filter(request, response, warc_writer):
            assert request
            assert response
            return request, response

        postbuff = BytesIO(b'somedatatopost')

        url = 'http://localhost:{0}/post'.format(self.port)

        with record_http(warc_writer, nop_filter):
            res = requests.post(url, data=postbuff)

        # response
        ai = ArchiveIterator(warc_writer.get_stream())
        response = next(ai)
        assert response.rec_type == 'response'
        assert response.rec_headers['WARC-Target-URI'] == url

        assert res.json() == json.loads(response.content_stream().read().decode('utf-8'))

        # request
        request = next(ai)
        assert request.rec_type == 'request'
        assert request.rec_headers['WARC-Target-URI'] == url

        data = request.content_stream().read().decode('utf-8')
        assert data == 'somedatatopost'


    def test_skip_filter(self):
        warc_writer = BufferWARCWriter(gzip=False)

        def skip_filter(request, response, warc_writer):
            assert request
            assert response
            return None, None

        with record_http(warc_writer, skip_filter):
            res = requests.get('http://localhost:{0}/get?foo=bar'.format(self.port),
                               headers={'Host': 'httpbin.org'})

        assert res.json()['args'] == {'foo': 'bar'}

        # skipped, nothing written
        assert warc_writer.get_contents() == b''

    def test_record_to_temp_file(self):
        temp_dir = tempfile.mkdtemp('warctest')

        full_path = os.path.join(temp_dir, 'example.warc.gz')

        url = 'http://localhost:{0}/get?foo=bar'.format(self.port)

        with record_http(full_path):
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

        os.remove(full_path)
        os.rmdir(temp_dir)


