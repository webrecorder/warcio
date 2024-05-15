from warcio.capture_http import capture_http

import threading
from wsgiref.simple_server import make_server, WSGIServer
import time

import requests
from hookdns import hosts
from warcio.archiveiterator import ArchiveIterator


from pytest import raises


# ==================================================================
class TestCaptureHttpProxy():
    def setup(cls):
        def app(env, start_response):
            result = ('Proxied: ' + env['PATH_INFO']).encode('utf-8')
            headers = [('Content-Length', str(len(result)))]
            start_response('200 OK', headers=headers)
            return iter([result])

        from wsgiprox.wsgiprox import WSGIProxMiddleware
        wsgiprox = WSGIProxMiddleware(app, '/')

        class NoLogServer(WSGIServer):
            def handle_error(self, request, client_address):
                pass

        server = make_server('localhost', 0, wsgiprox, server_class=NoLogServer)
        addr, cls.port = server.socket.getsockname()

        print(f"cls.port: {cls.port}", flush=True)

        cls.proxies = {
            'https': 'http://proxy.com:' + str(cls.port),
            'http': 'http://proxy.com:' + str(cls.port)
        }

        print(f"cls.proxies: {cls.proxies}", flush=True)

        def run():
            try:
                server.serve_forever()
            except  Exception as e:
                print(e)

        thread = threading.Thread(target=run)
        thread.daemon = True
        thread.start()
        time.sleep(0.1)

    def test_capture_http_proxy(self):
        with hosts({"proxy.com": "127.0.0.1"}):
            with capture_http() as warc_writer:
                res = requests.get("http://example.com/test", proxies=self.proxies, verify=False)

            ai = ArchiveIterator(warc_writer.get_stream())
            response = next(ai)
            assert response.rec_type == 'response'
            assert response.rec_headers['WARC-Target-URI'] == "http://example.com/test"
            assert response.content_stream().read().decode('utf-8') == 'Proxied: /http://example.com/test'
            assert response.rec_headers['WARC-Proxy-Host'] == 'http://proxy.com:{0}'.format(self.port)

            request = next(ai)
            assert request.rec_type == 'request'
            assert request.rec_headers['WARC-Target-URI'] == "http://example.com/test"
            assert request.rec_headers['WARC-Proxy-Host'] == 'http://proxy.com:{0}'.format(self.port)

            with raises(StopIteration):
                assert next(ai)

    def test_capture_https_proxy(self):
        with hosts({"proxy.com": "127.0.0.1"}):
            with capture_http() as warc_writer:
                res = requests.get("https://example.com/test", proxies=self.proxies, verify=False)
                res = requests.get("https://example.com/foo", proxies=self.proxies, verify=False)

            # not recording this request
            res = requests.get("https://example.com/skip", proxies=self.proxies, verify=False)

            with capture_http(warc_writer):
                res = requests.get("https://example.com/bar", proxies=self.proxies, verify=False)

            ai = ArchiveIterator(warc_writer.get_stream())
            response = next(ai)
            assert response.rec_type == 'response'
            assert response.rec_headers['WARC-Target-URI'] == "https://example.com/test"
            assert response.rec_headers['WARC-Proxy-Host'] == 'https://proxy.com:{0}'.format(self.port)
            assert response.content_stream().read().decode('utf-8') == 'Proxied: /https://example.com/test'

            request = next(ai)
            assert request.rec_type == 'request'
            assert request.rec_headers['WARC-Target-URI'] == "https://example.com/test"
            assert request.rec_headers['WARC-Proxy-Host'] == 'https://proxy.com:{0}'.format(self.port)

            response = next(ai)
            assert response.rec_type == 'response'
            assert response.rec_headers['WARC-Target-URI'] == "https://example.com/foo"
            assert response.rec_headers['WARC-Proxy-Host'] == 'https://proxy.com:{0}'.format(self.port)
            assert response.content_stream().read().decode('utf-8') == 'Proxied: /https://example.com/foo'

            request = next(ai)
            assert request.rec_type == 'request'
            assert request.rec_headers['WARC-Target-URI'] == "https://example.com/foo"
            assert request.rec_headers['WARC-Proxy-Host'] == 'https://proxy.com:{0}'.format(self.port)

            response = next(ai)
            assert response.rec_type == 'response'
            assert response.rec_headers['WARC-Target-URI'] == "https://example.com/bar"
            assert response.rec_headers['WARC-Proxy-Host'] == 'https://proxy.com:{0}'.format(self.port)
            assert response.content_stream().read().decode('utf-8') == 'Proxied: /https://example.com/bar'

            request = next(ai)
            assert request.rec_type == 'request'

            with raises(StopIteration):
                assert next(ai)

    def test_capture_https_proxy_same_session(self):
        sesh = requests.session()

        with hosts({"proxy.com": "127.0.0.1"}):
            with capture_http() as warc_writer:
                res = sesh.get("https://example.com/test", proxies=self.proxies, verify=False)
                res = sesh.get("https://example.com/foo", proxies=self.proxies, verify=False)

            # *will* be captured, as part of same session... (fix this?)
            res = sesh.get("https://example.com/skip", proxies=self.proxies, verify=False)

            with capture_http(warc_writer):
                res = sesh.get("https://example.com/bar", proxies=self.proxies, verify=False)

            ai = ArchiveIterator(warc_writer.get_stream())
            response = next(ai)
            assert response.rec_type == 'response'
            assert response.rec_headers['WARC-Target-URI'] == "https://example.com/test"
            assert response.rec_headers['WARC-Proxy-Host'] == 'https://proxy.com:{0}'.format(self.port)
            assert response.content_stream().read().decode('utf-8') == 'Proxied: /https://example.com/test'

            request = next(ai)
            assert request.rec_type == 'request'
            assert request.rec_headers['WARC-Target-URI'] == "https://example.com/test"
            assert request.rec_headers['WARC-Proxy-Host'] == 'https://proxy.com:{0}'.format(self.port)

            response = next(ai)
            assert response.rec_type == 'response'
            assert response.rec_headers['WARC-Target-URI'] == "https://example.com/foo"
            assert response.rec_headers['WARC-Proxy-Host'] == 'https://proxy.com:{0}'.format(self.port)
            assert response.content_stream().read().decode('utf-8') == 'Proxied: /https://example.com/foo'

            request = next(ai)
            assert request.rec_type == 'request'
            assert request.rec_headers['WARC-Target-URI'] == "https://example.com/foo"
            assert request.rec_headers['WARC-Proxy-Host'] == 'https://proxy.com:{0}'.format(self.port)

            response = next(ai)
            assert response.rec_type == 'response'
            assert response.rec_headers['WARC-Target-URI'] == "https://example.com/skip"
            assert response.rec_headers['WARC-Proxy-Host'] == 'https://proxy.com:{0}'.format(self.port)
            assert response.content_stream().read().decode('utf-8') == 'Proxied: /https://example.com/skip'

            request = next(ai)
            assert request.rec_type == 'request'

            response = next(ai)
            assert response.rec_type == 'response'
            assert response.rec_headers['WARC-Target-URI'] == "https://example.com/bar"
            assert response.rec_headers['WARC-Proxy-Host'] == 'https://proxy.com:{0}'.format(self.port)
            assert response.content_stream().read().decode('utf-8') == 'Proxied: /https://example.com/bar'

            request = next(ai)
            assert request.rec_type == 'request'

            with raises(StopIteration):
                assert next(ai)

