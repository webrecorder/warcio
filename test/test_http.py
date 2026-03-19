import pytest
import threading
import time
from contextlib import contextmanager
from wsgiref.simple_server import make_server

from warcio.utils import fsspec_open

from . import get_test_file
from .test_cli import check_helper


try:
    import fsspec  # noqa: F401

    HAS_FSSPEC = True
except ModuleNotFoundError:
    HAS_FSSPEC = False


if not HAS_FSSPEC:
    pytest.skip("fsspec is not installed", allow_module_level=True)


@contextmanager
def mock_http_server(file_path, serve_path=None):
    """Context manager for a mock HTTP server that serves a file.

    Args:
        file_path: Path to the file to serve
        serve_path: URL path to serve the file at (defaults to filename)

    Yields:
       str: complete URL to the file
    """
    # Read the file content
    with open(file_path, "rb") as f:
        file_content = f.read()

    if serve_path is None:
        import os

        serve_path = "/" + os.path.basename(file_path)

    # Simple WSGI app that serves the file
    def simple_app(environ, start_response):
        if environ["PATH_INFO"] == serve_path:
            status = "200 OK"
            content_type = (
                "application/gzip"
                if file_path.endswith(".gz")
                else "application/octet-stream"
            )
            headers = [
                ("Content-Type", content_type),
                ("Content-Length", str(len(file_content))),
            ]
            start_response(status, headers)
            return [file_content]
        else:
            status = "404 Not Found"
            start_response(status, [("Content-Type", "text/plain")])
            return [b"Not Found"]

    # Start the mock HTTP server
    server = make_server("localhost", 0, simple_app)
    addr, port = server.socket.getsockname()

    def run_server():
        try:
            server.serve_forever()
        except Exception as e:
            print(e)

    thread = threading.Thread(target=run_server)
    thread.daemon = True
    thread.start()
    time.sleep(0.1)  # Give server time to start

    base_url = f"http://localhost:{port}"
    full_url = base_url + serve_path

    try:
        yield full_url
    finally:
        server.shutdown()
        server.server_close()


def test_check_warc_from_http(capsys):
    """Run the check command against a WARC from a HTTP server."""
    input_file_path = get_test_file("example.warc.gz")

    with mock_http_server(input_file_path) as http_url:
        # check file from http server
        check_output = check_helper(["check", "-v", http_url], capsys, 0)
        assert "Invalid" not in check_output


def test_read_warc_from_http():
    """Read a WARC file from a HTTP server."""
    input_file_path = get_test_file("example.warc")

    with mock_http_server(input_file_path) as http_url:
        # read from http
        with fsspec_open(http_url, "rb") as f:
            content = f.read().decode(errors="ignore")

            assert (
                "WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1"
                in content
            )
