import pytest
from warcio.utils import fsspec_open
from warcio.cli import main

from . import get_test_file, check_helper
from .conftest import requires_aws_s3

try:
    import fsspec  # noqa: F401
    import s3fs  # noqa: F401
    HAS_FSSPEC = True
except ModuleNotFoundError:
    HAS_FSSPEC = False

try:
    import botocore.session  # noqa: F401
    import moto  # noqa: F401
    HAS_MOTO = True
except ModuleNotFoundError:
    HAS_MOTO = False


if not HAS_FSSPEC:
    pytest.skip("fsspec[s3] is not installed", allow_module_level=True)


@pytest.fixture
def mock_s3_tmpdir(monkeypatch):
    """Fixture that provides a mocked S3 backend using moto server."""
    if not HAS_MOTO:
        pytest.skip("moto is not installed")

    from moto.server import ThreadedMotoServer

    # Clear any cached S3FileSystem instances
    s3fs.S3FileSystem.clear_instance_cache()

    # Start moto server on a local port
    server = ThreadedMotoServer(port="5555", verbose=False)
    server.start()

    # Configure environment to use the mock server
    endpoint_url = "http://127.0.0.1:5555"
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    # Create mock bucket using the endpoint
    session = botocore.session.Session()
    s3_client = session.create_client(
        's3',
        region_name='us-east-1',
        endpoint_url=endpoint_url
    )
    bucket_name = 'test-bucket'
    s3_client.create_bucket(Bucket=bucket_name)

    # Patch s3fs to use the mock endpoint
    original_init = s3fs.S3FileSystem.__init__

    def patched_init(self, *args, **kwargs):
        # Force endpoint URL for all S3FileSystem instances
        kwargs['client_kwargs'] = kwargs.get('client_kwargs', {})
        kwargs['client_kwargs']['endpoint_url'] = endpoint_url
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(s3fs.S3FileSystem, '__init__', patched_init)

    try:
        yield f's3://{bucket_name}/test'
    finally:
        server.stop()
        # Clear cache again after test
        s3fs.S3FileSystem.clear_instance_cache()


def _test_recompress_warc_verbose(capsys, s3_path):
    """Shared implementation for recompress test."""
    compress_output_path = s3_path + "/foo.warc.gz"

    test_file = get_test_file('example.warc.gz')

    # recompress!
    main(args=['recompress', '-v', test_file, compress_output_path])

    out = capsys.readouterr().out
    assert '{"offset": "0", "warc-type": "warcinfo"}' in out
    assert '"warc-target-uri": "http://example.com/"' in out

    assert 'No Errors Found!' in out
    assert '6 records read' in out


def _test_write_and_read_s3(s3_path):
    """Shared implementation for write/read test."""
    file_path = s3_path + "/foo.text"

    with fsspec_open(file_path, "wt") as f:
        f.write("foo")

    with fsspec_open(file_path, "rt") as f:
        content = f.read()

    assert content == "foo", "invalid file content"


def _test_copy_to_s3_and_check_extract(s3_path, capsys):
    """Shared implementation for copy/check/extract test."""
    input_file = get_test_file('example.warc.gz')
    output_file = s3_path + '/example.warc.gz'

    # copy text file to S3
    with open(input_file, "rb") as input_f:
        with fsspec_open(output_file, "wb") as output_f:
            output_f.write(input_f.read())

    # check uploaded file
    check_output = check_helper(['check', '-v', output_file], capsys, 0)
    assert 'Invalid' not in check_output

    # extract from uploaded file
    extract_output = check_helper(['extract', output_file, '0'], capsys, None)
    assert 'WARC-Filename: temp-20170306040353.warc.gz' in extract_output


@requires_aws_s3
def test_recompress_warc_verbose_live(capsys, s3_tmpdir):
    _test_recompress_warc_verbose(capsys, s3_tmpdir)


@requires_aws_s3
def test_write_and_read_s3_live(s3_tmpdir):
    _test_write_and_read_s3(s3_tmpdir)


@requires_aws_s3
def test_copy_to_s3_and_check_extract_live(s3_tmpdir, capsys):
    _test_copy_to_s3_and_check_extract(s3_tmpdir, capsys)


def test_recompress_warc_verbose_mocked(capsys, mock_s3_tmpdir):
    _test_recompress_warc_verbose(capsys, mock_s3_tmpdir)


def test_write_and_read_s3_mocked(mock_s3_tmpdir):
    _test_write_and_read_s3(mock_s3_tmpdir)


def test_copy_to_s3_and_check_extract_mocked(mock_s3_tmpdir, capsys):
    _test_copy_to_s3_and_check_extract(mock_s3_tmpdir, capsys)
