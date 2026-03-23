import os
import uuid
import pytest

# IMPORTANT:
# Import capture_http before any other indirect imports of urllib3/requests
# This ensures the monkey patch is applied before botocore imports urllib3
from warcio.capture_http import capture_http  # noqa: F401

try:
    import botocore.session  # noqa: F401

    HAS_AWS_DEPS = True
except ImportError:
    HAS_AWS_DEPS = False


TEST_S3_BUCKET = os.environ.get("WARCIO_TEST_S3_BUCKET", "commoncrawl-ci-temp")
ENABLE_S3_TESTS = bool(os.environ.get("WARCIO_ENABLE_S3_TESTS", False))

# Cache for AWS access check to avoid repeated network calls
_aws_s3_access_cache = None


def check_aws_s3_access():
    """Check if AWS S3 access is available (cached result)."""
    global _aws_s3_access_cache

    if _aws_s3_access_cache is not None:
        return _aws_s3_access_cache

    if not HAS_AWS_DEPS:
        return False

    from botocore.config import Config
    from botocore.exceptions import (
        NoCredentialsError,
        ClientError,
        EndpointConnectionError,
    )

    try:
        config = Config(retries={"max_attempts": 1, "mode": "standard"})
        session = botocore.session.Session()
        s3_client = session.create_client("s3", config=config)

        # Try list objects on test bucket
        s3_client.list_objects_v2(Bucket=TEST_S3_BUCKET, MaxKeys=1)
        _aws_s3_access_cache = True
    except (NoCredentialsError, ClientError, ConnectionError,
            EndpointConnectionError):
        _aws_s3_access_cache = False

    return _aws_s3_access_cache


def requires_aws_s3(func):
    """Pytest decorator that checks if AWS S3 test can be run."""
    return pytest.mark.skipif(
        not ENABLE_S3_TESTS,
        reason="S3 tests are NOT enabled via environment variable."
    )(
        pytest.mark.skipif(
            not HAS_AWS_DEPS,
            reason="S3 unavailable (missing dependency)",
        )(
            pytest.mark.skipif(
                not check_aws_s3_access(),
                reason="S3 unavailable (no access)",
            )(func)
        )
    )


@pytest.fixture
def s3_tmpdir():
    """S3 equivalent of tmpdir: provides a temporary S3 path and cleans up."""
    from botocore.exceptions import (
        NoCredentialsError,
        ClientError,
        EndpointConnectionError,
    )

    bucket_name = TEST_S3_BUCKET

    # Generate unique prefix using UUID to avoid collisions
    temp_prefix = f'warcio/ci/tmpdirs/{uuid.uuid4().hex}'

    # Yield the S3 path
    yield f's3://{bucket_name}/{temp_prefix}'

    try:
        # Cleanup: delete all objects with this prefix
        session = botocore.session.Session()
        s3_client = session.create_client('s3')

        # List all objects with the temp prefix
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=temp_prefix
        )

        if 'Contents' in response:
            # Delete all objects
            objects_to_delete = [
                {'Key': obj['Key']} for obj in response['Contents']
            ]
            s3_client.delete_objects(
                Bucket=bucket_name, 
                Delete={'Objects': objects_to_delete}
            )
    except (NoCredentialsError, ClientError, ConnectionError,
            EndpointConnectionError):
        # Ignore cleanup errors - test objects will eventually expire
        pass
