Contributing to warcio
======================

We welcome contributions to warcio! Whether you're adding new features, improving documentation, or fixing bugs, your help is greatly appreciated.


Local installation
------------------

Clone the repository, setup a virtual environment, and run the following command to install test dependencies:

::

    pip install warcio[testing]


Tests
-----

To test code changes, please run our test suite before submitting pull requests:

::

    pytest test

By default, all remote requests to S3 are mocked. To change this behaviour and actually do live S3 reads and writes (if AWS credentials are set), the following environment variable can be set:

::

    WARCIO_ENABLE_S3_TESTS=1

The S3 bucket used for testing can be set via environment variable (default: `commoncrawl-ci-temp`):

::

    WARCIO_TEST_S3_BUCKET=my-s3-bucket

