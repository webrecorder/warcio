WARCIO: WARC (and ARC) Streaming Library
========================================
.. image:: https://travis-ci.org/webrecorder/warcio.svg?branch=master
      :target: https://travis-ci.org/webrecorder/warcio
.. image:: https://coveralls.io/repos/github/webrecorder/warcio/badge.svg?branch=master
      :target: https://coveralls.io/github/webrecorder/warcio?branch=master


Background
~~~~~~~~~~

This library provides a fast, standalone way to read and write `WARC
Format <https://en.wikipedia.org/wiki/Web_ARChive>`__ commonly used in
web archives. Supports Python 2.7+ and Python 3.3+ (using
`six <https://pythonhosted.org/six/>`__, the only external dependency)

Install with: ``pip install warcio``

This library is a spin-off of the WARC reading and writing component of
the `pywb <https://github.com/ikreymer/pywb>`__ high-fidelity replay
library, a key component of
`Webrecorder <https://github.com/webrecorder/webrecorder>`__

The library is designed for fast, low-level access to web archival
content, oriented around a stream of WARC records rather than files.

Reading WARC Records
--------------------

A key feature of the library is to be able to iterate over a stream of
WARC records using the ``ArchiveIterator``

It includes the following features: - Reading a WARC/ARC stream - On the
fly ARC to WARC record conversion - Decompressing and de-chunking HTTP
payload content stored in WARC/ARC files.

For example, the following prints the the url for each WARC ``response``
record:

.. code:: python

    from warcio.archiveiterator import ArchiveIterator

    with open('path/to/file', 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'response':
                print(record.rec_headers.get_header('WARC-Target-URI'))

The stream object could be a file on disk or a remote network stream.
The ``ArchiveIterator`` reads the WARC content in a single pass. The
``record`` is represented by an ``ArcWarcRecord`` object which contains
the format (ARC or WARC), record type, the record headers, http headers
(if any), and raw stream for reading the payload.

.. code:: python

    class ArcWarcRecord(object):
        def __init__(self, *args):
            (self.format, self.rec_type, self.rec_headers, self.raw_stream,
             self.http_headers, self.content_type, self.length) = args

Reading WARC Content
~~~~~~~~~~~~~~~~~~~~

The ``raw_stream`` can be used to read the rest of the payload directly.
A special ``ArcWarcRecord.content_stream()`` function provides a stream that
automatically decompresses and de-chunks the HTTP payload, if it is
compressed and/or transfer-encoding chunked.

ARC Files
~~~~~~~~~

The library provides support for reading (but not writing ARC) files.
The ARC format is legacy but is important to support in a consistent
matter. The ``ArchiveIterator`` can equally iterate over ARC and WARC
files to emit ``ArcWarcRecord`` objects. The special ``arc2warc`` option
converts ARC records to WARCs on the fly, allowing for them to be
accessed using the same API.

(Special ``WARCIterator`` and ``ARCIterator`` subclasses of ``ArchiveIterator``
are also available to read only WARC or only ARC files).

WARC and ARC Streaming
~~~~~~~~~~~~~~~~~~~~~~
For example, here is a snippet for reading an ARC and a WARC using the
same API.

The example streams a WARC and ARC file over HTTP using
`requests <http://docs.python-requests.org/en/master/>`__, printing the
``warcinfo`` record (or ARC header) and any response records (or all ARC
records) that contain HTML:

.. code:: python

    import requests
    from warcio.archiveiterator import ArchiveIterator

    def print_records(url):
        resp = requests.get(url, stream=True)

        for record in ArchiveIterator(resp.raw, arc2warc=True):
            if record.rec_type == 'warcinfo':
                print(record.raw_stream.read())

            elif record.rec_type == 'response':
                if record.http_headers.get_header('Content-Type') == 'text/html':
                    print(record.rec_headers.get_header('WARC-Target-URI'))
                    print(record.content_stream().read())
                    print('')

    # WARC
    print_records('https://archive.org/download/ExampleArcAndWarcFiles/IAH-20080430204825-00000-blackbook.warc.gz')


    # ARC with arc2warc
    print_records('https://archive.org/download/ExampleArcAndWarcFiles/IAH-20080430204825-00000-blackbook.arc.gz')


Writing WARC Records
--------------------

The library provides a simple and extensible interface for writing WARC
records conformant to WARC 1.0 ISO standard
`(see draft) <http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf>`__

The library comes with a basic ``WARCWriter`` class for writing to a
single WARC file and ``BufferWARCWriter`` for writing to an in-memory
buffer. The ``BaseWARCWriter`` can be extended to support more complex
operations.

(There is no support for writing legacy ARC files)

The following example loads ``http://example.com/``, creates a WARC
response record, and writes it, gzip compressed, to ``example.warc.gz``
The block and payload digests are computed automatically.

.. code:: python

    from warcio.warcwriter import WARCWriter
    from warcio.statusandheaders import StatusAndHeaders

    import requests

    with open('example.warc.gz', 'wb') as output:
        writer = WARCWriter(output, gzip=True)

        resp = requests.get('http://example.com/',
                            headers={'Accept-Encoding': 'identity'},
                            stream=True)

        # get raw headers from urllib3
        headers_list = resp.raw.headers.items()

        http_headers = StatusAndHeaders('200 OK', headers_list, protocol='HTTP/1.0')

        record = writer.create_warc_record('http://example.com/', 'response',
                                            payload=resp.raw,
                                            http_headers=http_headers)

        writer.write_record(record)

The library also includes additional semantics for: - Creating
``warcinfo`` and ``revisit`` records - Writing ``response`` and
``request`` records together - Writing custom WARC records - Reading a
full WARC record from a stream

Please refer to `warcwriter.py <warcio/warcwriter.py>`__ and
`test/test\_writer.py <test/test_writer.py>`__ for additional examples.

WARCIO CLI: Indexing and Recompression
--------------------------------------

The library currently ships with two simple command line tools.

Index
~~~~~

The ``warcio index`` cmd will print a simple index of the records in the
warc file as newline delimited JSON lines (NDJSON).

WARC header fields to include in the index can be specified via the
``-f`` flag, and are included in the JSON block (in order, for
convenience).

::

    warcio index ./test/data/example-iana.org-chunked.warc -f warc-type,warc-target-uri,content-length
    {"warc-type": "warcinfo", "content-length": "137"}
    {"warc-type": "response", "warc-target-uri": "http://www.iana.org/", "content-length": "7566"}
    {"warc-type": "request", "warc-target-uri": "http://www.iana.org/", "content-length": "76"}


HTTP header fields can be included by prefixing them with the prefix
``http:``. The special field ``offset`` refers to the record offset within
the warc file.

::

    warcio index ./test/data/example-iana.org-chunked.warc -f offset,content-type,http:content-type,warc-target-uri
    {"offset": "0", "content-type": "application/warc-fields"}
    {"offset": "405", "content-type": "application/http;msgtype=response", "http:content-type": "text/html; charset=UTF-8", "warc-target-uri": "http://www.iana.org/"}
    {"offset": "8379", "content-type": "application/http;msgtype=request", "warc-target-uri": "http://www.iana.org/"}

(Note: this library does not produce CDX or CDXJ format indexes often
associated with web archives. To create these indexes, please see the
`cdxj-indexer <https://github.com/webrecorder/cdxj-indexer>`__ tool which extends warcio indexing to provide this functionality)

Recompress
~~~~~~~~~~

The ``recompress`` command allows for re-compressing or normalizing WARC
(or ARC) files to a record-compressed, gzipped WARC file.

Each WARC record is compressed individually and concatenated. This is
the 'canonical' WARC storage format used by
`Webrecorder <https://github.com/webrecorder/webrecorder>`__ and other
web archiving institutions, and usually stored with a ``.warc.gz``
extension.

It can be used to: - Compress an uncompressed WARC - Convert any ARC
file to a compressed WARC - Fix an improperly compressed WARC file (eg.
a WARC compressed entirely instead of by record)

::

    warcio recompress ./input.arc.gz ./output.warc.gz

License
~~~~~~~

``warcio`` is licensed under the Apache 2.0 License and is part of the
Webrecorder project.

See `NOTICE <NOTICE>`__ and `LICENSE <LICENSE>`__ for details.
