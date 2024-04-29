1.7.4
~~~~~

- ``capture_http`` support for chunk-encoded requests `#116 <https://github.com/webrecorder/warcio/pull/116>`_

- indexer: option to enable ``verify_http`` `#116 <https://github.com/webrecorder/warcio/pull/116>`_

- Enable writing block digests for warcinfo records `#115 <https://github.com/webrecorder/warcio/pull/115>`_


1.7.3
~~~~~

- Fix documentation for capture_http filter_records `#110 <https://github.com/webrecorder/warcio/pull/110>`_

- Fix capture_http with http and https proxies `#113 <https://github.com/webrecorder/warcio/pull/113>`_


1.7.2
~~~~~

- Ensure 1.1 revisit profile used with WARC/1.1 revisits `#96 <https://github.com/webrecorder/warcio/pull/96>`_

- Include record offsets in ``warcio check`` output `#98 <https://github.com/webrecorder/warcio/pull/98>`_

- CI fix for python 2.7, use jinja<3.0.0 (`#105 <https://github.com/webrecorder/warcio/pull/105>`_)

- Fix in ``StatusAndHeaders`` when writing, then reading record `#106 <https://github.com/webrecorder/warcio/pull/106>`_

- Fix issues related to http header re-encoding, ensure correct content-length and %-encoding `#106 <https://github.com/webrecorder/warcio/pull/106>`_, `#107 <https://github.com/webrecorder/warcio/pull/107>`_


1.7.1
~~~~~

- Windows fixes: Fix reading from stdin, ensure all WARCs/ARCs are treated as binary `#86 <https://github.com/webrecorder/warcio/pull/86>`_

- Fix ``ensure_digest(block=True)`` breaking on an existing record, RecordBuilder supports ``header_filter`` `#85 <https://github.com/webrecorder/warcio/pull/85>`_


1.7.0
~~~~~

- Docs and Misc Cleanup: add docs for ``extract`` tool, correct doc for ``get_statuscode()``, move all CLI tools to separate modules for better reusability.

- Support indexing a WARC read from stdin `#79 <https://github.com/webrecorder/warcio/pull/79>`_

- Automatically %-encode urls that have a space in ``WARC-Target-URI`` `#80 <https://github.com/webrecorder/warcio/pull/80>`_

- Separate record creation into ``RecordBuilder`` class to allow building WARC records without a ``WARCWriter``, which now derives from ``RecordBuilder`` `#63 <https://github.com/webrecorder/warcio/pull/63>`_

- Support the ability to optionally check ARC/WARC record's block and payload digests `#54 <https://github.com/webrecorder/warcio/pull/54>`_, `#58 <https://github.com/webrecorder/warcio/pull/58>`_, `#68 <https://github.com/webrecorder/warcio/pull/68>`_, `#77 <https://github.com/webrecorder/warcio/pull/77>`_
    - Creation of ``ArchiveIterator`` and ``ArcWarcRecordLoader`` now accept an ``check_digests`` boolean keyword argument indicating if each records digest should be checked, defaults to ``False``
    - Core digest checking functionality is provided by ``DigestChecker`` and ``DigestVerifyingReader`` importable from `warcio.digestverifyingreader <digestverifyingreader.py>`_
    - New block and payload digest checking utility class, ``Checker``, has been added and is importable from `warcio.checker <checker.py>`_
    - The CLI has been updated to provide ``warcio check``, a command for performing block and payload digest checking
- Ensured that ARCHeadersParser's splitting on spaces does not split any spaces in uri's `#62 <https://github.com/webrecorder/warcio/pull/62>`_
- Move the ``compute_headers_buffer`` method and ``headers_buff`` property to the StatusAndHeaders and fix incorrect digests in some test WARCs `#67 <https://github.com/webrecorder/warcio/pull/67>`_
- Ensured that the ``BaseWARCWriter`` does not use a mutable default value for the ``warc_header_dict`` keyword argument `#70 <https://github.com/webrecorder/warcio/pull/70>`_


1.6.3
~~~~~

- Make ``warcio recompress`` more robust in fixing improperly compressed WARCs, --verbose mode for printing results `#52 <https://github.com/webrecorder/warcio/issues/52>`_
- BufferedReader supports streaming all members of multi-member gzip file with ``read_all_members=True`` option. 


1.6.2
~~~~~

- Ensure any non-ascii data in http headers is %-encoded, even if non-conformant to RFC 8187 `#51 <https://github.com/webrecorder/warcio/issues/51>`_


1.6.1
~~~~~

- Fixes for ``warcio.utils.open()`` not opening files in binary mode in Python 2.7 on Windows `#49 <https://github.com/webrecorder/warcio/issues/49>`_
- ``capture_http()`` various fixes and improvements, default writer, ``WARC-IP-Address`` header support `#50 <https://github.com/webrecorder/warcio/issues/50>`_


1.6.0
~~~~~

- Support WARC/1.1 standard WARC records, reading `#39 <https://github.com/webrecorder/warcio/issues/39>`_ and writing `#46 <https://github.com/webrecorder/warcio/issues/46>`_ with microsecond precision ``WARC-Date``
- Support simplified semantics for capturing http traffic to a WARC `#43 <https://github.com/webrecorder/warcio/issues/43>`_
- Support parsing incorrect wget 1.19 WARCs with angle brackets, eg: ``WARC-Target-URI: <uri>`` `#42 <https://github.com/webrecorder/warcio/issues/42>`_
- Correct encoding of non-ascii HTTP headers per RFC 8187 `#45 <https://github.com/webrecorder/warcio/issues/45>`_
- New Util Added: ``warcio.utils.open`` provides exclusive creation mode ``open(..., 'x')`` for Python 2.7

1.5.3
~~~~~

- ArchiveIterator calls new ``close_decompressor()`` function in BufferedReader instead of close() to only close decompressor, not underlying stream.  `#35 <https://github.com/webrecorder/warcio/issues/35>`_


1.5.2
~~~~~

- Write any errors during decompression to stderr `#31 <https://github.com/webrecorder/warcio/issues/31>`_
- ``to_native_str()`` returns original value unchanged if not a string/bytes type
- ``WarcWriter.create_visit_record()`` accepts additional WARC headers dictionary
- ``ArchiveIterator.close()`` added which calls ``decompressor.flush()`` to address possible issues in `#34 <https://github.com/webrecorder/warcio/issues/34>`_
- Switch ``Warc-Record-ID`` uuid creation to ``uuid4()`` from ``uuid1()``


1.5.1
~~~~~

- remove ``test/data`` from wheel build, as it breaks latest setuptools wheel installation
- add ``Content-Length`` when adding ``Content-Range`` via ``StatusAndHeaders.add_range`` `#29 <https://github.com/webrecorder/warcio/issues/29>`_


1.5.0
~~~~~
- new extract cli command `#26 <https://github.com/webrecorder/warcio/issues/26>`_ (by @nlevitt)
- fix for writing WARC record with no content-type `#27 <https://github.com/webrecorder/warcio/issues/27>`_ (by @thomaspreece)
- better verification of chunk header before attempting to de-chunk with ChunkedDataReader
- MANIFEST.in added (by @pmlandwehr)


1.4.0
~~~~~
- Indexing API improvements:
    - Indexer class moved to ``indexer.py`` and all aspects of indexing process can be extended.
    - Support for accessing http headers with ``http:``-prefixed fields `#22 <https://github.com/webrecorder/warcio/issues/22>`_
    - Special fields: ``filename`` field and ``http:status``
    - JSON ``offset`` and ``length`` fields returned as strings for consistency.
    - ``ArchiveIterator`` API: add ``get_record_offset()`` and ``get_record_length()`` to return current offset/length, iterator now tracks current record

- ``StatusAndHeaders`` accepts headers in more flexible formats (mapping, byte or string) and normalizes to string tuples `#19 <https://github.com/webrecorder/warcio/issues/19>`_


1.3.4
~~~~~
- Continuous read for more data to decompress (introduced in 1.3.2 for brotli decomp) should only happen if no unused data remaining. Otherwise, likely at gzip member end.


1.3.3
~~~~~
- Set default read ``block_size`` to 16384, ensure ``block_size`` is never None (caused an issue in py2.7)


1.3.2
~~~~~
- Fixes issues with BufferedReader returning empty response due to brotli decompressor requiring additional data, for more details see: `#21 <https://github.com/webrecorder/warcio/issues/21>`_


1.3.1
~~~~~
- Fixes `#15 <https://github.com/webrecorder/warcio/issues/15>`_, including:
- ``WARCWriter.create_warc_record()`` works correctly when specifying a payload with no length param.
- Writing DNS records now works (tests included).
- HTTP headers only expected for writing ``request``, ``response`` records if the URI has a ``http:`` or ``https:`` scheme (consistent with reading).


1.3
~~~
- Support for reading "streaming" WARC records, with no ``Content-Length`` set. ``Content-Length`` and digests computed as expected when the record is written.

- Additional tests for streaming WARC records, loading HTTP headers+payload from buffer, POST request record, arc2warc conversion.

- ``recompress`` command now parses records fully and generates correct block and payload digests.

- ``WARCWriter.writer.create_record_from_stream()`` removed, redundant with ``ArcWarcRecordLoader()``



1.2
~~~
- Support for special field ``offset`` to include WARC record offset when indexing (by @nlevitt, `#4 <https://github.com/webrecorder/warcio/issues/4>`_)
- ``ArchiveIterator`` supports full iterator semantics
- WARC headers encoded/decoded as UTF-8, with fallback to ISO-8859-1 (see `#6 <https://github.com/webrecorder/warcio/issues/6>`_, `#7 <https://github.com/webrecorder/warcio/issues/7>`_)
- ``ArchiveIterator``, ``StatusAndHeaders`` and ``WARCWriter`` now available from package root (by @nlevitt, `#10 <https://github.com/webrecorder/warcio/issues/10>`_)
- ``StatusAndHeaders`` supports dict-like API (by @nlevitt, `#11 <https://github.com/webrecorder/warcio/issues/11>`_)
- When reading, http headers never added by default, unless ``ensure_http_headers=True`` is set (see `#12 <https://github.com/webrecorder/warcio/issues/12>`_, `#13 <https://github.com/webrecorder/warcio/issues/13>`_)
- All tests run on Windows, CI using Appveyor
- Additional tests for writing/reading resource, metadata records
- ``warcio -V`` now outputs current version.

1.1
~~~

- Header filtering: support filtering via custom header function, instead of an exclusion list
- Add tests for invalid data passed to ``recompress``, remove unused code


1.0
~~~

Initial Release!


