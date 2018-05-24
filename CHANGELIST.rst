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


