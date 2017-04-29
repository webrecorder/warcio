1.2
~~~
- Support for special field ``offset`` to include WARC record offset when indexing (by @nlevitt, #4)
- ``ArchiveIterator`` supports full iterator semantics
- WARC headers encoded as UTF-8, decoded as UTF-8 first, then ISO-8859-1 (see #6, #7)
- ``ArchiveIterator``, ``StatusAndHeaders`` and ``WARCWriter`` now available from package (by @nlevitt, #10)
- ``StatusAndHeaders`` supports dict-link API (by @nlevitt, #11)
- When reading, http headers never added by default, unless ``ensure_http_headers=True`` is set (see #12, #13)
- CI on Windows using Appveyor
- Additional tests for resource, metadata records
- ``warcio -V`` outputs current version.

1.1
~~~

- Header filtering: support filtering via custom header function, instead of an exclusion list
- Add tests for invalid data passed to ``recompress``, remove unused code


1.0
~~~

Initial Release!


