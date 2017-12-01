from warcio.archiveiterator import ArchiveIterator, WARCIterator, ARCIterator
from warcio.recordloader import ArchiveLoadFailed

from warcio.warcwriter import BufferWARCWriter

import pytest
from io import BytesIO

from . import get_test_file


#==============================================================================
class TestArchiveIterator(object):
    def _load_archive(self, filename, offset=0, cls=ArchiveIterator,
                     errs_expected=0, check_digests=True, **kwargs):

        with open(get_test_file(filename), 'rb') as fh:
            fh.seek(offset)
            iter_ = cls(fh, check_digests=check_digests, **kwargs)
            rec_types = [record.rec_type for record in iter_]

        assert iter_.err_count == errs_expected

        return rec_types

    def _load_archive_memory(self, stream, offset=0, cls=ArchiveIterator,
                     errs_expected=0, check_digests=True, **kwargs):

        stream.seek(offset)
        iter_ = cls(stream, check_digests=check_digests, **kwargs)
        rec_types = [record.rec_type for record in iter_]

        assert iter_.err_count == errs_expected

        return rec_types

    def _read_first_response(self, filename):
        record = self._find_first_by_type(filename, 'response')
        if record:
            return record.content_stream().read()

    def _find_first_by_type(self, filename, match_type, check_digests=True, **params):
        with open(get_test_file(filename), 'rb') as fh:
            for record in ArchiveIterator(fh, check_digests=check_digests, **params):
                if record.rec_type == match_type:
                    return record

    def test_example_warc_gz(self):
        expected = ['warcinfo', 'warcinfo', 'response', 'request', 'revisit', 'request']
        assert self._load_archive('example.warc.gz') == expected

    def test_example_warc(self):
        expected = ['warcinfo', 'warcinfo', 'response', 'request', 'revisit', 'request']
        assert self._load_archive('example.warc') == expected

    def test_example_warc_2(self):
        expected = ['warcinfo', 'response', 'request']
        assert self._load_archive('example-iana.org-chunked.warc') == expected

    def test_iterator(self):
        """ Test iterator semantics on 3 record WARC
        """
        with open(get_test_file('example-iana.org-chunked.warc'), 'rb') as fh:
            a = ArchiveIterator(fh, check_digests=True)
            for record in a:
                assert record.rec_type == 'warcinfo'
                break

            record = next(a)
            assert record.rec_type == 'response'

            for record in a:
                assert record.rec_type == 'request'
                break

            with pytest.raises(StopIteration):
                record = next(a)

        assert a.record == None
        assert a.read_to_end() == None

    def test_example_warc_trunc(self):
        """ WARC file with content-length truncated on a response record
        Error output printed, but still read
        """
        expected = ['warcinfo', 'warcinfo', 'response', 'request']
        assert self._load_archive('example-trunc.warc', errs_expected=1,
                                  check_digests=False) == expected

        with pytest.raises(ValueError):
            assert self._load_archive('example-trunc.warc', errs_expected=1,
                                      check_digests=True) == expected

    def test_example_arc_gz(self):
        expected = ['arc_header', 'response']
        assert self._load_archive('example.arc.gz') == expected

    def test_example_arc(self):
        expected = ['arc_header', 'response']
        assert self._load_archive('example.arc') == expected

    def test_example_arc2warc(self):
        expected = ['warcinfo', 'response']
        assert self._load_archive('example.arc.gz', arc2warc=True) == expected

    def test_example_warc_resource(self):
        expected = ['warcinfo', 'warcinfo', 'resource']
        assert self._load_archive('example-resource.warc.gz') == expected

    def test_resource_no_http_headers(self):
        record = self._find_first_by_type('example-resource.warc.gz', 'resource')
        assert record.http_headers == None
        assert len(record.content_stream().read()) == int(record.rec_headers.get('Content-Length'))

    def test_resource_with_http_headers(self):
        record = self._find_first_by_type('example-resource.warc.gz', 'resource',
                                          ensure_http_headers=True)

        assert record.http_headers != None

        assert (record.http_headers.get_header('Content-Length') ==
                record.rec_headers.get_header('Content-Length'))

        expected = 'HTTP/1.0 200 OK\r\n\
Content-Type: text/html; charset=utf-8\r\n\
Content-Length: 1303\r\n'

        assert str(record.http_headers) == expected
        assert len(record.content_stream().read()) == int(record.rec_headers.get('Content-Length'))

    def test_read_content(self):
        assert 'Example Domain' in self._read_first_response('example.warc.gz').decode('utf-8')
        assert 'Example Domain' in self._read_first_response('example.warc').decode('utf-8')
        assert 'Example Domain' in self._read_first_response('example.arc.gz').decode('utf-8')
        assert 'Example Domain' in self._read_first_response('example.arc').decode('utf-8')

    def test_read_content_chunked(self):
        buff = self._read_first_response('example-iana.org-chunked.warc').decode('utf-8')
        assert buff.startswith('<!doctype html>')
        assert 'Internet Assigned Numbers Authority' in buff

    def test_bad_warc(self):
        with pytest.raises(ArchiveLoadFailed):
            self._load_archive('example-bad.warc.gz.bad')

    def test_bad_offset_warc(self):
        with pytest.raises(ArchiveLoadFailed):
            self._load_archive('example.warc.gz', offset=10)

    def test_bad_arc_invalid_lengths(self):
        expected = ['arc_header', 'response', 'response', 'response']
        assert self._load_archive('bad.arc') == expected

    def test_err_non_chunked_gzip(self):
        with pytest.raises(ArchiveLoadFailed):
            self._load_archive('example-bad-non-chunked.warc.gz')

    def test_err_warc_iterator_on_arc(self):
        expected = ['arc_header', 'response']
        with pytest.raises(ArchiveLoadFailed):
            self._load_archive('example.arc.gz', cls=WARCIterator)

    def test_err_arc_iterator_on_warc(self):
        expected = ['arc_header', 'response']
        with pytest.raises(ArchiveLoadFailed):
            self._load_archive('example.warc.gz', cls=ARCIterator)

    def test_digests(self):
        expected = ['warcinfo', 'warcinfo', 'response', 'request', 'revisit', 'request']

        with open(get_test_file('example.warc'), 'rb') as fh:
            contents = fh.read()

        contents_sha = contents.replace(b'sha1:', b'xxx:', 1)
        with pytest.raises(ValueError):
            assert self._load_archive_memory(BytesIO(contents_sha)) == expected

        contents_colon = contents.replace(b'sha1:', b'', 1)
        with pytest.raises(ValueError):
            assert self._load_archive_memory(BytesIO(contents_colon)) == expected

        contents_block = contents
        thing = b'WARC-Block-Digest: sha1:'
        index = contents_block.find(thing)
        index += len(thing)
        b = contents_block[index:index+3]
        contents_block = contents_block.replace(thing+b, thing+b'111')
        with pytest.raises(ValueError):
            assert self._load_archive_memory(BytesIO(contents_block)) == expected

        contents_payload = contents
        thing = b'WARC-Payload-Digest: sha1:'
        index = contents_payload.find(thing)
        index += len(thing)
        b = contents_payload[index:index+3]
        contents_payload = contents_payload.replace(thing+b, thing+b'111')
        with pytest.raises(ValueError):
            assert self._load_archive_memory(BytesIO(contents_payload)) == expected

        expected = ['request', 'request', 'request']
        with pytest.raises(ValueError):
            # record 1: invalid payload digest
            assert self._load_archive('example_digest.warc') == expected

        # record 2: b64 digest; record 3: b64 filename safe digest
        assert self._load_archive('example_digest.warc', offset=922) == expected
