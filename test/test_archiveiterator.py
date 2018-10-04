from warcio.archiveiterator import ArchiveIterator, WARCIterator, ARCIterator
from warcio.recordloader import ArchiveLoadFailed

from warcio.warcwriter import BufferWARCWriter

import pytest

from . import get_test_file
from contextlib import closing, contextmanager


#==============================================================================
class TestArchiveIterator(object):
    def _load_archive(self, filename, offset=0, cls=ArchiveIterator,
                     errs_expected=0, **kwargs):

        with open(get_test_file(filename), 'rb') as fh:
            fh.seek(offset)
            iter_ = cls(fh, **kwargs)
            rec_types = [record.rec_type for record in iter_]

        assert iter_.err_count == errs_expected

        return rec_types

    def _read_first_response(self, filename):
        with self._find_first_by_type(filename, 'response') as record:
            if record:
                return record.content_stream().read()

    @contextmanager
    def _find_first_by_type(self, filename, match_type, **params):
        with open(get_test_file(filename), 'rb') as fh:
            with closing(ArchiveIterator(fh, **params)) as a:
                for record in a:
                    if record.rec_type == match_type:
                        yield record
                        break

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
            with closing(ArchiveIterator(fh)) as a:
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
        assert a.reader == None
        assert a.read_to_end() == None

    def test_example_warc_trunc(self):
        """ WARC file with content-length truncated on a response record
        Error output printed, but still read
        """
        expected = ['warcinfo', 'warcinfo', 'response', 'request']
        assert self._load_archive('example-trunc.warc', errs_expected=1) == expected

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
        with self._find_first_by_type('example-resource.warc.gz', 'resource') as record:
            assert record.http_headers == None
            assert len(record.content_stream().read()) == int(record.rec_headers.get('Content-Length'))

    def test_resource_with_http_headers(self):
        with self._find_first_by_type('example-resource.warc.gz', 'resource',
                                      ensure_http_headers=True) as record:

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

    def test_corrects_wget_bug(self):
        with self._find_first_by_type('example-wget-bad-target-uri.warc.gz', 'response') as record:
            assert record.rec_headers.get('WARC-Target-URI') == 'http://example.com/'


