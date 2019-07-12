from warcio.archiveiterator import ArchiveIterator, WARCIterator, ARCIterator
from warcio.exceptions import ArchiveLoadFailed
from warcio.bufferedreaders import DecompressingBufferedReader, BufferedReader

from warcio.warcwriter import BufferWARCWriter

import pytest
from io import BytesIO
import sys

import os

from . import get_test_file
from contextlib import closing, contextmanager
import subprocess


#==============================================================================
class TestArchiveIterator(object):
    def _load_archive(self, filename, offset=0, cls=ArchiveIterator,
                     errs_expected=0, **kwargs):

        with open(get_test_file(filename), 'rb') as fh:
            fh.seek(offset)
            iter_ = cls(fh, **kwargs)
            rec_types = [record.rec_type for record in iter_ if record.digest_checker.passed is not False]

        assert iter_.err_count == errs_expected

        return rec_types

    def _load_archive_memory(self, stream, offset=0, cls=ArchiveIterator,
                             errs_expected=0, full_read=False, **kwargs):

        stream.seek(offset)
        iter_ = cls(stream, **kwargs)
        if full_read:
            rec_types = [record.rec_type for record in iter_
                         if (record.content_stream().read() or True) and record.digest_checker.passed is not False]
        else:
            rec_types = [record.rec_type for record in iter_ if record.digest_checker.passed is not False]

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
                    assert a.get_record_offset() == 0
                    assert record.digest_checker.passed is None
                    assert len(record.digest_checker.problems) == 0
                    break

                record = next(a)
                assert record.rec_type == 'response'
                assert a.get_record_offset() == 405
                assert record.digest_checker.passed is None
                assert len(record.digest_checker.problems) == 0

                for record in a:
                    assert record.rec_type == 'request'
                    assert a.get_record_offset() == 8379
                    assert record.digest_checker.passed is None
                    assert len(record.digest_checker.problems) == 0
                    break

                with pytest.raises(StopIteration):
                    record = next(a)

        assert a.record == None
        assert a.reader == None
        assert a.read_to_end() == None

    def test_unseekable(self):
        """ Test iterator on unseekable 3 record uncompressed WARC input
        """
        proc = subprocess.Popen(['cat', get_test_file('example-iana.org-chunked.warc')],
                                stdout=subprocess.PIPE)

        def raise_tell(x):
            raise Exception()

        # on windows, this tell() exists but doesn't work correctly, so just override (in py3)
        # this is designed to emulated stdin, which does not have a tell(), as expected
        stdout = proc.stdout
        if os.name == 'nt' and hasattr(proc.stdout, 'tell'):
            if sys.version_info < (3, 0):
                stdout = BufferedReader(stdout)
            else:
                stdout.tell = raise_tell

        with closing(ArchiveIterator(stdout)) as a:
            for record in a:
                assert record.rec_type == 'warcinfo'
                assert a.get_record_offset() == 0
                break

            record = next(a)
            assert record.rec_type == 'response'
            assert a.get_record_offset() == 405

            for record in a:
                assert record.rec_type == 'request'
                assert a.get_record_offset() == 8379
                break

            with pytest.raises(StopIteration):
                record = next(a)

        assert a.record == None
        assert a.reader == None
        assert a.read_to_end() == None

        proc.stdout.close()
        proc.wait()

    def test_unseekable_gz(self):
        """ Test iterator on unseekable 3 record uncompressed gzipped WARC input
        """
        proc = subprocess.Popen(['cat', get_test_file('example-resource.warc.gz')],
                                stdout=subprocess.PIPE)

        def raise_tell(x):
            raise Exception()

        # on windows, this tell() exists but doesn't work correctly, so just override (in py3)
        # this is designed to emulated stdin, which does not have a tell(), as expected
        stdout = proc.stdout
        if os.name == 'nt' and hasattr(proc.stdout, 'tell'):
            #can't override tell() in py2
            if sys.version_info < (3, 0):
                stdout = BufferedReader(stdout)
            else:
                stdout.tell = raise_tell

        with closing(ArchiveIterator(stdout)) as a:
            for record in a:
                assert record.rec_type == 'warcinfo'
                assert a.get_record_offset() == 0
                break

            record = next(a)
            assert record.rec_type == 'warcinfo'
            assert a.get_record_offset() == 361

            for record in a:
                assert record.rec_type == 'resource'
                assert a.get_record_offset() == 802
                break

            with pytest.raises(StopIteration):
                record = next(a)

        assert a.record == None
        assert a.reader == None
        assert a.read_to_end() == None

        proc.stdout.close()
        proc.wait()

    def test_example_warc_trunc(self):
        """ WARC file with content-length truncated on a response record
        Error output printed, but still read
        """
        expected = ['warcinfo', 'warcinfo', 'response', 'request']
        assert self._load_archive('example-trunc.warc', errs_expected=1) == expected

        assert self._load_archive('example-trunc.warc', errs_expected=1,
                                  check_digests=True) == expected
        with pytest.raises(ArchiveLoadFailed):
            assert self._load_archive('example-trunc.warc', errs_expected=1,
                                      check_digests='raise') == expected

    def test_example_arc_gz(self):
        expected = ['arc_header', 'response']
        assert self._load_archive('example.arc.gz') == expected

    def test_example_space_in_url_arc(self):
        expected = ['arc_header', 'response']
        assert self._load_archive('example-space-in-url.arc') == expected

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

    def test_corrects_space_in_target_uri(self):
        with self._find_first_by_type('example-space-in-target-uri.warc.gz', 'resource') as record:
            assert record.rec_headers.get('WARC-Target-URI') == 'file:///example%20with%20spaces.png'

    def _digests_mutilate_helper(self, contents, expected_t, expected_f, capsys, full_read=False):
        with pytest.raises(ArchiveLoadFailed):
            assert self._load_archive_memory(BytesIO(contents), check_digests='raise', full_read=full_read) == expected_t
        capsys.readouterr()
        assert self._load_archive_memory(BytesIO(contents), check_digests='log', full_read=full_read) == expected_t
        out, err = capsys.readouterr()
        assert err
        assert self._load_archive_memory(BytesIO(contents), check_digests=True, full_read=full_read) == expected_t
        out, err = capsys.readouterr()
        assert not err
        assert self._load_archive_memory(BytesIO(contents), check_digests=False, full_read=full_read) == expected_f
        out, err = capsys.readouterr()
        assert not err

    def test_digests_mutilate(self, capsys):
        expected_f = ['warcinfo', 'warcinfo', 'response', 'request', 'revisit', 'request']
        expected_t = ['warcinfo', 'warcinfo', 'request', 'revisit', 'request']

        with open(get_test_file('example.warc'), 'rb') as fh:
            contents = fh.read()

        contents_sha = contents.replace(b'WARC-Block-Digest: sha1:', b'WARC-Block-Digest: xxx:', 1)
        assert contents != contents_sha, 'a replace happened'
        self._digests_mutilate_helper(contents_sha, expected_t, expected_f, capsys)

        contents_sha = contents.replace(b'WARC-Payload-Digest: sha1:', b'WARC-Payload-Digest: xxx:', 1)
        assert contents != contents_sha, 'a replace happened'
        self._digests_mutilate_helper(contents_sha, expected_t, expected_f, capsys)

        contents_block = contents
        thing = b'WARC-Block-Digest: sha1:'
        index = contents_block.find(thing)
        index += len(thing)
        b = contents_block[index:index+3]
        contents_block = contents_block.replace(thing+b, thing+b'111')
        assert contents != contents_block, 'a replace happened'
        '''
        If we don't read the stream, the digest check will not happen & all recs will be seen
        '''
        self._digests_mutilate_helper(contents_block, expected_f, expected_f, capsys)
        self._digests_mutilate_helper(contents_block, expected_t, expected_f, capsys, full_read=True)

        contents_payload = contents
        thing = b'WARC-Payload-Digest: sha1:'
        index = contents_payload.find(thing)
        index += len(thing)
        b = contents_payload[index:index+3]
        contents_payload = contents_payload.replace(thing+b, thing+b'111')
        assert contents != contents_payload, 'a replace happened'
        self._digests_mutilate_helper(contents_payload, expected_f, expected_f, capsys)
        self._digests_mutilate_helper(contents_payload, expected_t, expected_f, capsys, full_read=True)

    def test_digests_file(self):
        expected_f = ['request', 'request', 'request', 'request']
        expected_t = ['request', 'request', 'request']

        # record 1: invalid payload digest
        assert self._load_archive('example-digest.warc', check_digests=True) == expected_t
        assert self._load_archive('example-digest.warc', check_digests=False) == expected_f

        # record 2: b64 digest; record 3: b64 filename safe digest
        assert self._load_archive('example-digest.warc', offset=922, check_digests=True) == expected_t
        assert self._load_archive('example-digest.warc', offset=922, check_digests=False) == expected_t
