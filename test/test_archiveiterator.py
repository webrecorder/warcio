from pywb.warclib.archiveiterator import ArchiveIterator
from pywb.warclib.recordloader import ArchiveLoadFailed

from pywb import get_test_dir

import pytest

test_warc_dir = get_test_dir() + 'warcs/'


#==============================================================================
class TestArchiveIterator(object):
    def _load_archive(self, filename, offset=0, **kwargs):
        with open(test_warc_dir + filename, 'rb') as fh:
            fh.seek(offset)
            iter_ = ArchiveIterator(fh, **kwargs)
            rec_types = [record.rec_type for record in iter_]

        return rec_types

    def test_example_warc_gz(self):
        expected = ['warcinfo', 'response', 'request', 'revisit', 'request', 'response']
        assert self._load_archive('example.warc.gz') == expected

    def test_example_warc(self):
        expected = ['warcinfo', 'response', 'request', 'revisit', 'request', 'response']
        assert self._load_archive('example.warc') == expected

    def test_example_arc_gz(self):
        expected = ['arc_header', 'response']
        assert self._load_archive('example.arc.gz') == expected

    def test_example_arc(self):
        expected = ['arc_header', 'response']
        assert self._load_archive('example.arc') == expected

    def test_example_arc2warc(self):
        expected = ['arc_header', 'response']
        assert self._load_archive('example.arc.gz', arc2warc=True) == expected

    def test_bad_warc(self):
        with pytest.raises(Exception):
            self._load_archive('example-bad.warc.gz.bad')

    def test_bad_offset_warc(self):
        with pytest.raises(ArchiveLoadFailed):
            self._load_archive('example.warc.gz', offset=10)

    def test_bad_arc_invalid_lengths(self):
        expected = ['arc_header', 'response', 'response', 'response']
        assert self._load_archive('bad.arc') == expected




