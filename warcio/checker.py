from __future__ import print_function

from warcio.archiveiterator import ArchiveIterator
from warcio.exceptions import ArchiveLoadFailed


class Checker(object):
    def __init__(self, cmd):
        self.inputs = cmd.inputs
        self.verbose = cmd.verbose
        self.exit_value = 0

    def process_all(self):
        for filename in self.inputs:
            try:
                self.process_one(filename)
            except ArchiveLoadFailed as e:
                print(filename)
                print('  saw exception ArchiveLoadFailed: '+str(e).rstrip())
                print('  skipping rest of file')
                self.exit_value = 1
        return self.exit_value

    def process_one(self, filename):
        with open(filename, 'rb') as stream:
            for record in ArchiveIterator(stream, check_digests=True):
                digest_present = (record.rec_headers.get_header('WARC-Payload-Digest') or
                                  record.rec_headers.get_header('WARC-Block-Digest'))

                record.content_stream().read()  # make sure digests are checked

                d_msg = None
                output = []

                if record.digest_checker.passed is False:
                    self.exit_value = 1
                    output = list(record.digest_checker.problems) 
                elif record.digest_checker.passed is True and self.verbose:
                    d_msg = 'digest pass'
                elif record.digest_checker.passed is None and self.verbose:
                    if digest_present:
                        d_msg = 'digest present but not checked'
                    else:
                        d_msg = 'no digest to check'

                if d_msg or output:
                    rec_id = record.rec_headers.get_header('WARC-Record-ID')
                    rec_type = record.rec_headers.get_header('WARC-Type')
                    print(filename)
                    print(' ', 'WARC-Record-ID', rec_id, rec_type)
                    if d_msg:
                        print('   ', d_msg)
                    for o in output:
                        print('   ', o)
