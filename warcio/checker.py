from __future__ import print_function

from warcio.archiveiterator import ArchiveIterator


class Checker(object):
    def __init__(self, cmd):
        self.inputs = cmd.inputs
        self.verbose = cmd.verbose
        self.exit_value = 0

    def process_all(self):
        for filename in self.inputs:
            self.process_one(filename)
        return self.exit_value

    def process_one(self, filename):
        with open(filename, 'rb') as stream:
            self.file_printed = False
            self.filename = filename
            for record in ArchiveIterator(stream, check_digests=True):
                # XXX skip if Arc and not Warc
                record.content_stream().read()  # make sure digests are checked
                rec_id = record.rec_headers.get_header('WARC-Record-ID')
                rec_type = record.rec_headers.get_header('WARC-Type')
                if record.digest_checker.status is False:
                    self.exit_value = 1
                    self.fprint()
                    print(' ', 'WARC-Record-ID', rec_id, rec_type)
                    for p in record.digest_checker.problems:
                        print('  ', p)
                elif record.digest_checker.status is True and self.verbose:
                    self.fprint()
                    print(' ', 'WARC-Record-ID', rec_id, rec_type)
                    print('   digest pass')
                elif record.digest_checker.status is None and self.verbose:
                    self.fprint()
                    print(' ', 'WARC-Record-ID', rec_id, rec_type)
                    print('   digest not checked')

    def fprint(self):
        if not self.file_printed:
            print(self.filename)
            self.file_printed = True
