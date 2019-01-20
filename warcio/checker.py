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
            file_printed = False
            filename = filename
            for record in ArchiveIterator(stream, check_digests=True):
                record.content_stream().read()  # make sure digests are checked
                rec_id = record.rec_headers.get_header('WARC-Record-ID')
                rec_type = record.rec_headers.get_header('WARC-Type')
                if record.digest_checker.passed is False:
                    self.exit_value = 1
                    file_printed = _fprint(filename, file_printed)
                    print(' ', 'WARC-Record-ID', rec_id, rec_type)
                    for p in record.digest_checker.problems:
                        print('  ', p)
                elif record.digest_checker.passed is True and self.verbose:
                    file_printed = _fprint(filename, file_printed)
                    print(' ', 'WARC-Record-ID', rec_id, rec_type)
                    print('   digest pass')
                elif record.digest_checker.passed is None and self.verbose:
                    file_printed = _fprint(filename, file_printed)
                    print(' ', 'WARC-Record-ID', rec_id, rec_type)
                    print('   digest not checked')


def _fprint(filename, file_printed):
    if not file_printed:
        print(filename)
    return True
