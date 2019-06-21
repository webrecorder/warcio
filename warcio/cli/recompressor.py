from warcio.archiveiterator import ArchiveIterator
from warcio.exceptions import ArchiveLoadFailed

from warcio.warcwriter import WARCWriter
from warcio.bufferedreaders import DecompressingBufferedReader

import tempfile
import shutil
import traceback
import sys


# ============================================================================
class Recompressor(object):
    def __init__(self, filename, output, verbose=False):
        self.filename = filename
        self.output = output
        self.verbose = verbose

    def recompress(self):
        from warcio.cli import main
        try:
            count = 0
            msg = ''
            with open(self.filename, 'rb') as stream:
                try:
                    count = self.load_and_write(stream, self.output)
                    msg = 'No Errors Found!'
                except Exception as e:
                    if self.verbose:
                        print('Parsing Error(s) Found:')
                        print(str(e) if isinstance(e, ArchiveLoadFailed) else repr(e))
                        print()

                    count = self.decompress_and_recompress(stream, self.output)
                    msg = 'Compression Errors Found and Fixed!'

                if self.verbose:
                    print('Records successfully read and compressed:')
                    main(['index', self.output])
                    print('')

                print('{0} records read and recompressed to file: {1}'.format(count, self.output))
                print(msg)

        except:
            if self.verbose:
                print('Exception Details:')
                traceback.print_exc()
                print('')

            print('Recompress Failed: {0} could not be read as a WARC or ARC'.format(self.filename))
            sys.exit(1)

    def load_and_write(self, stream, output):
        count = 0
        with open(output, 'wb') as out:
            writer = WARCWriter(filebuf=out, gzip=True)

            for record in ArchiveIterator(stream,
                                          no_record_parse=False,
                                          arc2warc=True,
                                          verify_http=False):

                writer.write_record(record)
                count += 1

            return count

    def decompress_and_recompress(self, stream, output):
        with tempfile.TemporaryFile() as tout:
            decomp = DecompressingBufferedReader(stream, read_all_members=True)

            # decompress entire file to temp file
            stream.seek(0)
            shutil.copyfileobj(decomp, tout)

            # attempt to compress and write temp
            tout.seek(0)
            return self.load_and_write(tout, output)



