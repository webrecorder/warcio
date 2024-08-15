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
                    with open(self.output, 'wb') as out:
                        count = self._load_and_write_stream(stream, out)
                        msg = 'No Errors Found!'
                except Exception as e:
                    if self.verbose:
                        print('Parsing Error(s) Found:')
                        print(str(e) if isinstance(e, ArchiveLoadFailed) else repr(e))
                        print()

                    with open(self.output, 'wb') as out, tempfile.TemporaryFile() as tout:
                        count = self._decompress_and_recompress_stream(stream, out, tout)
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
        with open(output, 'wb') as out:
            return self._load_and_write_stream(stream, out)

    def decompress_and_recompress(self, stream, output):
        with open(output, 'wb') as out, tempfile.TemporaryFile() as tout:
            return self._decompress_and_recompress_stream(stream, out, tout)

    def _load_and_write_stream(self, in_stream, out_stream):
        count = 0
        writer = WARCWriter(filebuf=out_stream, gzip=True)

        for record in ArchiveIterator(in_stream,
                                      no_record_parse=False,
                                      arc2warc=True,
                                      verify_http=False):

            writer.write_record(record)
            count += 1

        return count

    def _decompress_and_recompress_stream(self, in_stream, out_stream, tmp_stream):
        decomp = DecompressingBufferedReader(in_stream, read_all_members=True)

        # decompress entire file to temp file
        in_stream.seek(0)
        shutil.copyfileobj(decomp, tmp_stream)

        # attempt to compress and write temp
        tmp_stream.seek(0)
        return self._load_and_write_stream(tmp_stream, out_stream)
