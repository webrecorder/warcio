from argparse import ArgumentParser, RawTextHelpFormatter

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

from warcio.warcwriter import WARCWriter
from warcio.bufferedreaders import DecompressingBufferedReader

from warcio.indexer import Indexer
from warcio.utils import BUFF_SIZE

import tempfile
import shutil
import sys


# ============================================================================
def main(args=None):
    parser = ArgumentParser(description='warcio utils',
                            formatter_class=RawTextHelpFormatter)

    parser.add_argument('-V', '--version', action='version', version=get_version())

    subparsers = parser.add_subparsers(dest='cmd')
    subparsers.required = True

    index = subparsers.add_parser('index', help='WARC/ARC Indexer')
    index.add_argument('inputs', nargs='+')
    index.add_argument('-f', '--fields', default='offset,warc-type,warc-target-uri')
    index.add_argument('-o', '--output')
    index.set_defaults(func=indexer)

    recompress = subparsers.add_parser('recompress', help='Recompress an existing WARC or ARC',
                                       description='Read an existing, possibly broken WARC ' +
                                                   'and correctly recompress it to fix any compression errors\n' +
                                                   'Also convert any ARC file to a standard compressed WARC file')
    recompress.add_argument('filename')
    recompress.add_argument('output')
    recompress.add_argument('-v', '--verbose', action='store_true')
    recompress.set_defaults(func=Recompressor())

    extract = subparsers.add_parser('extract', help='Extract WARC/ARC Record')
    extract.add_argument('filename')
    extract.add_argument('offset')
    group = extract.add_mutually_exclusive_group()
    group.add_argument('--payload', action='store_true', help='output only record payload (after content and transfer decoding, if applicable)')
    group.add_argument('--headers', action='store_true', help='output only record headers (and http headers, if applicable)')

    extract.set_defaults(func=extract_record)

    cmd = parser.parse_args(args=args)
    cmd.func(cmd)

# ============================================================================
def extract_record(cmd):
    READ_SIZE = BUFF_SIZE * 4

    with open(cmd.filename, 'rb') as fh:
        fh.seek(int(cmd.offset))
        it = iter(ArchiveIterator(fh))
        record = next(it)

        try:
            stdout_raw = sys.stdout.buffer
        except AttributeError:  #pragma: no cover
            stdout_raw = sys.stdout

        if cmd.payload:
            stream = record.content_stream()
            buf = stream.read(READ_SIZE)
            while buf:
                stdout_raw.write(buf)
                buf = stream.read(READ_SIZE)
        else:
            stdout_raw.write(record.rec_headers.to_bytes())
            if record.http_headers:
                stdout_raw.write(record.http_headers.to_bytes())
            if not cmd.headers:
                buf = record.raw_stream.read(READ_SIZE)
                while buf:
                    stdout_raw.write(buf)
                    buf = record.raw_stream.read(READ_SIZE)


# ============================================================================
def get_version():
    import pkg_resources
    return '%(prog)s ' + pkg_resources.get_distribution('warcio').version


# ============================================================================
def indexer(cmd):
    indexer = Indexer(cmd.fields, cmd.inputs, cmd.output)
    indexer.process_all()


# ============================================================================
class Recompressor(object):
    def __call__(self, cmd):
        try:
            count = 0
            msg = ''
            with open(cmd.filename, 'rb') as stream:
                try:
                    count = self.load_and_write(stream, cmd.output)
                    msg = 'No Errors Found!'
                except Exception as e:
                    if cmd.verbose:
                        print('Parsing Error(s) Found:')
                        print(str(e) if isinstance(e, ArchiveLoadFailed) else repr(e))
                        print()

                    count = self.decompress_and_recompress(stream, cmd.output)
                    msg = 'Compression Errors Found and Fixed!'

                if cmd.verbose:
                    print('Records successfully read and compressed:')
                    main(['index', cmd.output])
                    print('')

                print('{0} records read and recompressed to file: {1}'.format(count, cmd.output))
                print(msg)

        except:
            if cmd.verbose:
                print('Exception Details:')
                import traceback
                traceback.print_exc()
                print('')

            print('Recompress Failed: {0} could not be read as a WARC or ARC'.format(cmd.filename))
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


# ============================================================================
if __name__ == "__main__":  #pragma: no cover
    main()

