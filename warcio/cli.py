from argparse import ArgumentParser, RawTextHelpFormatter

from warcio.recordloader import ArchiveLoadFailed
from warcio.archiveiterator import ArchiveIterator

from warcio.warcwriter import WARCWriter
from warcio.bufferedreaders import DecompressingBufferedReader

from warcio.indexer import Indexer

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

    recompress = subparsers.add_parser('recompress', help='WARC/ARC Indexer')
    recompress.add_argument('filename')
    recompress.add_argument('output')
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
    with open(cmd.filename, 'rb') as fh:
        fh.seek(int(cmd.offset))
        it = iter(ArchiveIterator(fh))
        record = next(it)

        try:
            stdout_raw = sys.stdout.buffer
        except AttributeError:
            stdout_raw = sys.stdout

        if cmd.payload:
            stream = record.content_stream()
            buf = stream.read(65536)
            while buf:
                stdout_raw.write(buf)
                buf = stream.read(65536)
        else:
            stdout_raw.write(record.rec_headers.to_bytes())
            if record.http_headers:
                stdout_raw.write(record.http_headers.to_bytes())
            if not cmd.headers:
                buf = record.raw_stream.read(65536)
                while buf:
                    stdout_raw.write(buf)
                    buf = record.raw_stream.read(65536)

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
        with open(cmd.filename, 'rb') as stream:
            try:
                self.load_and_write(stream, cmd.output)

            except ArchiveLoadFailed as af:
                if 'ERROR: non-chunked gzip file detected' in af.msg:
                    self.decompress_and_recompress(stream, cmd.output)
                else:
                    raise

    def load_and_write(self, stream, output):
        with open(output, 'wb') as out:
            writer = WARCWriter(filebuf=out, gzip=True)

            for record in ArchiveIterator(stream,
                                          no_record_parse=False,
                                          arc2warc=True,
                                          verify_http=False):

                writer.write_record(record)

    def decompress_and_recompress(self, stream, output):
        with tempfile.TemporaryFile() as tout:
            decomp = DecompressingBufferedReader(stream)

            # decompress entire file to temp file
            stream.seek(0)
            shutil.copyfileobj(decomp, tout)

            # attempt to compress and write temp
            tout.seek(0)
            self.load_and_write(tout, output)


# ============================================================================
if __name__ == "__main__":  #pragma: no cover
    main()

