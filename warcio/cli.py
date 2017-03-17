from collections import OrderedDict
from argparse import ArgumentParser, RawTextHelpFormatter

import json
import sys
import socket
import errno

from warcio.recordloader import ArchiveLoadFailed
from warcio.archiveiterator import ArchiveIterator
from warcio.utils import open_or_default

from warcio.warcwriter import WARCWriter
from warcio.bufferedreaders import DecompressingBufferedReader
import tempfile
import shutil

from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL) 

# ============================================================================
def main(args=None):
    parser = ArgumentParser(description='warcio utils',
                            formatter_class=RawTextHelpFormatter)

    subparsers = parser.add_subparsers(dest='cmd')
    subparsers.required = True

    index = subparsers.add_parser('index', help='WARC/ARC Indexer')
    index.add_argument('inputs', nargs='+')
    index.add_argument('-f', '--fields', default='warc-type,warc-target-uri')
    index.add_argument('-o', '--output')
    index.set_defaults(func=indexer)

    recompress = subparsers.add_parser('recompress', help='WARC/ARC Indexer')
    recompress.add_argument('filename')
    recompress.add_argument('output')
    recompress.set_defaults(func=Recompressor())

    extract = subparsers.add_parser('extract', help='WARC/ARC Record Extractor')
    extract.add_argument('-u', '--uri', help='Record Target URI')
    extract.add_argument('-a', '--all_records', dest='print_all', action='store_true', help='Keep searching the full WARC for any matching records (default)')
    extract.add_argument('-1', '--first_record_only', dest='print_all', action='store_false', help='Return only the first matching WARC record')
    extract.add_argument('-r', '--records', default='response,revisit', help='WARC record types to match. Ex. request,response,revisit,metadata,warcinfo')
    extract.add_argument('inputs', nargs='+')
    extract.set_defaults(print_all=True)
    extract.set_defaults(func=extract_record)

    cmd = parser.parse_args(args=args)
    cmd.func(cmd)

# ============================================================================
def indexer(cmd):
    fields = cmd.fields.split(',')

    with open_or_default(cmd.output, 'wt', sys.stdout) as out:
        for filename in cmd.inputs:
            with open(filename, 'rb') as fh:
                for record in ArchiveIterator(fh,
                                              no_record_parse=True,
                                              arc2warc=True):

                    index = OrderedDict()
                    for field in fields:
                        value = record.rec_headers.get_header(field)
                        if value:
                            index[field] = value

                    out.write(json.dumps(index) + '\n')


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
                                          no_record_parse=True,
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
def extract_record(cmd):
    records = cmd.records.split(',')
    writer = WARCWriter(filebuf=sys.stdout.buffer, gzip=False)
    for filename in cmd.inputs:
        with open(filename, 'rb') as fh:
            for record in ArchiveIterator(fh, no_record_parse=True, arc2warc=True):
                if record.format == 'arc':
                    rec_uri = record.rec_headers.get_header('uri')
                elif record.format in ('warc', 'arc2warc'):
                    rec_uri = record.rec_headers.get_header('WARC-Target-URI')

                if record.rec_type in records and (cmd.uri is None or rec_uri is None or (cmd.uri is not None and cmd.uri == rec_uri)):
                        writer.write_record(record)
                        if(not cmd.print_all):
                            break

# ============================================================================
if __name__ == "__main__":  #pragma: no cover
    main()

