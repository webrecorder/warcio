from collections import OrderedDict
from argparse import ArgumentParser, RawTextHelpFormatter

import json
import sys

from warcio.recordloader import ArchiveLoadFailed
from warcio.archiveiterator import ArchiveIterator
from warcio.utils import open_or_default

from warcio.warcwriter import WARCWriter
from warcio.bufferedreaders import DecompressingBufferedReader
import tempfile
import shutil


# ============================================================================
def main(args=None):
    parser = ArgumentParser(description='warcio utils',
                            formatter_class=RawTextHelpFormatter)

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

    cmd = parser.parse_args(args=args)
    cmd.func(cmd)


# ============================================================================
def indexer(cmd):
    fields = cmd.fields.split(',')

    with open_or_default(cmd.output, 'wt', sys.stdout) as out:
        for filename in cmd.inputs:
            with open(filename, 'rb') as fh:
                it = ArchiveIterator(fh, no_record_parse=True, arc2warc=True)
                for record in it:
                    index = OrderedDict()
                    for field in fields:
                        if field == 'offset':
                            value = it.offset
                        else:
                            value = record.rec_headers.get_header(field)
                        if value is not None:
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
if __name__ == "__main__":  #pragma: no cover
    main()

