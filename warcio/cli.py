from collections import OrderedDict
from argparse import ArgumentParser, RawTextHelpFormatter

import json
import sys
import os

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

    cmd = parser.parse_args(args=args)
    cmd.func(cmd)


# ============================================================================
def get_version():
    import pkg_resources
    return '%(prog)s ' + pkg_resources.get_distribution('warcio').version


# ============================================================================
def indexer(cmd):
    indexer = Indexer(cmd.fields, cmd.inputs, cmd.output)
    indexer.process_all()


# ============================================================================
class Indexer(object):
    field_names = {}

    def __init__(self, fields, inputs, output):
        if isinstance(fields, str):
            fields = fields.split(',')
        self.fields = fields
        self.no_record_parse = not any(field.startswith('http:') for field in self.fields)

        self.inputs = inputs
        self.output = output

    def process_all(self):
        with open_or_default(self.output, 'wt', sys.stdout) as out:
            for filename in self.inputs:
                with open(filename, 'rb') as fh:
                    self.process_one(fh, out, filename)

    def process_one(self, input_, output, filename):
        it = ArchiveIterator(input_,
                             no_record_parse=self.no_record_parse,
                             arc2warc=True)

        for record in it:
            index = self._new_dict(record)

            for field in self.fields:
                value = self.get_field(record, field, it, filename)

                if value is not None:
                    field = self.field_names.get(field, field)
                    index[field] = value

            self._write_line(output, index, record, filename)

    def _new_dict(self, record):
        return OrderedDict()

    def get_field(self, record, name, it, filename):
        value = None
        if name == 'offset':
            value = it.get_record_offset()
        elif name == 'length':
            value = it.get_record_length()
        elif name == 'filename':
            value = os.path.basename(filename)
        elif name == 'http:status':
            if record.rec_type in ('response', 'revisit') and record.http_headers:
                value = record.http_headers.get_statuscode()
        elif name.startswith('http:'):
            if record.http_headers:
                value = record.http_headers.get_header(name[5:])
        else:
            value = record.rec_headers.get_header(name)

        return value

    def _write_line(self, out, index, record, filename):
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

