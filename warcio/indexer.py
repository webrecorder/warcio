import json
import sys
import os

from collections import OrderedDict

from warcio.archiveiterator import ArchiveIterator
from warcio.utils import open_or_default


# ============================================================================
class Indexer(object):
    field_names = {}

    def __init__(self, fields, inputs, output, verify_http=False):
        if isinstance(fields, str):
            fields = fields.split(',')
        self.fields = fields
        self.record_parse = any(field.startswith('http:') for field in self.fields)

        self.inputs = inputs
        self.output = output
        self.verify_http = verify_http

    def process_all(self):
        with open_or_default(self.output, 'wt', sys.stdout) as out:
            for filename in self.inputs:
                try:
                    stdin = sys.stdin.buffer
                except AttributeError:  # py2
                    stdin = sys.stdin
                with open_or_default(filename, 'rb', stdin) as fh:
                    self.process_one(fh, out, filename)

    def process_one(self, input_, output, filename):
        it = self._create_record_iter(input_)

        self._write_header(output, filename)

        for record in it:
            self.process_index_entry(it, record, filename, output)

    def process_index_entry(self, it, record, filename, output):
        index = self._new_dict(record)

        for field in self.fields:
            value = self.get_field(record, field, it, filename)

            if value is not None:
                field = self.field_names.get(field, field)
                index[field] = value

        self._write_line(output, index, record, filename)

    def _create_record_iter(self, input_):
        return ArchiveIterator(input_,
                               no_record_parse=not self.record_parse,
                               arc2warc=True,
                               verify_http=self.verify_http)

    def _new_dict(self, record):
        return OrderedDict()

    def get_field(self, record, name, it, filename):
        value = None
        if name == 'offset':
            value = str(it.get_record_offset())
        elif name == 'length':
            value = str(it.get_record_length())
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

    def _write_header(self, out, filename):
        pass

    def _write_line(self, out, index, record, filename):
        out.write(json.dumps(index) + '\n')


