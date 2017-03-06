from collections import OrderedDict
from argparse import ArgumentParser, RawTextHelpFormatter

import json
import sys

from warcio.recordloader import ArchiveLoadFailed
from warcio.archiveiterator import ArchiveIterator
from warcio.utils import open_or_default


# ============================================================================
def main(args=None):
    parser = ArgumentParser(description='warcio utils',
                            formatter_class=RawTextHelpFormatter)

    subparsers = parser.add_subparsers(dest='type')

    index = subparsers.add_parser('index', help='WARC/ARC Indexer')
    index.add_argument('inputs', nargs='+')
    index.add_argument('-f', '--fields', default='warc-type,warc-target-uri')
    index.add_argument('-o', '--output')
    index.set_defaults(func=indexer)

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
if __name__ == "__main__":  #pragma: no cover
    main()

