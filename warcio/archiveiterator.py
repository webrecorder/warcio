from warcio.bufferedreaders import DecompressingBufferedReader

from warcio.exceptions import ArchiveLoadFailed
from warcio.recordloader import ArcWarcRecordLoader

from warcio.utils import BUFF_SIZE

import sys
import six

# ============================================================================
class UnseekableYetTellable:
    def __init__(self, fh):
        self.fh = fh
        self.offset = 0

    def tell(self):
        return self.offset

    def read(self, size=-1):
        result = self.fh.read(size)
        self.offset += len(result)
        return result

# ============================================================================
class ArchiveIterator(six.Iterator):
    """ Iterate over records in WARC and ARC files, both gzip chunk
    compressed and uncompressed

    The indexer will automatically detect format, and decompress
    if necessary.

    """

    GZIP_ERR_MSG = """
    ERROR: non-chunked gzip file detected, gzip block continues
    beyond single record.

    This file is probably not a multi-member gzip but a single gzip file.

    To allow seek, a gzipped {1} must have each record compressed into
    a single gzip member and concatenated together.

    This file is likely still valid and can be fixed by running:

    warcio recompress <path/to/file> <path/to/new_file>

"""

    INC_RECORD = """\
    WARNING: Record not followed by newline, perhaps Content-Length is invalid
    Offset: {0}
    Remainder: {1}
"""

    def __init__(self, fileobj, no_record_parse=False,
                 verify_http=False, arc2warc=False,
                 ensure_http_headers=False, block_size=BUFF_SIZE,
                 check_digests=False):

        self.fh = fileobj

        self.loader = ArcWarcRecordLoader(verify_http=verify_http,
                                          arc2warc=arc2warc)
        self.known_format = None

        self.mixed_arc_warc = arc2warc

        self.member_info = None
        self.no_record_parse = no_record_parse
        self.ensure_http_headers = ensure_http_headers

        try:
            self.offset = self.fh.tell()
        except:
            self.fh = UnseekableYetTellable(self.fh)
            self.offset = self.fh.tell()

        self.reader = DecompressingBufferedReader(self.fh,
                                                  block_size=block_size)

        self.next_line = None

        self.check_digests = check_digests
        self.err_count = 0
        self.record = None

        self.the_iter = self._iterate_records()

    def __iter__(self):
        return self.the_iter

    def __next__(self):
        return six.next(self.the_iter)

    def close(self):
        self.record = None
        if self.reader:
            self.reader.close_decompressor()
            self.reader = None

    def _iterate_records(self):
        """ iterate over each record
        """
        raise_invalid_gzip = False
        empty_record = False

        while True:
            try:
                self.record = self._next_record(self.next_line)
                if raise_invalid_gzip:
                    self._raise_invalid_gzip_err()

                yield self.record

            except EOFError:
                empty_record = True

            self.read_to_end()

            if self.reader.decompressor:
                # if another gzip member, continue
                if self.reader.read_next_member():
                    continue

                # if empty record, then we're done
                elif empty_record:
                    break

                # otherwise, probably a gzip
                # containing multiple non-chunked records
                # raise this as an error
                else:
                    raise_invalid_gzip = True

            # non-gzip, so we're done
            elif empty_record:
                break

        self.close()

    def _raise_invalid_gzip_err(self):
        """ A gzip file with multiple ARC/WARC records, non-chunked
        has been detected. This is not valid for replay, so notify user
        """
        frmt = 'warc/arc'
        if self.known_format:
            frmt = self.known_format

        frmt_up = frmt.upper()

        msg = self.GZIP_ERR_MSG.format(frmt, frmt_up)
        raise ArchiveLoadFailed(msg)

    def _consume_blanklines(self):
        """ Consume blank lines that are between records
        - For warcs, there are usually 2
        - For arcs, may be 1 or 0
        - For block gzipped files, these are at end of each gzip envelope
          and are included in record length which is the full gzip envelope
        - For uncompressed, they are between records and so are NOT part of
          the record length

          count empty_size so that it can be substracted from
          the record length for uncompressed

          if first line read is not blank, likely error in WARC/ARC,
          display a warning
        """
        empty_size = 0
        first_line = True

        while True:
            line = self.reader.readline()
            if len(line) == 0:
                return None, empty_size

            stripped = line.rstrip()

            if len(stripped) == 0 or first_line:
                empty_size += len(line)

                if len(stripped) != 0:
                    # if first line is not blank,
                    # likely content-length was invalid, display warning
                    err_offset = self.fh.tell() - self.reader.rem_length() - empty_size
                    sys.stderr.write(self.INC_RECORD.format(err_offset, line))
                    self.err_count += 1

                first_line = False
                continue

            return line, empty_size

    def read_to_end(self, record=None):
        """ Read remainder of the stream
        If a digester is included, update it
        with the data read
        """

        # no current record to read
        if not self.record:
            return None

        # already at end of this record, don't read until it is consumed
        if self.member_info:
            return None

        curr_offset = self.offset

        while True:
            b = self.record.raw_stream.read(BUFF_SIZE)
            if not b:
                break

        """
        - For compressed files, blank lines are consumed
          since they are part of record length
        - For uncompressed files, blank lines are read later,
          and not included in the record length
        """
        #if self.reader.decompressor:
        self.next_line, empty_size = self._consume_blanklines()

        self.offset = self.fh.tell() - self.reader.rem_length()
        #if self.offset < 0:
        #    raise Exception('Not Gzipped Properly')

        if self.next_line:
            self.offset -= len(self.next_line)

        length = self.offset - curr_offset

        if not self.reader.decompressor:
            length -= empty_size

        self.member_info = (curr_offset, length)
        #return self.member_info
        #return next_line

    def get_record_offset(self):
        if not self.member_info:
            self.read_to_end()

        return self.member_info[0]

    def get_record_length(self):
        if not self.member_info:
            self.read_to_end()

        return self.member_info[1]

    def _next_record(self, next_line):
        """ Use loader to parse the record from the reader stream
        Supporting warc and arc records
        """
        record = self.loader.parse_record_stream(self.reader,
                                                 next_line,
                                                 self.known_format,
                                                 self.no_record_parse,
                                                 self.ensure_http_headers,
                                                 self.check_digests)

        self.member_info = None

        # Track known format for faster parsing of other records
        if not self.mixed_arc_warc:
            self.known_format = record.format

        return record


# ============================================================================
class WARCIterator(ArchiveIterator):
    def __init__(self, *args, **kwargs):
        super(WARCIterator, self).__init__(*args, **kwargs)
        self.known_format = 'warc'


# ============================================================================
class ARCIterator(ArchiveIterator):
    def __init__(self, *args, **kwargs):
        super(ARCIterator, self).__init__(*args, **kwargs)
        self.known_format = 'arc'



