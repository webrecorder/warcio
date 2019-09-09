from warcio.archiveiterator import ArchiveIterator

from warcio.utils import BUFF_SIZE
import sys


# ============================================================================
class Extractor(object):
    READ_SIZE = BUFF_SIZE * 4

    def __init__(self, filename, offset):
        self.filename = filename
        self.offset = offset

    def extract(self, payload_only, headers_only):
        with open(self.filename, 'rb') as fh:
            fh.seek(int(self.offset))
            it = iter(ArchiveIterator(fh))
            record = next(it)

            try:
                stdout_raw = sys.stdout.buffer
            except AttributeError:  #pragma: no cover
                stdout_raw = sys.stdout

            if payload_only:
                stream = record.content_stream()
                buf = stream.read(self.READ_SIZE)
                while buf:
                    stdout_raw.write(buf)
                    buf = stream.read(self.READ_SIZE)
            else:
                stdout_raw.write(record.rec_headers.to_bytes())
                if record.http_headers:
                    stdout_raw.write(record.http_headers.to_bytes())
                if not headers_only:
                    buf = record.raw_stream.read(self.READ_SIZE)
                    while buf:
                        stdout_raw.write(buf)
                        buf = record.raw_stream.read(self.READ_SIZE)


