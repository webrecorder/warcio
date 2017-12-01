import base64

from warcio.utils import to_native_str, Digester

# ============================================================================
class LimitReader(object):
    """
    A reader which will not read more than specified limit
    """

    def __init__(self, stream, limit):
        self.stream = stream
        self.limit = limit
        self.payload_digester = None
        self.block_digester = None
        self.payload_digest = None
        self.block_digest = None

        if hasattr(stream, 'tell'):
            self.tell = self._tell

    def _update(self, buff):
        length = len(buff)
        self.limit -= length

        if self.payload_digester:
            self.payload_digester.update(buff)
        if self.block_digester:
            self.block_digester.update(buff)

        if self.limit == 0:
            if not _compare_digest_rfc_3548(self.block_digester, self.block_digest):
                raise ValueError('block digest failed')
            if not _compare_digest_rfc_3548(self.payload_digester, self.payload_digest):
                raise ValueError('payload digest failed')

        return buff

    def read(self, length=None):
        if length is not None:
            length = min(length, self.limit)
        else:
            length = self.limit

        if length == 0:
            return b''

        buff = self.stream.read(length)
        return self._update(buff)

    def readline(self, length=None):
        if length is not None:
            length = min(length, self.limit)
        else:
            length = self.limit

        if length == 0:
            return b''

        buff = self.stream.readline(length)
        return self._update(buff)

    def close(self):
        self.stream.close()

    def _tell(self):
        return self.stream.tell()

    @staticmethod
    def wrap_stream(stream, content_length):
        """
        If given content_length is an int > 0, wrap the stream
        in a LimitReader. Otherwise, return the stream unaltered
        """
        try:
            content_length = int(content_length)
            if content_length >= 0:
                # optimize: if already a LimitStream, set limit to
                # the smaller of the two limits
                if isinstance(stream, LimitReader):
                    stream.limit = min(stream.limit, content_length)
                else:
                    stream = LimitReader(stream, content_length)

        except (ValueError, TypeError):
            pass

        return stream

    def configure_digesters(self, rec_type, segment_number, payload_digest, block_digest):
        if rec_type == 'revisit':
            block_digest = None  # XXX my bug, or is example.warc wrong?
            payload_digest = None  # not a bug
        if segment_number is not None:  #pragma: no cover
            payload_digest = None

        if block_digest:
            algo, _ = _parse_digest(block_digest)
            self.block_digester = Digester(algo)
            self.block_digest = block_digest
        if payload_digest:
            # don't start the payload digest yet
            self.payload_digest = payload_digest

        return payload_digest is not None

    def begin_payload(self):
        if self.payload_digest:
            algo, _ = _parse_digest(self.payload_digest)
            self.payload_digester = Digester(algo)
        if self.limit == 0:
            # payload is of length 0
            if not _compare_digest_rfc_3548(self.payload_digester, self.payload_digest):
                raise ValueError('payload digest failed')


def _compare_digest_rfc_3548(digester, digest):
    '''
    The WARC standard does not recommend a digest algorithm and appears to
    allow any encoding from RFC3548. The Python base64 module supports
    RFC3548 although the base64 alternate alphabet is not exactly a first
    class citizen. Hopefully digest algos are named with the same names
    used by OpenSSL.
    '''
    if not digester or not digest:
        return True

    digester_b32 = str(digester)

    our_algo, our_value = _parse_digest(digester_b32)
    warc_algo, warc_value = _parse_digest(digest)

    warc_b32 = _to_b32(len(our_value), warc_value)

    if our_value == warc_b32:
        return True

    return False


def _to_b32(length, value):
    '''
    Convert value to base 32, given that it's supposed to have the same
    length as the digest we're about to compare it to
    '''
    if len(value) == length:
        return value  # casefold needed here? -- rfc recommends not allowing

    if len(value) > length:
        binary = base64.b16decode(value, casefold=True)  # we know Ilya does lowercase
    else:
        binary = _b64_wrapper(value)

    return to_native_str(base64.b32encode(binary), encoding='ascii')


base64_url_filename_safe_alt = b'-_'


def _b64_wrapper(value):
    if '-' in value or '_' in value:
        return base64.b64decode(value, altchars=base64_url_filename_safe_alt)
    else:
        return base64.b64decode(value)


def _parse_digest(digest):
    algo, sep, value = digest.partition(':')
    if sep == ':':
        return algo, value
    else:
        raise ValueError('could not parse digest algorithm out of '+digest)
