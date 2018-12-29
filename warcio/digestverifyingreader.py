import base64
import sys

from warcio.limitreader import LimitReader
from warcio.utils import to_native_str, Digester
from warcio.exceptions import ArchiveLoadFailed


# ============================================================================
class CheckDigest(object):
    def __init__(self, kind=None):
        self._problem = []
        self._status = None
        self.kind = kind

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def problems(self):
        return self._problem

    def problem(self, value):
        self._problem.append(value)
        if self.kind == 'raise':
            raise ArchiveLoadFailed(value)
        if self.kind == 'log':
            sys.stderr.write(value + '\n')
        self.status = False


# ============================================================================
class DigestVerifyingReader(LimitReader):
    """
    A reader which verifies the digest of the wrapped reader
    """

    def __init__(self, stream, limit, check_digest, record_type=None,
                 payload_digest=None, block_digest=None, segment_number=None):

        super(DigestVerifyingReader, self).__init__(stream, limit)

        self.check_digest = check_digest

        if record_type == 'revisit':
            block_digest = None  # XXX my bug, or is example.warc wrong?
            payload_digest = None  # no payload, so can't check it
        if segment_number is not None:  #pragma: no cover
            payload_digest = None

        self.payload_digest = payload_digest
        self.block_digest = block_digest

        self.payload_digester = None
        self.payload_digester_obj = None
        self.block_digester = None

        if block_digest:
            try:
                algo, _ = _parse_digest(block_digest)
                self.block_digester = Digester(algo)
            except ValueError:
                self.problem('unknown hash algorithm name in block digest')
                self.block_digester = None
        if payload_digest:
            try:
                algo, _ = _parse_digest(self.payload_digest)
                self.payload_digester_obj = Digester(algo)
            except ValueError:
                self.problem('unknown hash algorithm name in payload digest')
                self.payload_digester_obj = None

    def begin_payload(self):
        self.payload_digester = self.payload_digester_obj
        if self.limit == 0:
            if _compare_digest_rfc_3548(self.payload_digester, self.payload_digest) is False:
                self.problem('payload digest failed: {}'.format(self.payload_digest))
                self.payload_digester = None  # prevent double-fire

    def _update(self, buff):
        super(DigestVerifyingReader, self)._update(buff)

        if self.payload_digester:
            self.payload_digester.update(buff)
        if self.block_digester:
            self.block_digester.update(buff)

        if self.limit == 0:
            if _compare_digest_rfc_3548(self.block_digester, self.block_digest) is False:
                self.problem('block digest failed: {}'.format(self.block_digest))
            if _compare_digest_rfc_3548(self.payload_digester, self.payload_digest) is False:
                self.problem('payload digest failed {}'.format(self.payload_digest))

        return buff

    def problem(self, reason):
        self.check_digest.problem(reason)


def _compare_digest_rfc_3548(digester, digest):
    '''
    The WARC standard does not recommend a digest algorithm and appears to
    allow any encoding from RFC3548. The Python base64 module supports
    RFC3548 although the base64 alternate alphabet is not exactly a first
    class citizen. Hopefully digest algos are named with the same names
    used by OpenSSL.
    '''
    if not digester or not digest:
        return None

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
        binary = base64.b16decode(value, casefold=True)
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
