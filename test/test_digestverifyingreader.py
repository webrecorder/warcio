import pytest

from warcio.digestverifyingreader import _compare_digest_rfc_3548
from warcio.utils import Digester


empty_sha1_b32 = '3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ'
empty_sha1_b64 = '2jmj7l5rSw0yVb/vlWAYkK/YBwk='
empty_sha1_b64_alt = '2jmj7l5rSw0yVb_vlWAYkK_YBwk='
empty_sha1_b16 = 'DA39A3EE5E6B4B0D3255BFEF95601890AFD80709'


def test_compare_digest_rfc_3548():
    assert _compare_digest_rfc_3548(None, None) is None
    sha1d = Digester('sha1')
    assert _compare_digest_rfc_3548(sha1d, 'sha1:'+empty_sha1_b32) is True
    assert _compare_digest_rfc_3548(sha1d, 'sha1:'+empty_sha1_b32.replace('3I', 'xx')) is False

    assert _compare_digest_rfc_3548(sha1d, 'sha1:'+empty_sha1_b64) is True
    assert _compare_digest_rfc_3548(sha1d, 'sha1:'+empty_sha1_b64_alt) is True
    assert _compare_digest_rfc_3548(sha1d, 'sha1:'+empty_sha1_b16) is True
    assert _compare_digest_rfc_3548(sha1d, 'sha1:'+empty_sha1_b16.lower()) is True

    with pytest.raises(ValueError):
        assert _compare_digest_rfc_3548(sha1d, 'foo') is False
        assert _compare_digest_rfc_3548(sha1d, 'foo:bar') is False
