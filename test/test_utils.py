import sys
import pytest
from collections import Counter

import warcio.utils as utils

try:
    from multidict import CIMultiDict, MultiDict
except ImportError:
    pass


class TestUtils(object):
    def test_headers_to_str_headers(self):
        result = [('foo', 'bar'), ('baz', 'barf')]

        header_dict = {'foo': b'bar', b'baz': 'barf'}
        ret = utils.headers_to_str_headers(header_dict)
        assert Counter(ret) == Counter(result)

        aiohttp_raw_headers = ((b'foo', b'bar'), (b'baz', b'barf'))
        assert Counter(utils.headers_to_str_headers(aiohttp_raw_headers)) == Counter(result)

    @pytest.mark.skipif('multidict' not in sys.modules, reason='requires multidict be installed')
    def test_multidict_headers_to_str_headers(self):
        result = [('foo', 'bar'), ('baz', 'barf')]

        aiohttp_headers = MultiDict(foo='bar', baz=b'barf')
        ret = utils.headers_to_str_headers(aiohttp_headers)
        assert Counter(ret) == Counter(result)

        # This case-insensitive thingie titlecases the key
        aiohttp_headers = CIMultiDict(foo='bar', baz=b'barf')
        titlecase_result = [('Foo', 'bar'), ('Baz', 'barf')]

        ret = utils.headers_to_str_headers(aiohttp_headers)
        assert Counter(ret) == Counter(titlecase_result)

