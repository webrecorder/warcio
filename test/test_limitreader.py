from pywb.warclib.limitreader import LimitReader
from contextlib import closing
import requests

from io import BytesIO

class TestLimitReader(object):
    def test_limit_reader_1(self):
        assert b'abcdefghji' == LimitReader(BytesIO(b'abcdefghjiklmnopqrstuvwxyz'), 10).read(26)

    def test_limit_reader_2(self):
        assert b'abcdefgh' == LimitReader(BytesIO(b'abcdefghjiklmnopqrstuvwxyz'), 8).readline(26)

    def test_limit_reader_3(self):
        reader = LimitReader(BytesIO(b'abcdefghjiklmnopqrstuvwxyz'), 8)
        new_reader = LimitReader.wrap_stream(reader, 4)
        assert reader == new_reader
        assert b'abcd' == new_reader.readline(26)
        #assert b'abcd' == LimitReader.wrap_stream(LimitReader(BytesIO(b'abcdefghjiklmnopqrstuvwxyz'), 8), 4).readline(26)

    def test_limit_reader_multiple_read(self):
        reader = LimitReader(BytesIO(b'abcdefghjiklmnopqrstuvwxyz'), 10)
        string = None
        for x in [2, 2, 20]:
            string = reader.read(x)

        assert b'efghji' == string

    def test_limit_reader_zero(self):
        assert b'' == LimitReader(BytesIO(b'a'), 0).readline(0)

    def test_limit_reader_invalid_wrap(self):
        b = BytesIO(b'some data')
        assert LimitReader.wrap_stream(b, 'abc') == b

    def test_limit_reader_close(self):
        reader = LimitReader(BytesIO(b'abcdefg'), 3)
        with closing(reader):
            assert b'abc' == reader.read(10)
            assert reader.tell() == 3

    def _test_limit_post(self):
        reader = LimitReader(BytesIO(b'abcdefg'), 3)
        r = requests.request(method='POST',
                             url='http://httpbin.org/post',
                             data=reader,
                             headers={'Content-Length': '3'})

        assert '"abc"' in r.text


