from . import get_test_file
from warcio.recompressor import Recompressor

import pytest

#capsys,
def test_recompress_non_chunked(tmp_path):
    test_file = get_test_file('example-resource.warc.gz')
    tmp_file = tmp_path / "output.warc.gz"
    print(test_file)
    print(tmp_file)
    recompressor = Recompressor(test_file, str(tmp_file), verbose=True)
    # with pytest.raises(SystemExit):
    recompressor.recompress()
    # out, err = capsys.readouterr()
    # print("out")
    # print(out)
    # print("err")
    # print(err)
    # assert len(out) > 0
    # with open(tmp_path) as temp:
