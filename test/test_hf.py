import pytest
from warcio.archiveiterator import ArchiveIterator
from warcio.cli import main
from warcio.utils import fsspec_open


try:
    import fsspec  # noqa: F401
    import huggingface_hub  # noqa: F401
    HAS_FSSPEC = True
except ModuleNotFoundError:
    HAS_FSSPEC = False

if not HAS_FSSPEC:
    pytest.skip("fsspec[hf] is not installed", allow_module_level=True)


def _assert_valid_warc(stream):
    first_uri = None
    for record in ArchiveIterator(stream):
        if record.rec_type == 'response':
            first_uri = record.rec_headers.get_header('WARC-Target-URI')
            break
    assert first_uri is not None
    assert first_uri.startswith("http")


def test_hf_from_file():
    # Read WARC records directly from HF Buckets
    with fsspec_open(
        'hf://buckets/commoncrawl/commoncrawl/crawl-data/CC-MAIN-2026-17/'
        'segments/1775805908305.14/warc/CC-MAIN-20260410081153-20260410111153-00000.warc.gz',
        'rb'
    ) as stream:
        _assert_valid_warc(stream)


def test_hf_from_directory():
    # Iterate on WARC files
    files = huggingface_hub.hffs.ls('buckets/commoncrawl/commoncrawl/crawl-data/CC-MAIN-2026-17/segments/1775805908305.14/warc/')
    assert len(files) == 1000
    assert files[0]['name'].endswith(".warc.gz")
    for file in files[:1]:
        with fsspec_open('hf://' + file['name'], 'rb') as stream:
            _assert_valid_warc(stream)


def test_cli_index(capsys):
    files = ['hf://buckets/lhoestq/commoncrawl-samples/example.warc.gz']
    args = ['index', '-f', 'offset,length,http:status,warc-type,filename']
    args.extend(files)

    expected = """\
{"offset": "0", "length": "353", "warc-type": "warcinfo", "filename": "example.warc.gz"}
{"offset": "353", "length": "431", "warc-type": "warcinfo", "filename": "example.warc.gz"}
{"offset": "784", "length": "1228", "http:status": "200", "warc-type": "response", "filename": "example.warc.gz"}
{"offset": "2012", "length": "609", "warc-type": "request", "filename": "example.warc.gz"}
{"offset": "2621", "length": "586", "http:status": "200", "warc-type": "revisit", "filename": "example.warc.gz"}
{"offset": "3207", "length": "609", "warc-type": "request", "filename": "example.warc.gz"}
"""
    res = main(args=args)
    assert capsys.readouterr().out == expected
