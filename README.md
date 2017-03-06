# WARCIO: WARC (and ARC) Streaming Library

### Background

This library provides a fast way to read and write [WARC Format](https://en.wikipedia.org/wiki/Web_ARChive) commonly used in web archives.
This library is a spin-off from the [pywb](https://github.com/ikreymer/pywb) high-fidelity replay library, a key component of [Webrecorder](https://github.com/webrecorder/webrecorder)

The library is designed for fast, low-level access to web archival content, oriented around a stream of WARC records rather than files.

## Reading WARC Records

A key feature of the library is to be able to iteratoe over a stream of WARC records using the `ArchiveIterator`
For example, the following prints the the url for each WARC `response` record:

```python
from warcio.archiveiterator import ArchiveIterator

with open('path/to/file', 'rb') as stream:
    for record in ArchiveIterator(stream):
        if record.rec_type == 'response':
            print(record.rec_headers.get_header('WARC-Target-URI'))
```

The stream object could be a file on disk or a remote network stream. The `ArchiveIterator` reads the WARC content in a single pass.
The `record` is represented by an `ArcWarcRecord` object which contains the format (ARC or WARC), record type, the record headers, http headers (if any), and raw stream for reading the payload.

```
class ArcWarcRecord(object):
    def __init__(self, *args):
        (self.format, self.rec_type, self.rec_headers, self.raw_stream,
         self.http_headers, self.content_type, self.length) = args
```

### Reading WARC Content

The `raw_stream` can be used to read the rest of the payload directly.
A special `content_stream()` function provides a stream that automatically decompress and de-chunks the HTTP payload, if it is compressed and/or transfer-encoding chunked.


### ARC Files

The library provides support for reading (but not writing ARC) files. The ARC format is legacy but is important to support in a consistent matter. The `ArchiveIterator` can equally iterate over ARC files and produce ArcWarcRecord objects. The special `arc2warc` option further converts ARC records to WARCs on the fly, allowing for them to be accessed using the same API.

For example, here is a snippet for reading an ARC and a WARC using the same API.

The snippet streams the file over HTTP using [requests](http://docs.python-requests.org/en/master/), printing the `warcinfo` record (or ARC header) and any response records (all ARC records) that contain HTML:

```python
import requests
from warcio.archiveiterator import ArchiveIterator

def print_records(url):
    resp = requests.get(url, stream=True)

    for record in ArchiveIterator(resp.raw, arc2warc=True):
        if record.rec_type == 'warcinfo':
            print(record.raw_stream.read())

        elif record.rec_type == 'response':
            if record.http_headers.get_header('Content-Type') == 'text/html':
                print(record.rec_headers.get_header('WARC-Target-URI'))
                print(record.content_stream().read())
                print('')

# WARC
print_records('https://archive.org/download/ExampleArcAndWarcFiles/IAH-20080430204825-00000-blackbook.warc.gz')


# ARC with arc2warc
print_records('https://archive.org/download/ExampleArcAndWarcFiles/IAH-20080430204825-00000-blackbook.arc.gz')
```


