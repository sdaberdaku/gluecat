"""Forwards the Glue Hive client's requests to MiniStack and adds the table fields AWS Glue always returns
(Retention 0, StorageDescriptor Compressed false, NumberOfBuckets 0, StoredAsSubDirectories false) where MiniStack
leaves them out: MiniStack 1.5.31 never stores Retention, and the client unboxes all four, failing with a
NullPointerException on every table. Listens on 127.0.0.1:4566, which glue.eu-central-1.proxy names in /etc/hosts."""
import json
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = "http://ministack:4566"
STORAGE_DEFAULTS = {"Compressed": False, "NumberOfBuckets": 0, "StoredAsSubDirectories": False}


def add_defaults(table):
    table.setdefault("Retention", 0)
    storage = table.setdefault("StorageDescriptor", {})
    for key, value in STORAGE_DEFAULTS.items():
        storage.setdefault(key, value)


class Proxy(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # answers the client's Expect: 100-continue at once

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        headers = {k: v for k, v in self.headers.items() if k.lower() not in ("host", "content-length", "expect")}
        request = urllib.request.Request(UPSTREAM + self.path, body, headers, method="POST")
        try:
            response = urllib.request.urlopen(request)
        except urllib.error.HTTPError as error:
            response = error
        status, data = response.getcode(), response.read()
        if status == 200 and self.headers.get("X-Amz-Target") in ("AWSGlue.GetTable", "AWSGlue.GetTables"):
            document = json.loads(data)
            for table in ([document["Table"]] if "Table" in document else []) + document.get("TableList", []):
                add_defaults(table)
            data = json.dumps(document).encode()
        self.send_response(status)
        self.send_header("Content-Type", response.headers.get("Content-Type", "application/x-amz-json-1.1"))
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", 4566), Proxy).serve_forever()
