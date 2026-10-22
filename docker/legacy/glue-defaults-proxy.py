"""Forwards the Glue Hive client's requests to MiniStack and adds `Retention: 0` to the tables in GetTable and
GetTables answers. MiniStack 1.5.31 drops Retention on CreateTable and UpdateTable whoever sends it (it is missing from
the key list of `_create_table` and from the `safe_keys` of `_update_table` in ministack/services/glue.py), while AWS
Glue returns 0 for tables created without one; the client unboxes it and fails with a NullPointerException on every
table. Listens on 127.0.0.1:4566, which glue.eu-central-1.proxy names in /etc/hosts."""
import json
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM = "http://ministack:4566"


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
                table.setdefault("Retention", 0)
            data = json.dumps(document).encode()
        self.send_response(status)
        self.send_header("Content-Type", response.headers.get("Content-Type", "application/x-amz-json-1.1"))
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", 4566), Proxy).serve_forever()
