"""Read-only OCI distribution registry serving one OCI image layout (stdlib only).

Exists so AgentENV (regctl) can pull a locally built image without any public
registry or the registry:2 container image. Loopback only.

Implements the pull subset of the distribution spec:
  GET  /v2/                                   -> 200 {}
  HEAD|GET /v2/<name>/manifests/<tag|digest>  -> manifest/index blob
  HEAD|GET /v2/<name>/blobs/<digest>          -> blob (supports Range)
  GET  /v2/<name>/tags/list                   -> tags from index.json
<name> is ignored: the whole layout is served under any repository name.

usage: oci_layout_registry.py LAYOUT_DIR [--host 127.0.0.1] [--port 5000]
"""

import argparse
import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MANIFEST_RE = re.compile(r"^/v2/(?P<name>.+)/manifests/(?P<ref>[^/]+)$")
BLOB_RE = re.compile(r"^/v2/(?P<name>.+)/blobs/(?P<digest>sha256:[0-9a-f]{64})$")
TAGS_RE = re.compile(r"^/v2/(?P<name>.+)/tags/list$")


def make_handler(layout: str):
    def blob_path(digest: str) -> str:
        algo, hexd = digest.split(":", 1)
        return os.path.join(layout, "blobs", algo, hexd)

    def load_index() -> dict:
        with open(os.path.join(layout, "index.json")) as f:
            return json.load(f)

    def resolve(ref: str):
        """Return (digest, media_type) for a tag or digest reference."""
        for m in load_index()["manifests"]:
            tag = m.get("annotations", {}).get("org.opencontainers.image.ref.name")
            if ref == tag or ref == m["digest"]:
                return m["digest"], m["mediaType"]
        if ref.startswith("sha256:") and os.path.exists(blob_path(ref)):
            with open(blob_path(ref), "rb") as f:
                mt = json.loads(f.read()).get("mediaType", "application/vnd.oci.image.manifest.v1+json")
            return ref, mt
        return None, None

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):  # one line per request
            print(f"{self.address_string()} {fmt % args}", flush=True)

        def _send(self, code, body=b"", headers=None, head=False):
            self.send_response(code)
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Docker-Distribution-API-Version", "registry/2.0")
            self.end_headers()
            if not head and body:
                self.wfile.write(body)

        def _serve_file(self, path, media_type, digest, head):
            size = os.path.getsize(path)
            start, end = 0, size - 1
            rng = self.headers.get("Range")
            code = 200
            if rng and rng.startswith("bytes="):
                a, _, b = rng[6:].partition("-")
                start = int(a) if a else 0
                end = int(b) if b else size - 1
                code = 206
            self.send_response(code)
            self.send_header("Content-Type", media_type)
            self.send_header("Docker-Content-Digest", digest)
            self.send_header("Docker-Distribution-API-Version", "registry/2.0")
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("Accept-Ranges", "bytes")
            if code == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.end_headers()
            if head:
                return
            with open(path, "rb") as f:
                f.seek(start)
                left = end - start + 1
                while left > 0:
                    chunk = f.read(min(8 << 20, left))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    left -= len(chunk)

        def _route(self, head: bool):
            path = self.path.split("?", 1)[0]
            if path in ("/v2", "/v2/"):
                return self._send(200, b"{}", {"Content-Type": "application/json"}, head)
            m = TAGS_RE.match(path)
            if m:
                tags = [x.get("annotations", {}).get("org.opencontainers.image.ref.name")
                        for x in load_index()["manifests"]]
                body = json.dumps({"name": m["name"], "tags": [t for t in tags if t]}).encode()
                return self._send(200, body, {"Content-Type": "application/json"}, head)
            m = MANIFEST_RE.match(path)
            if m:
                digest, mt = resolve(m["ref"])
                if not digest:
                    return self._send(404, b'{"errors":[{"code":"MANIFEST_UNKNOWN"}]}',
                                      {"Content-Type": "application/json"}, head)
                return self._serve_file(blob_path(digest), mt, digest, head)
            m = BLOB_RE.match(path)
            if m and os.path.exists(blob_path(m["digest"])):
                return self._serve_file(blob_path(m["digest"]), "application/octet-stream", m["digest"], head)
            return self._send(404, b'{"errors":[{"code":"NOT_FOUND"}]}', {"Content-Type": "application/json"}, head)

        def do_GET(self):
            self._route(head=False)

        def do_HEAD(self):
            self._route(head=True)

    return Handler


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("layout")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=5000)
    a = ap.parse_args()
    srv = ThreadingHTTPServer((a.host, a.port), make_handler(os.path.abspath(a.layout)))
    print(f"serving {a.layout} on http://{a.host}:{a.port}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
