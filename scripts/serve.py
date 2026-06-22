#!/usr/bin/env python3
import argparse
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]


class SlidesHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT_DIR), **kwargs)

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self.path = "/slides/index.html"
        return super().do_GET()


def main():
    parser = argparse.ArgumentParser(description="Serve slides/index.html from repo root.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    server = ThreadingHTTPServer((args.host, args.port), SlidesHandler)
    print(
        f"Serving slides at http://localhost:{args.port}/ "
        f"(root -> /slides/index.html)"
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
