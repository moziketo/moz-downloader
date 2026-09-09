#!/usr/bin/env bash
# Local test server — CORS allows http://127.0.0.1:8765 on moz-downloader
cd "$(dirname "$0")"
echo "Open: http://127.0.0.1:8765/play.html"
python3 -m http.server 8765 --bind 127.0.0.1
