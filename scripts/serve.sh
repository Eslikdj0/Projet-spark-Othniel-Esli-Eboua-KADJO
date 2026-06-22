#!/usr/bin/env bash
set -euo pipefail

port="${1:-8001}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

python_bin="python3"
if ! command -v "$python_bin" >/dev/null 2>&1; then
  python_bin="python"
fi

if ! command -v "$python_bin" >/dev/null 2>&1; then
  echo "Python is required to run the slide server." >&2
  exit 1
fi

"$python_bin" "$script_dir/serve.py" --port "$port"
