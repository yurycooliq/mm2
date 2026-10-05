#!/bin/sh
set +x
set -eu
task_script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd -P)
exec python3 "$task_script_dir/start.py" "$@"
