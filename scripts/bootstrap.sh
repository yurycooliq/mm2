#!/bin/sh
set +x
set -eu
umask 077

if [ "$#" -gt 1 ]; then
    printf '%s\n' 'Usage: sh scripts/bootstrap.sh [env-path]' >&2
    exit 1
fi
task_script_dir=$(CDPATH= cd "$(dirname "$0")/.." && pwd -P)
task_env_path=${1:-"$task_script_dir/.env"}
if [ -e "$task_env_path" ] || [ -L "$task_env_path" ]; then
    printf '%s\n' 'Existing environment file left unchanged.'
    exit 0
fi

task_token=$(od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]')
task_console_token=$(od -An -N32 -tx1 /dev/urandom | tr -d '[:space:]')
case "$task_token$task_console_token" in
    ''|*[!0-9a-f]*)
        printf '%s\n' 'Could not generate a random API token.' >&2
        exit 1
        ;;
esac
if [ "${#task_token}" -ne 64 ] || [ "${#task_console_token}" -ne 64 ]; then
    printf '%s\n' 'Could not generate a random API token.' >&2
    exit 1
fi

# Noclobber creates the final file exclusively, including when another bootstrap races.
if (
    set -C
    printf '%s\n' \
        "MM2_ENGINE_API_TOKEN=$task_token" \
        "MM2_CONSOLE_TOKEN=$task_console_token" \
        'MM2_TELEGRAM_BOT_TOKEN=' \
        'MM2_TELEGRAM_OWNER_ID=' \
        'MM2_MINIAPP_URL=' \
        'MM2_ENGINE_CONFIG=./config/demo.toml' \
        'MM2_ALLOWED_ORIGINS=http://127.0.0.1:8080,http://localhost:8080' > "$task_env_path"
) 2>/dev/null; then
    unset task_token task_console_token
    printf '%s\n' 'Created private environment file. No token value was printed.'
else
    unset task_token task_console_token
    printf '%s\n' 'Could not create environment file exclusively. Existing paths were left unchanged.' >&2
    exit 1
fi
