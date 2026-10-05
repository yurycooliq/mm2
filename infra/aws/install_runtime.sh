#!/bin/sh
# Explicit host mutation, executed by the operator after reviewing apt versions.
set +x
set -eu
umask 077
if [ "$(id -u)" -ne 0 ] || [ "$#" -ne 5 ]; then
    printf '%s\n' 'Usage (Linux root): install_runtime.sh DOCKER_CE_VERSION CLI_VERSION CONTAINERD_VERSION BUILDX_VERSION COMPOSE_VERSION' >&2
    exit 1
fi
. /etc/os-release
if [ "$ID" != ubuntu ] || [ "$VERSION_ID" != 24.04 ] || [ "$(dpkg --print-architecture)" != amd64 ]; then
    printf '%s\n' 'This reviewed bootstrap accepts Ubuntu 24.04 LTS amd64 only.' >&2
    exit 1
fi
for task_version in "$@"; do
    case "$task_version" in ''|*[!A-Za-z0-9.+:~_-]*) printf '%s\n' 'Each Docker component needs an exact reviewed apt version.' >&2; exit 1;; esac
done
if command -v docker >/dev/null 2>&1; then
    printf '%s\n' 'Docker already exists. This initial installer does not perform runtime upgrades.' >&2
    exit 1
fi
apt-get update
apt-get install --no-install-recommends -y ca-certificates curl gnupg chrony
install -d -m 0755 /etc/apt/keyrings
task_key=$(mktemp)
trap 'rm -f "$task_key"' EXIT HUP INT TERM
curl --fail --silent --show-error --proto '=https' --tlsv1.2 --max-time 30 \
    https://download.docker.com/linux/ubuntu/gpg -o "$task_key"
task_fingerprint=$(gpg --batch --show-keys --with-colons "$task_key" | awk -F: '$1 == "fpr" {print $10; exit}')
if [ "$task_fingerprint" != 9DC858229FC7DD38854AE2D88D81803C0EBFCD88 ]; then
    printf '%s\n' 'Docker repository signing-key fingerprint differs from the reviewed identity.' >&2
    exit 1
fi
install -m 0644 "$task_key" /etc/apt/keyrings/docker.asc
cat > /etc/apt/sources.list.d/docker.sources <<'EOF'
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: noble
Components: stable
Architectures: amd64
Signed-By: /etc/apt/keyrings/docker.asc
EOF
apt-get update
apt-get install --no-install-recommends -y "docker-ce=$1" "docker-ce-cli=$2" "containerd.io=$3" \
    "docker-buildx-plugin=$4" "docker-compose-plugin=$5"
apt-mark hold docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
systemctl enable --now docker chrony
install -d -m 0700 /etc/mm2
install -d -m 0755 /opt/mm2/releases
printf '%s\n' 'Pinned Docker components and clock service installed. No mm2 service started.'
