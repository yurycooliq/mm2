#!/bin/sh
# Mount only the operator-selected EBS identity. Formatting is an explicit new-volume action.
set +x
set -eu
umask 077
if [ "$(id -u)" -ne 0 ] || [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    printf '%s\n' 'Usage (Linux root): mount_state.sh vol-ID [--initialize-new-volume]' >&2
    exit 1
fi
task_volume=$1
case "$task_volume" in vol-*) ;; *) printf '%s\n' 'Expected an exact EBS volume ID.' >&2; exit 1;; esac
task_suffix=${task_volume#vol-}
case "$task_suffix" in ''|*[!0-9a-f]*) printf '%s\n' 'Invalid EBS volume identity.' >&2; exit 1;; esac
if [ "${#task_suffix}" -lt 8 ] || [ "${#task_suffix}" -gt 32 ]; then exit 1; fi
task_initialize=${2:-}
case "$task_initialize" in ''|--initialize-new-volume) ;; *) printf '%s\n' 'Unknown initialization option.' >&2; exit 1;; esac
task_device=
for task_path in /sys/block/nvme*n1; do
    [ -f "$task_path/device/serial" ] || continue
    task_serial=$(tr -d '[:space:]' < "$task_path/device/serial")
    if [ "$task_serial" = "vol$task_suffix" ]; then
        if [ -n "$task_device" ]; then printf '%s\n' 'Ambiguous volume identity.' >&2; exit 1; fi
        task_device="/dev/${task_path##*/}"
    fi
done
if [ -z "$task_device" ] || [ ! -b "$task_device" ]; then
    printf '%s\n' 'The selected EBS NVMe volume is not attached. Device order is never inferred.' >&2
    exit 1
fi
task_root_source=$(findmnt --noheadings --output SOURCE /)
task_root_parent=$(lsblk --noheadings --output PKNAME "$task_root_source" | head -n 1)
if [ "$task_device" = "$task_root_source" ] || [ "${task_device#/dev/}" = "$task_root_parent" ]; then
    printf '%s\n' 'Refusing the host root volume.' >&2; exit 1
fi
if [ -n "$(lsblk --noheadings --output MOUNTPOINTS "$task_device" | tr -d '[:space:]')" ]; then
    printf '%s\n' 'Selected volume is already mounted. Inspect its identity and existing setup.' >&2; exit 1
fi
if [ "$(lsblk --noheadings --output NAME "$task_device" | wc -l)" -ne 1 ]; then
    printf '%s\n' 'Partitioned devices require separate reviewed recovery; no automatic formatting.' >&2; exit 1
fi
task_fs=$(blkid -o value -s TYPE "$task_device" || true)
if [ -z "$task_fs" ]; then
    if [ "$task_initialize" != --initialize-new-volume ] || [ -n "$(wipefs --no-act --noheadings --output TYPE "$task_device")" ]; then
        printf '%s\n' 'No recognized filesystem. Initialize only a confirmed newly created empty EBS volume explicitly.' >&2; exit 1
    fi
    mkfs.ext4 -L mm2-state "$task_device"
elif [ "$task_fs" != ext4 ] || [ -n "$task_initialize" ]; then
    printf '%s\n' 'Only an existing ext4 state filesystem may be mounted; existing data is never reformatted.' >&2; exit 1
fi
task_uuid=$(blkid -o value -s UUID "$task_device")
if [ -z "$task_uuid" ] || [ -L /srv/mm2 ]; then printf '%s\n' 'Missing filesystem identity or indirect mount target.' >&2; exit 1; fi
install -d -m 0700 /srv/mm2
if [ -n "$(find /srv/mm2 -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    printf '%s\n' 'Mount directory is not empty; existing files must be preserved.' >&2; exit 1
fi
if awk '$2 == "/srv/mm2" {found=1} END {exit !found}' /etc/fstab; then
    if ! awk -v expected="UUID=$task_uuid" '$2 == "/srv/mm2" && $1 == expected {found=1} END {exit !found}' /etc/fstab; then
        printf '%s\n' 'Existing fstab target identifies another filesystem.' >&2; exit 1
    fi
else
    printf 'UUID=%s /srv/mm2 ext4 defaults,nodev,nosuid,noexec 0 2\n' "$task_uuid" >> /etc/fstab
fi
mount /srv/mm2
if [ -e /srv/mm2/.mm2-state-volume ]; then
    if [ -L /srv/mm2/.mm2-state-volume ] || [ "$(cat /srv/mm2/.mm2-state-volume)" != "$task_volume" ]; then
        printf '%s\n' 'Mounted state identity differs from the selected EBS volume.' >&2; exit 1
    fi
else
    printf '%s\n' "$task_volume" > /srv/mm2/.mm2-state-volume
fi
for task_pair in engine:10001 console:10002 caddy:0; do
    task_lane=${task_pair%:*}
    task_uid=${task_pair#*:}
    task_directory="/srv/mm2/$task_lane"
    if [ -e "$task_directory" ]; then
        if [ -L "$task_directory" ] || [ ! -d "$task_directory" ] || [ "$(stat -c %u "$task_directory")" != "$task_uid" ]; then
            printf '%s\n' 'Existing state lane has unexpected ownership/type; recovery needs explicit review.' >&2; exit 1
        fi
    else
        install -d -m 0700 -o "$task_uid" -g "$task_uid" "$task_directory"
    fi
done
printf '%s\n' 'Verified EBS state filesystem mounted. No bot service started.'
