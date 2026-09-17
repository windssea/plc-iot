#!/bin/sh
# Stop before switching code: SQLite files and external configuration are retained.
set -eu
[ "$#" -eq 1 ] || { echo 'Usage: activate.sh RELEASE_ID' >&2; exit 2; }
release=$1
case "$release" in ''|*[!A-Za-z0-9._-]*|.*|-*) exit 2;; esac
[ "${#release}" -le 64 ] || exit 2
[ "$(id -u)" -eq 0 ] || exit 2
target=/opt/plcnext-iot/releases/$release
[ -x "$target/.venv/bin/python" ] && [ -f "$target/manifest.json" ] || exit 2
[ -d /run/systemd/system ] || { echo 'systemd is not running' >&2; exit 2; }
# Preflight and site-specific compatibility review must precede this command.
link=/opt/plcnext-iot/current.new
[ ! -e "$link" ] && [ ! -L "$link" ] || { echo 'Pending switch exists' >&2; exit 2; }
if [ "$(systemctl show -p LoadState --value plcnext-iot.service)" != not-found ]; then
    systemctl stop plcnext-iot.service
fi
ln -s "$target" "$link"
mv -Tf "$link" /opt/plcnext-iot/current
install -m 644 "$target/deploy/linux/plcnext-iot.service" /etc/systemd/system/plcnext-iot.service
systemctl daemon-reload
systemctl reset-failed plcnext-iot.service
systemctl enable plcnext-iot.service
systemctl start plcnext-iot.service
systemctl is-active --quiet plcnext-iot.service
echo 'Service process started. Verify MQTT heartbeat and child-device readings before acceptance.'
