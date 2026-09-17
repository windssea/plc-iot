#!/bin/sh
# Stage a release. Does not switch current, start services or overwrite config.
set -eu
if [ "$#" -ne 2 ]; then
    echo 'Usage: sh deploy/linux/install.sh RELEASE_ID /absolute/path/to/python3.12' >&2
    exit 2
fi
release=$1
python=$2
case "$release" in ''|*[!A-Za-z0-9._-]*|.*|-*) echo 'Invalid release ID' >&2; exit 2;; esac
[ "${#release}" -le 64 ] || exit 2
case "$python" in /*) ;; *) echo 'Python path must be absolute' >&2; exit 2;; esac
[ "$(id -u)" -eq 0 ] || { echo 'Installation requires root' >&2; exit 2; }
id plcnext-iot >/dev/null 2>&1 || { echo 'Create the plcnext-iot service account first' >&2; exit 2; }
source_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
target=/opt/plcnext-iot/releases/$release
[ ! -e "$target" ] || { echo 'Release already exists; use a new release ID' >&2; exit 2; }
"$python" -c 'import sys, venv; assert sys.version_info[:2] == (3,12), "Python 3.12 required"'
"$python" -c 'import json,sys; assert json.load(open(sys.argv[1]))["version"] == sys.argv[2], "Release ID mismatch"' "$source_dir/manifest.json" "$release"
install -d -m 755 /opt/plcnext-iot/releases
mkdir "$target"
cp -R "$source_dir/." "$target/"
"$python" -m venv "$target/.venv"
# PIP_NO_INDEX/PIP_FIND_LINKS can select an architecture-matched offline wheelhouse.
"$target/.venv/bin/python" -m pip install -r "$target/requirements-runtime.lock"
"$target/.venv/bin/python" -m pip check
chown -R root:root "$target"
chmod -R go-w "$target"
install -d -m 750 -o root -g plcnext-iot /etc/plcnext-iot
install -d -m 700 -o plcnext-iot -g plcnext-iot /var/lib/plcnext-iot
for file in bootstrap.json mqtt.json; do
    if [ ! -e "/etc/plcnext-iot/$file" ]; then
        install -m 640 -o root -g plcnext-iot "$target/deploy/linux/$file" "/etc/plcnext-iot/$file"
    fi
done
echo "Staged $target. Configure /etc/plcnext-iot, run preflight as the service user, then activate."
