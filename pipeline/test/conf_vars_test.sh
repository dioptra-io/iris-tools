#!/bin/bash

set -euo pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

TOPLEVEL="$(git rev-parse --show-toplevel)"
SETTINGS_CONF="conf/publish_settings.conf"

grep '^readonly' "${TOPLEVEL}/${SETTINGS_CONF}" |
sed -e '/PROBE_SRC_PORT_LIMIT/d' -e 's/readonly.//' -e 's/=.*//' |
while read -r var; do
	echo "${var}"
	(cd "${TOPLEVEL}" && git grep -lw "${var}" | grep -v "${SETTINGS_CONF}" | sed -e 's/^/    /')
done
