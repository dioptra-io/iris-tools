#!/bin/bash

set -euo pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

TOPLEVEL="$(git rev-parse --show-toplevel)"
CONF_DIR="${TOPLEVEL}/conf"
SELECT_MEASUREMENTS="${TOPLEVEL}/tools/select_measurements.sh"

rm -f "${CONF_DIR}/metadata_all" "${CONF_DIR}/data_all"
NOW="2024-01-01T00:00:00"
"${SELECT_MEASUREMENTS}" --zero -uq --now "${NOW}"
for i in {1..1736}; do
	NEW_NOW="$(date -d "${NOW} UTC + $(( i * 6 )) hours" +"%Y-%m-%dT%H:%M:%S")"
	echo && "${SELECT_MEASUREMENTS}" -uq --now "${NEW_NOW}"
done
NOW="2025-04-01T00:00:00"
echo && "${SELECT_MEASUREMENTS}" -uq --now "${NEW_NOW}"
