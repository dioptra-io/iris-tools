#!/bin/bash

set -euo pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

readonly TOPLEVEL="$(git rev-parse --show-toplevel)"
readonly PUBLISH_MEASUREMENTS="${TOPLEVEL}/tools/publish_measurements.sh"
readonly VERBOSE="4"

rm -f "${TOPLEVEL}/conf/metadata_all" "${TOPLEVEL}/conf/data_all"

NOW="2024-01-01T00:00:00"
"${PUBLISH_MEASUREMENTS}" --use-cache --now "${NOW}" --verbose "${VERBOSE}" --zero # start from scratch
for i in {0..1736}; do
	echo "-----------------------------------------------------------------------"
	NEW_NOW="$(date -d "${NOW} UTC + $(( i * 6 )) hours" +"%Y-%m-%dT%H:%M:%S")"
	"${PUBLISH_MEASUREMENTS}" --use-cache --now "${NEW_NOW}" --verbose "${VERBOSE}"
done
NOW="2025-04-01T00:00:00"
echo && "${PUBLISH_MEASUREMENTS}" --use-cache --now "${NEW_NOW}" --verbose "${VERBOSE}"
