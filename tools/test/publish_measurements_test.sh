#!/bin/bash

set -euo pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

readonly TOPLEVEL="$(git rev-parse --show-toplevel)"
readonly PUBLISH_MEASUREMENTS="${TOPLEVEL}/tools/publish_measurements.sh"
readonly VERBOSE="4"

rm -f "${TOPLEVEL}/conf/metadata_all" "${TOPLEVEL}/conf/data_all"

NOW="2025-03-22T00:00:00"
"${PUBLISH_MEASUREMENTS}" --use-cache --now "${NOW}" --verbose "${VERBOSE}" --zero # start from scratch
i=0
while :; do
	echo "-----------------------------------------------------------------------"
	NEW_NOW="$(date -d "${NOW} UTC + $(( i * 6 )) hours" +"%Y-%m-%dT%H:%M:%S")"
	"${PUBLISH_MEASUREMENTS}" --use-cache --now "${NEW_NOW}" --verbose "${VERBOSE}"
	CURRENT_TIME_SECONDS=$(date +%s)
	NEW_NOW_SECONDS=$(date -d "$NEW_NOW" +%s)
	if [[ $(( NEW_NOW_SECONDS - CURRENT_TIME_SECONDS )) -gt 86400 ]]; then
		break
	fi
	_=$(( i++ ))
done
