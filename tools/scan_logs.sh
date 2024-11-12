#!/bin/bash

#
# This script must be executed on the Iris server, as it relies on
# the `logcli`.
#
# It scans container logs of $CONTAINER_NAME for a specific $PATTERN
# within from $START_DATE to $END_DATE.
#
# The time period is divided into 30-day intervals to comply with
# Loki's configuration limits (i.e., longer periods are not supported).
# Given the volume of container logs, it can take several minutes
# to complete.
#
# By default, this script scans the logs of the Iris worker from the
# beginning of 2024 to the current date for the pattern "failed" (case
# insensitive).
#

set -eu
shellcheck "$0" # exits if shellcheck doesn't pass

# The following variables can be set via the environment.
: "${CONTAINER_NAME:="iris_worker_1"}"
: "${PATTERN:="failed"}"
: "${START_DATE:="2024-01-01T00:00:00Z"}"
: "${END_DATE:=""}"

readonly QUERY="{container_name=\"$CONTAINER_NAME\"}"

main() {
	scan_logs
}

scan_logs() {
        local start_date="${START_DATE}"
        local end_date="${END_DATE}"
        local tmpfile
        local addr
        local i
        local next_start_date
        local to_date

        if [[ "${START_DATE}" != *:* ]]; then
                start_date="${START_DATE}T00:00:00Z"
        fi
        if [[ "${END_DATE}" == "" ]]; then
                END_DATE="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
        elif [[ "${END_DATE}" != *:* ]]; then
                end_date="${END_DATE}T00:00:00Z"
        fi
        tmpfile="$(mktemp /tmp/worker_logs.$$.XXXX)"
        addr=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' iris_loki_1)
        i=1
        while [[ ! "${start_date}" > "${end_date}" ]]; do
                next_start_date=$(add_days "${start_date}" 30)
                to_date="$(subtract_second "${next_start_date}")"
                (1>&2 date)
                (1>&2 echo "logcli --addr=http://${addr}:3100 query ${QUERY} --from=${start_date} --to=${to_date} --limit 200000000 > ${tmpfile}.${i}")
                logcli --addr="http://${addr}:3100" query "${QUERY}" --from="${start_date}" --to="${to_date}" --limit 200000000 > "${tmpfile}.${i}"
                grep --color -i "${PATTERN}" "${tmpfile}.${i}" || :
		echo
                start_date="${next_start_date}"
        done
        rm -f "${tmpfile}".*
}

add_days() {
	date -u -d "$1 + $2 days" +"%Y-%m-%dT%H:%M:%SZ"
}

subtract_second() {
	date -u -d "$1 - 1 second" +"%Y-%m-%dT%H:%M:%SZ"
}

main "$@"
