#!/bin/bash

#
# This script must be executed on the Iris server, as it relies on
# the `logcli`.
#
# It fetches the container logs of $CONTAINER_NAME from $START_DATE to
# $END_DATE and, if $PATTERN is provided, it searches the logs for it.
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
: "${START_DATE:="2024-01-01T00:00:00Z"}"
: "${END_DATE:=""}"
: "${PATTERN:=""}"

readonly QUERY="{container_name=\"$CONTAINER_NAME\"}"

main() {
	scan_logs
}

scan_logs() {
	local start_date
	local end_date
	local tmpfile
	local addr
	local i
	local next_start_date
	local to_date

	if [[ "${START_DATE}" != *:* ]]; then
		start_date="${START_DATE}T00:00:00Z"
	else
		start_date="${START_DATE}"
	fi
	if [[ "${END_DATE}" == "" ]]; then
		END_DATE="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
	fi
	if [[ "${END_DATE}" != *:* ]]; then
		end_date="${END_DATE}T00:00:00Z"
	else
		end_date="${END_DATE}"
	fi
	tmpfile="$(mktemp /tmp/${CONTAINER_NAME}_logs.$$.XXXX)"
	addr=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' iris_loki_1)
	i=1
	while [[ ! "${start_date}" > "${end_date}" ]]; do
		next_start_date=$(add_days "${start_date}" 30)
		to_date="$(subtract_second "${next_start_date}")"
		if [[ "${to_date}" > "${end_date}" ]]; then
			to_date="${end_date}"
		fi
		(1>&2 date)
		(1>&2 echo "logcli --quiet --addr=http://${addr}:3100 query ${QUERY} --from=${start_date} --to=${to_date} --limit 200000000 > ${tmpfile}.${i}-latest-first")
		logcli --quiet --addr="http://${addr}:3100" query "${QUERY}" --from="${start_date}" --to="${to_date}" --limit 200000000 > "${tmpfile}.${i}-latest-first"
		tac "${tmpfile}.${i}-latest-first" > "${tmpfile}.${i}"
		rm "${tmpfile}.${i}-latest-first"
		if [[ "${PATTERN}" != "" ]]; then
			grep --color -i ${PATTERN} "${tmpfile}.${i}" || :
		fi
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
