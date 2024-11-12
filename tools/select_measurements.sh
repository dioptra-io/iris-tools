#!/bin/bash

set -eu
set -o pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

readonly PROG_NAME="${0##*/}"
CONFIG_FILE="$(git rev-parse --show-toplevel)/conf/tables.conf" # --config
FORCE=false # --force


usage() {
        local exit_code="$1"

        cat <<EOF
usage:
        ${PROG_NAME} [-hf] [-c <config>] <uuid>...
        -h, --help      print help message and exit
        -f, --force     recreate \$MEAS_MD_ALL_JSON and \$MEAS_MD_SELECTED_TXT even if they exist
        -c, --config    configuration file (default ${CONFIG_FILE})

        uuid: measurement uuid
EOF
        exit "${exit_code}"
}

main() {
	local output

	parse_args "$@"
        echo "sourcing ${CONFIG_FILE}"
        # shellcheck disable=SC1090
        source "${CONFIG_FILE}"

	if [[ -z "${IRIS_PASSWORD+x}" ]]; then
		irisctl auth login
	else
		echo "irisctl will use IRIS_PASSWORD environment variable when invoked"
	fi

	# First, see if the measurement metadata file that includes all
	# measurements needs to be created.  This file will be passed to
	# `irisctl` in subsequent calls so speed up its execution.
	if ! ${FORCE} && [[ -f "${MEAS_MD_ALL_JSON}" ]]; then
		echo using existing "${MEAS_MD_ALL_JSON}"
	else
		echo creating "${MEAS_MD_ALL_JSON}"
		if ! output="$(irisctl meas --all-users 2>&1 > /dev/null)"; then
			echo "${output}"
			exit 1
		fi
		mv "${output/saving in /}" "${MEAS_MD_ALL_JSON}"
	fi
	echo "$(jq .count "${MEAS_MD_ALL_JSON}" | uniq) total measurements in ${MEAS_MD_ALL_JSON}"

	# Next, select the measurements that we are interested in.  Look at
	# $CONFIG_FILE for the selection criteria.
	if ! ${FORCE} && [[ -f "${MEAS_MD_SELECTED_TXT}" ]]; then
		echo using existing "${MEAS_MD_SELECTED_TXT}"
	else
		echo creating "${MEAS_MD_SELECTED_TXT}"
		echo "filtering by measurement attributes"
		tmpfile1=$(filter_by_attribute)
		echo "$(wc -l < "${tmpfile1}") measurements in ${tmpfile1}"

		echo "filtering by agent failures"
		tmpfile2=$(filter_by_agent_failures "${tmpfile1}")
		echo "$(wc -l < "${tmpfile2}") measurements in ${tmpfile2}"

		echo "filtering by worker failures"
		tmpfile3=$(find_worker_failures)
		tmpfile4=$(filter_by_worker_failures "${tmpfile2}" "${tmpfile3}")
		mv "${tmpfile4}" "${MEAS_MD_SELECTED_TXT}"
		#rm -f "${tmpfile1}" "${tmpfile2}" "${tmpfile3}" XXX
	fi
	echo "$(wc -l < "${MEAS_MD_SELECTED_TXT}") selected measurements in ${MEAS_MD_SELECTED_TXT}"
}

filter_by_attribute() {
	local output

	output="$(mktemp /tmp/filter_by_attribute.XXXX)"
	irisctl list -t "${MEAS_TAG}" -s "${MEAS_STATE}" --after "${MEAS_AFTER}" --before "${MEAS_BEFORE}" "${MEAS_MD_ALL_JSON}" > "${output}"
	echo "${output}"
}

# Filter out measurements from $meas_uuids that did not have $NUM_AGENTS
# or $NUM_AGENTS_FINISHED agents that finished.
filter_by_agent_failures() {
	local meas_uuids="$1"
	local uuid
	local tmpfile
	local output

	tmpfile="$(mktemp /tmp/filter_by_agent_failures1.XXXX)"
	output="$(mktemp /tmp/filter_by_agent_failures2.XXXX)"
	while read -r uuid; do
		irisctl meas --uuid "${uuid}" -o | jq .agents[].state > "${tmpfile}"
		if [[ $(wc -l < "${tmpfile}") -ne ${NUM_AGENTS} ]]; then
			continue
		fi
		if [[ $(grep -c finished "${tmpfile}") -ne ${NUM_AGENTS_FINISHED} ]]; then
			continue
		fi
		echo "${uuid}" >> "${output}"
	done < <(awk '/^...*$/ { print $1 }' "${meas_uuids}")
	#rm -f "${tmpfile}" XXX
	echo "${output}"
}

# Scan Iris worker container logs from $MEAS_AFTER to $MEAS_BEFORE to
# find worker failures.
find_worker_failures() {
	local start_date="${MEAS_AFTER}"
	local end_date="${MEAS_BEFORE}"
	local tmpfile
	local output
	local addr
	local i
	local next_start_date
	local to_date

	if [[ "${MEAS_AFTER}" != *:* ]]; then
		start_date="${MEAS_AFTER}T00:00:00Z"
	fi
	if [[ "${MEAS_BEFORE}" != *:* ]]; then
		end_date="${MEAS_BEFORE}T00:00:00Z"
	fi
	tmpfile="$(mktemp /tmp/worker_logs.XXXX)"
	output="$(mktemp /tmp/worker_logs_failed.XXXX)"
	addr=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' iris_loki_1)
	i=1
	while [[ ! "${start_date}" > "${end_date}" ]]; do
		next_start_date=$(add_days "${start_date}" 30)
		to_date="$(subtract_second "${next_start_date}")"
		(1>&2 date)
		(1>&2 echo logcli --addr="http://${addr}:3100" query "{container_name=\"iris_worker_1\"}" --from="${start_date}" --to="${to_date}" --limit 200000000)
		logcli --addr="http://${addr}:3100" query "{container_name=\"iris_worker_1\"}" --from="${start_date}" --to="${to_date}" --limit 200000000 > "${tmpfile}.${i}" 2> /dev/null
		grep -i 'failed .* watch_measurement_agent' "${tmpfile}.${i}" | sed -e 's/.*watch_measurement_agent(.//' -e 's/.,.*//' | sort | uniq >> "${output}" || :
		start_date="${next_start_date}"
	done
	#rm -f "${tmpfile}".* XXX
	echo "${output}"
}

add_days() {
	date -u -d "$1 + $2 days" +"%Y-%m-%dT%H:%M:%SZ"
}

subtract_second() {
	date -u -d "$1 - 1 second" +"%Y-%m-%dT%H:%M:%SZ"
}

# Filter out from $meas_uuids the measurements that are in $worker_failed_uuids.
filter_by_worker_failures() {
	local meas_uuids="$1"
	local worker_failed_uuids="$2"
	local output

	output="$(mktemp /tmp/filter_by_worker_failures.XXXX)"
        while read -r uuid; do
                if ! grep -q "${uuid}" "${worker_failed_uuids}"; then
			echo "${uuid}" >> "${output}"
		fi
        done < "${meas_uuids}"
	echo "${output}"
}

parse_args() {
	local args
	local arg

	if ! args="$(getopt \
			--options "c:fh" \
			--longoptions "config: force help" \
			-- "$@")"; then
		usage 1
	fi
	eval set -- "${args}"
	
	if [[ "$#" -eq 0 ]]; then
		echo "${PROG_NAME}: specify one or more commands"
		usage 1
	fi

	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-c|--config) CONFIG_FILE="$1"; shift 1;;
		-f|--force) FORCE=true;;
		-h|--help) usage 0;;
		--) break;;
		*) echo "internal error parsing arg=${arg}"; usage 1;;
		esac
	done

	if [[ "$#" -ne 0 ]]; then
		echo "${PROG_NAME}: extra command line arguments"
		usage 1
	fi
}

main "$@"
