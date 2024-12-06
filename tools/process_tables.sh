#!/bin/bash

set -eu
set -o pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

readonly PROG_NAME="${0##*/}"
CONFIG_FILE="$(git rev-parse --show-toplevel)/conf/tables.conf" # --config
FORCE=false # --force
DO_SELECT=false # select
DO_EXPORT=false # export
DO_UPLOAD=false # upload


usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} [-hf] [-c <config>] <command>...
	-h, --help      print help message and exit
	-f, --force     recreate and/or redo operations even if already done
	-c, --config    configuration file (default ${CONFIG_FILE})

	command: select, export, upload, publish
EOF
	exit "${exit_code}"
}

main() {
	local flags=()

	parse_args "$@"
	# shellcheck disable=SC1090
	source "${CONFIG_FILE}"
	echo "CONFIG_FILE=${CONFIG_FILE}"
	echo "MEAS_MD_ALL_JSON=${MEAS_MD_ALL_JSON}"
	echo "MEAS_MD_SELECTED_TXT=${MEAS_MD_SELECTED_TXT}"

	flags=(-c "${CONFIG_FILE}")
	if ${FORCE}; then
		flags+=(-f)
	fi
	if ${DO_SELECT}; then
		"${SELECT_MEASUREMENTS}" "${flags[@]}"
	fi
	while read -r uuid; do
		if ${DO_EXPORT}; then
			"${EXPORT_TABLES}" "${flags[@]}" "${uuid}"
		fi
		if ${DO_UPLOAD}; then
			"${UPLOAD_TABLES}" "${flags[@]}" "${uuid}"
		fi
		echo
	done < "${MEAS_MD_SELECTED_TXT}"
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
	
	if [[ "$#" -eq 1 ]]; then
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

	for arg in "$@"; do
		case "${arg}" in
		select) DO_SELECT=true;;
		export) DO_EXPORT=true;;
		upload) DO_UPLOAD=true;;
		publish) DO_SELECT=true; DO_EXPORT=true; DO_UPLOAD=true;;
		*) echo "${arg}: invalid argument"; usage 1;;
		esac
	done
}

main "$@"
