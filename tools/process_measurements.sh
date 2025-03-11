#!/bin/bash

set -eu
set -o pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

readonly PROG_NAME="${0##*/}"
CONFIG_FILE="$(git rev-parse --show-toplevel)/conf/settings.conf" # --config
INPUT_FILE="" # --input
POSITIONAL_ARGS=() # <uuid>... (if any)
DO_EXPORT_TABLES=false # export_tables
DO_UPLOAD_METADATA=false # upload_metadata
DO_UPLOAD_DATA=false # upload_data


usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} -h
	${PROG_NAME} [-c <config>] <command>... <uuid>...
	${PROG_NAME} [-c <config>] -i <input-file> <command>...
	-h, --help	print help message and exit
	-c, --config	configuration file (default ${CONFIG_FILE})
	-i, --input	file containing measurement UUIDs

	command: export_tables, upload_metadata, upload_data (implies export_tables)
EOF
	exit "${exit_code}"
}

main() {
	local flags=()
	local uuid
	local n

	parse_args "$@"
	info "sourcing ${CONFIG_FILE}"
	# shellcheck disable=SC1090
	source "${CONFIG_FILE}"

	flags=(-c "${CONFIG_FILE}")
	n=0
	while read -r uuid; do
		if [[ ! "${uuid}" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]; then
			echo "skipping invalid uuid: ${uuid}"
			continue
		fi
		if ${DO_EXPORT_TABLES}; then
			info "${EXPORT_TABLES}" "${flags[@]}" "${uuid}" # XXX
		fi
		if ${DO_UPLOAD_METADATA}; then
			info "${UPLOAD_METADATA}" "${flags[@]}" "${uuid}" # XXX
		fi
		if ${DO_UPLOAD_DATA}; then
			info "${UPLOAD_DATA}" "${flags[@]}" "${uuid}" # XXX
		fi
		_=$(( n++ ))
		if [[ $n -gt 1 ]]; then
			echo
		fi
	done < <(get_uuids)
}

get_uuids() {
	if [[ "${INPUT_FILE}" != "" ]]; then
		cat "${INPUT_FILE}"
	else
		echo "${POSITIONAL_ARGS[@]}" | tr ' ' '\n'
	fi
}

parse_args() {
	local args
	local arg
	local no_cmd

	if ! args="$(getopt \
			--options "c:hi:" \
			--longoptions "config: help input:" \
			-- "$@")"; then
		usage 1
	fi
	eval set -- "${args}"
	
	if [[ "$#" -eq 1 ]]; then
		echo "${PROG_NAME}: specify one or more commands"
		usage 1
	fi

	# parse flags
	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-c|--config) CONFIG_FILE="$1"; shift 1;;
		-h|--help) usage 0;;
		-i|--input) INPUT_FILE="$1"; shift 1;;
		--) break;;
		*) echo "internal error parsing arg=${arg}"; usage 1;;
		esac
	done

	# parse command
	no_cmd=true
	while [[ $# -gt 0 ]]; do
		case "$1" in
		export_tables) DO_EXPORT_TABLES=true; no_cmd=false; shift 1;;
		upload_metadata) DO_UPLOAD_METADATA=true; no_cmd=false; shift 1;;
		upload_data) DO_EXPORT_TABLES=true; DO_UPLOAD_DATA=true; no_cmd=false; shift 1;;
		*) if [[ "${INPUT_FILE}" != "" ]]; then echo "cannot specify both -i and positional arguments"; return 1; fi; break 1;;
		esac
	done
	if ${no_cmd}; then
		echo "specify a command"
		return 1
	fi
	if [[ "${INPUT_FILE}" != "" && ! -f "${INPUT_FILE}" ]]; then
		echo "${INPUT_FILE} does not exist"
		return 1
	fi
	POSITIONAL_ARGS=("$@")
	return 0
}

info() {
	if [[ 1 -lt 0 ]]; then
		(1>&2 echo -n -e "\033[1;31m$PROG_NAME: \033[0m")
		(1>&2 echo -e "\033[1;34minfo: $*\033[0m")
	fi
}

main "$@"
