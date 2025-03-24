#!/bin/bash

set -eu
set -o pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

readonly PROG_NAME="${0##*/}"
readonly TOPLEVEL="$(git rev-parse --show-toplevel)"

#
# Global variables to support command line flags.
#
CONFIG_FILE="${TOPLEVEL}/conf/settings.conf"	# --config
DRY_RUN=false					# --dry-run
INPUT_FILE=""					# --input
DO_PUBLISH_METADATA=false			# publish_metadata
DO_EXPORT_RAW_TABLES=false			# export_raw_tables
DO_EXPORT_CLEANED_TABLES=false			# export_cleaned_tables
DO_PUBLISH_DATA=false				# publish_data
POSITIONAL_ARGS=()				# <uuid>... (if any)


usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} -h
	${PROG_NAME} [-c <config>] <command>... <uuid>...
	${PROG_NAME} [-c <config>] -i <input-file> <command>...
	-c, --config	configuration file (default ${CONFIG_FILE})
	-n, --dry-run	enable the dry-run mode
	-h, --help	print help message and exit
	-i, --input	file containing measurement UUIDs

	command: export_raw_tables, export_cleaned_tables, publish_metadata, publish_data (implies export_cleaned_tables)
EOF
	exit "${exit_code}"
}

main() {
	local dryrun=""
	local flags=()
	local uuid
	local n

	parse_args "$@"
	info "sourcing ${CONFIG_FILE}"
	# shellcheck disable=SC1090
	source "${CONFIG_FILE}"

	if ${DRY_RUN}; then
		dryrun="info"
	fi
	flags=(-c "${CONFIG_FILE}")
	n=0
	while read -r uuid; do
		if [[ ! "${uuid}" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]; then
			echo "skipping invalid uuid: ${uuid}"
			continue
		fi
		if ${DO_PUBLISH_METADATA}; then
			eval ${dryrun} "${UPLOAD_METADATA}" "${flags[@]}" "${uuid}"
		fi
		if ${DO_EXPORT_RAW_TABLES}; then
			eval ${dryrun} "${EXPORT_TABLES}" "${flags[@]}" "${uuid}"
		fi
		if ${DO_EXPORT_CLEANED_TABLES}; then
			eval ${dryrun} "${EXPORT_TABLES}" "${flags[@]}" "${uuid}"
		fi
		if ${DO_PUBLISH_DATA}; then
			eval ${dryrun} "${UPLOAD_DATA}" "${flags[@]}" "${uuid}"
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
			--options "c:hi:n" \
			--longoptions "config: help input: dry-run" \
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
		-n|--dry-run) DRY_RUN=true;;
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
		publish_metadata) DO_PUBLISH_METADATA=true; no_cmd=false; shift 1;;
		export_raw_tables) DO_EXPORT_RAW_TABLES=true; no_cmd=false; shift 1;;
		export_cleaned_tables) DO_EXPORT_CLEANED_TABLES=true; no_cmd=false; shift 1;;
		publish_data) DO_EXPORT_CLEANED_TABLES=true; DO_PUBLISH_DATA=true; no_cmd=false; shift 1;;
		*) if [[ "${INPUT_FILE}" != "" ]]; then echo "cannot specify both -i and positional arguments"; return 1; fi; break 1;;
		esac
	done
	if ${no_cmd}; then
		echo "specify a command"
		return 1
	fi
	if ${DO_EXPORT_RAW_TABLES} && ${DO_EXPORT_CLEANED_TABLES}; then
		echo "cannot specify both export_raw_tables and export_cleaned_tables"
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
