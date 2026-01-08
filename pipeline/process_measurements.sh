#!/usr/bin/env bash

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC1090,SC2064"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
readonly TOPLEVEL="$(git rev-parse --show-toplevel)"
source "${TOPLEVEL}/pipeline/common.sh"

#
# Global variables to support command line flags and arguments.
#
CONFIG_FILE="${TOPLEVEL}/conf/publish_settings.conf"	# --config
DRY_RUN=false						# --dry-run
INPUT_FILE=""						# --input
VERBOSE=1						# --verbose
DO_PUBLISH_METADATA=false				# publish_metadata
DO_EXPORT_RAW_TABLES=false				# export_raw_tables
DO_EXPORT_CLEANED_TABLES=false				# export_cleaned_tables
DO_PUBLISH_DATA=false					# publish_data
POSITIONAL_ARGS=()					# <uuid>... (if any)


#
# Print usage message and exit.
#
usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} -h
	${PROG_NAME} [-v <n>] [-c <config>] [-n] <command>... <uuid>...
	${PROG_NAME} [-v <n>] [-c <config>] [-n] -i <input-file> <command>...
	-c, --config	configuration file (default ${CONFIG_FILE})
	-n, --dry-run	enable the dry-run mode
	-h, --help	print help message and exit
	-i, --input	file containing measurement UUIDs
	-v, --verbose	set the verbosity level (default: ${VERBOSE})

	command: export_raw_tables, export_cleaned_tables, publish_metadata, publish_data (implies export_cleaned_tables)
EOF
	exit "${exit_code}"
}

main() {
	local flags=()
	local n
	local uuid

	parse_cmdline_and_conf "$@"

	flags=(-c "${CONFIG_FILE}")
	if ${DRY_RUN}; then
		flags+=("--dry-run")
	fi
	n=0
	while read -r uuid; do
		if [[ ! "${uuid}" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]; then
			echo "skipping invalid uuid: ${uuid}"
			continue
		fi
		if ${DO_PUBLISH_METADATA}; then
			log_info 1 "${UPLOAD_METADATA}" "${flags[@]}" "${uuid}"
			"${UPLOAD_METADATA}" "${flags[@]}" "${uuid}"
		fi
		if ${DO_EXPORT_RAW_TABLES}; then
			log_info 1 "${EXPORT_RAW_TABLES}" "${flags[@]}" "${uuid}"
			"${EXPORT_RAW_TABLES}" "${flags[@]}" "${uuid}"
		fi
		if ${DO_EXPORT_CLEANED_TABLES}; then
			log_info 1 "${EXPORT_CLEANED_TABLES}" "${flags[@]}" "${uuid}"
			"${EXPORT_CLEANED_TABLES}" "${flags[@]}" "${uuid}"
		fi
		if ${DO_PUBLISH_DATA}; then
			log_info 1 "${UPLOAD_DATA}" "${flags[@]}" "${uuid}"
			"${UPLOAD_DATA}" "${flags[@]}" "${uuid}"
		fi
		_=$(( n++ ))
		if [[ $n -gt 1 ]]; then
			echo
		fi
	done < <(get_uuids)
}

#
# Get the list of UUIDs to process.  UUIDs can be specified either on
# the command line or in a file, one UUID per line.
#
get_uuids() {
	if [[ "${INPUT_FILE}" != "" ]]; then
		cat "${INPUT_FILE}"
	else
		echo "${POSITIONAL_ARGS[@]}" | tr ' ' '\n'
	fi
}

#
# Parse the command line and the configuration file.
#
parse_cmdline_and_conf() {
	local args
	local arg
	local no_cmd

	if ! args="$(getopt \
			--options "c:hi:nv:" \
			--longoptions "config: help input: dry-run verbose:" \
			-- "$@")"; then
		usage 1
	fi
	eval set -- "${args}"
	
	# Parse flags.
	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-c|--config) CONFIG_FILE="$1"; shift 1;;
		-n|--dry-run) DRY_RUN=true;;
		-h|--help) usage 0;;
		-i|--input) INPUT_FILE="$1"; shift 1;;
		-v|--verbose) VERBOSE="$1"; shift 1;;
		--) break;;
		*) log_fatal "panic: error parsing arg=${arg}";;
		esac
	done

	# Parse arguments.
	no_cmd=true
	while [[ $# -gt 0 ]]; do
		case "$1" in
		publish_metadata) DO_PUBLISH_METADATA=true; no_cmd=false; shift 1;;
		export_raw_tables) DO_EXPORT_RAW_TABLES=true; no_cmd=false; shift 1;;
		export_cleaned_tables) DO_EXPORT_CLEANED_TABLES=true; no_cmd=false; shift 1;;
		publish_data) DO_EXPORT_CLEANED_TABLES=true; DO_PUBLISH_DATA=true; no_cmd=false; shift 1;;
		*) break;;
		esac
	done
	POSITIONAL_ARGS=("$@")

	if ${no_cmd}; then
		log_fatal "specify at least one command"
	fi
	if ${DO_EXPORT_RAW_TABLES} && ${DO_EXPORT_CLEANED_TABLES}; then
		log_fatal "cannot specify both export_raw_tables and export_cleaned_tables"
	fi
	if [[ "${INPUT_FILE}" != "" && ${#POSITIONAL_ARGS[@]} -ne 0 ]]; then
		log_fatal "cannot specify both -i and positional arguments"
	fi
	if [[ "${INPUT_FILE}" != "" && ! -f "${INPUT_FILE}" ]]; then
		log_fatal "${INPUT_FILE} does not exist"
	fi

	log_info 1 "sourcing ${CONFIG_FILE}"
	source "${CONFIG_FILE}"
}

main "$@"
