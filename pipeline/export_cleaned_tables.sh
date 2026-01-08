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
FORCE=false						# --force
VERBOSE=1						# --verbose
POSITIONAL_ARGS=()					# <uuid>...


usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} --help
	${PROG_NAME} [-c <config>] [-f] [-v <n>] <uuid>...
	-c, --config    configuration file (default ${CONFIG_FILE})
	-n, --dry-run   enable the dry-run mode
	-f, --force     export tables even if already exported (i.e., exists in the cache)
	-h, --help      print help message and exit
	-v, --verbose   set the verbosity level (default: ${VERBOSE})

	uuid: measurement uuid
EOF
	exit "${exit_code}"
}

cleanup() {
	log_info 1 "removing /tmp/${PROG_NAME}.$$.*"
	rm -f "/tmp/${PROG_NAME}.$$."*
}
trap cleanup EXIT

main() {
	local meas_uuid
	local table_prefix

	parse_cmdline_and_conf "$@"

	# Authenticate with Iris API so we can run irisctl.
	irisctl_auth

	log_info 1 "assuming MEAS_MD_ALL_JSON ${MEAS_MD_ALL_JSON} is up-to-date"
	log_info 1 "tables to export: ${TABLES_TO_EXPORT[*]}"
	for meas_uuid in "${POSITIONAL_ARGS[@]}"; do
		for table_prefix in "${TABLES_TO_EXPORT[@]}"; do
			if [[ "${table_prefix}" != "results__" ]]; then
				log_fatal "do not have query for exporting ${table_prefix} tables"
			fi
			log_info 1 exporting cleaned "${meas_uuid}" "${table_prefix}" tables
			export_cleaned_tables "${meas_uuid}" "${table_prefix}"
			echo
		done
	done
}

export_cleaned_tables() {
	local meas_uuid="$1"
	local table_prefix="$2"
	local tmpfile
	local meas_tables_names=()
	local table_name
	local probes_table_name
	local export_file
	local query

	tmpfile="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	log_info 1 irisctl analyze tables --meas-uuid "${meas_uuid}" "${MEAS_MD_ALL_JSON}" -o
	irisctl analyze tables --meas-uuid "${meas_uuid}" "${MEAS_MD_ALL_JSON}" -o |
	awk -v pat="${table_prefix}" '$0 ~ pat { print $1 }' > "${tmpfile}"

	while IFS= read -r line; do
		meas_tables_names+=("${line}")
	done < "${tmpfile}"

	for table_name in "${meas_tables_names[@]}"; do
		log_info 2 "checking table_name=${table_name}"
		if [[ "${table_name}" != "cleaned_"* ]]; then
			if grep -q "cleaned_${table_name}" "${tmpfile}"; then
				echo "not exporting the following table because it has a cleaned version"
				echo "${table_name}"
				continue
			fi
		fi
		export_file="${EXPORT_DIR}/${table_name}.${EXPORT_FORMAT}"
		if ! ${FORCE} && [[ -f "${export_file}" ]]; then
			echo "${export_file} already exported"
			continue
		fi
		verify_free_space 10
		mkdir -p "${EXPORT_DIR}"
		log_info 1 "exporting cleaned ${table_name}"
		probes_table_name="${table_name//results/probes}"
		query="${CLEANED_RESULTS_TABLE_EXPORT//\$\{table\}/$table_name}"
		query="${query//\$\{probes_table\}/$probes_table_name}"
		query="${query//\$\{DATABASE_NAME\}/$DATABASE_NAME}"
		query="${query//\$\{EXPORT_FORMAT\}/$EXPORT_FORMAT}"
		if ${DRY_RUN}; then
			log_info 1 clickhouse-client --user="${CLICKHOUSE_USERNAME}" --password="${CLICKHOUSE_PASSWORD}" -mn "<<EOF > ${export_file}"
			echo "${query}"
			echo "EOF"
			continue
		fi
		"${TIME[@]}" clickhouse-client --user="${CLICKHOUSE_USERNAME}" --password="${CLICKHOUSE_PASSWORD}" -mn <<EOF > "${export_file}"
${query}
EOF
	done
}

verify_free_space() {
	local treshold="$1"
	local used

	used=$(df -h . | awk 'NR==2 { print $5 }' | tr -d '%')
	if [[ ${used} -ge $((100 - treshold)) ]]; then
		log_fatal "filesystem has less than ${treshold}% free space"
	fi
}

#
# Parse the command line and the configuration file.
#
parse_cmdline_and_conf() {
	local args
	local arg

	if ! args="$(getopt \
			--options "c:fhnv:" \
			--longoptions "config: dry-run force help verbose:" \
			-- "$@")"; then
		usage 1
	fi
	eval set -- "${args}"

	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-c|--config) CONFIG_FILE="$1"; shift 1;;
		-n|--dry-run) DRY_RUN=true;;
		-f|--force) FORCE=true;;
		-h|--help) usage 0;;
		-v|--verbose) VERBOSE="$1"; shift 1;;
		--) break;;
		*) log_fatal "panic: error parsing arg=${arg}";;
		esac
	done
	POSITIONAL_ARGS=("$@")
	if [[ ${#POSITIONAL_ARGS[@]} -lt 1 ]]; then
		log_fatal "specify at least one measurement uuid"
	fi

	log_info 1 "sourcing ${CONFIG_FILE}"
	source "${CONFIG_FILE}"
}

main "$@"
