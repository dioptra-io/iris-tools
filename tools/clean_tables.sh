#!/bin/bash

set -eu
set -o pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

readonly PROG_NAME="${0##*/}"
CONFIG_FILE="$(git rev-parse --show-toplevel)/tables.conf" # --config
FORCE=false # --force


usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} [-hf] [-c <config>] <uuid>...
	-h, --help      print help message and exit
	-f, --force     recreate cleaned tables even if they exist
	-c, --config    configuration file (default ${CONFIG_FILE})

	uuid: measurement uuid
EOF
	exit "${exit_code}"
}

main() {
	local meas_uuid
	local table_prefix

        parse_args "$@"
        echo "sourcing ${CONFIG_FILE}"
        # shellcheck disable=SC1090
        source "${CONFIG_FILE}"

	if [[ -z "${IRIS_PASSWORD+x}" ]]; then
		irisctl auth login
	else
		echo "irisctl will use IRIS_PASSWORD environment variable when invoked"
	fi

	# shellcheck disable=SC1091
	source "${IRIS_ENV}"

	echo "tables to clean: ${TABLES_TO_CLEAN[*]}"
	for meas_uuid in "$@"; do
		for table_prefix in "${TABLES_TO_CLEAN[@]}"; do
			echo "cleaning ${meas_uuid} ${table_prefix} tables"
			clean_tables "${meas_uuid}" "${table_prefix}"
			echo
		done
	done
}

clean_tables() {
	local meas_uuid="$1"
	local table_prefix="$2"
	local tmpfile
	local meas_tables_names=()
	local table_name

	tmpfile="$(mktemp /tmp/clean_tables.XXXX)"
	irisctl analyze tables --meas-uuid "${meas_uuid}" "${MEAS_MD_ALL_JSON}" -o |
	awk -v pat="${table_prefix}" '$0 ~ pat { print $1 }' > "${tmpfile}"

	while IFS= read -r line; do
		meas_tables_names+=("${line}")
	done < "${tmpfile}"

	for table_name in "${meas_tables_names[@]}"; do
		if [[ "${table_name}" == "cleaned_"* ]]; then
			continue
		fi
		#
		# XXX This logic assumes that each table has a cleaned counterpart.
		#
		if ! ${FORCE}; then
			if grep -q "cleaned_${table_name}" "${tmpfile}"; then
				echo "${table_name} already cleaned"
				continue
			fi
		fi

		echo "creating query file to clean ${table_name}"
		uncleaned_rows=$(clickhouse-client --user "${IRIS_CLICKHOUSE_USER}" --password "${IRIS_CLICKHOUSE_PASSWORD}" -q "SELECT COUNT(*) FROM ${DATABASE_NAME}.${table_name}")
		echo "rows: ${uncleaned_rows}"

		case "${table_name}" in
		"probes__"*)   order_by="${ORDER_BY_PROBES}";   select="${SELECT_PROBES}";;
		"results__"*)  order_by="${ORDER_BY_RESULTS}";  select="${SELECT_RESULTS}";;
		"links__"*)    order_by="${ORDER_BY_LINKS}";    select="${SELECT_LINKS}";;
		"prefixes__"*) order_by="${ORDER_BY_PREFIXES}"; select="${SELECT_PREFIXES}";;
		*) echo "internal error parsing ${table_name}"; exit 1;;
		esac
		clean_sql="$(create_clean_query "cleaned_${table_name}" "${table_name}" "probes_${table_name#*_}" "${order_by}" "${select}")"

		echo "clickhouse-client --user iris --password \${IRIS_CLICKHOUSE_PASSWORD} --queries-file ${clean_sql}"
		clickhouse-client --user "${IRIS_CLICKHOUSE_USER}" --password "${IRIS_CLICKHOUSE_PASSWORD}" --queries-file "${clean_sql}"
		cleaned_rows=$(clickhouse-client --user "${IRIS_CLICKHOUSE_USER}" --password "${IRIS_CLICKHOUSE_PASSWORD}" -q "SELECT COUNT(*) FROM ${DATABASE_NAME}.cleaned_${table_name}")
		echo "rows: ${cleaned_rows} ($((uncleaned_rows - cleaned_rows)) rows deleted)"

		# perform sanity check
		if [[ ${cleaned_rows} -gt ${uncleaned_rows} ]]; then
			echo "error: cleaned_rows ${cleaned_rows} is greater than uncleaned_rows ${uncleaned_rows}"
			exit 1
		fi
		echo
	done
}

create_clean_query() {
	local cleaned_table_name="$1"
	local from_table_name="$2"
	local probes_table_name="$3"
	local order_by="$4"
	local select="$5"
	local clean_sql="./clean.sql"

	cat > "${clean_sql}" <<EOF
CREATE TABLE IF NOT EXISTS ${DATABASE_NAME}.${cleaned_table_name}
ENGINE = MergeTree
ORDER BY ${order_by}
TTL toDateTime('2100-01-01 00:00:00')
TO VOLUME 'archive'
SETTINGS storage_policy = 'archive', index_granularity = 8192
AS SELECT ${select}
FROM ${DATABASE_NAME}.${from_table_name}
WHERE probe_dst_prefix IN (
    SELECT DISTINCT probe_dst_prefix
    FROM ${DATABASE_NAME}.${probes_table_name}
    WHERE (round = 1) AND (probe_ttl <= 10)
)
EOF
	echo "${clean_sql}"
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

        if [[ "$#" -eq 0 ]]; then
                echo "${PROG_NAME}: specify at least one measurement uuid"
		usage 1
        fi
}

main "$@"
