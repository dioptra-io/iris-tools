#!/bin/bash

set -eu
set -o pipefail
shopt -s nullglob
shellcheck "$0" # exits if shellcheck doesn't pass

readonly PROG_NAME="${0##*/}"
CONFIG_FILE="$(git rev-parse --show-toplevel)/conf/tables.conf" # --config
FORCE=false # --force
POSITIONAL_ARGS=()


usage() {
        local exit_code="$1"

        cat <<EOF
usage:
        ${PROG_NAME} [-hf] [-c <config>] <uuid>...
        -h, --help      print help message and exit
        -f, --force     export tables even if already exported (i.e., exists in the cache)
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

	# shellcheck disable=SC1090
	source "${IRIS_ENV}"

        echo "tables to export: ${TABLES_TO_EXPORT[*]}"
	for meas_uuid in "${POSITIONAL_ARGS[@]}"; do
		for table_prefix in "${TABLES_TO_EXPORT[@]}"; do
			if [[ "${table_prefix}" != "results__" ]]; then
				echo "do not have query for exproting ${table_prefix} tables"
				return 1
			fi
			echo exporting "${meas_uuid}" "${table_prefix}" tables
			export_tables "${meas_uuid}" "${table_prefix}"
			echo
		done
	done
}

export_tables() {
        local meas_uuid="$1"
        local table_prefix="$2"
        local tmpfile
        local meas_tables_names=()
        local table_name
	local export_file
	local query

        tmpfile="$(mktemp /tmp/export_tables.XXXX)"
        irisctl analyze tables --meas-uuid "${meas_uuid}" "${MEAS_MD_ALL_JSON}" -o |
        awk -v pat="${table_prefix}" '$0 ~ pat { print $1 }' > "${tmpfile}"

        while IFS= read -r line; do
                meas_tables_names+=("${line}")
        done < "${tmpfile}"

        for table_name in "${meas_tables_names[@]}"; do
                if [[ "${table_name}" != "cleaned_"* ]]; then
			if grep -q "cleaned_${table_name}" "${tmpfile}"; then
				echo "skipping ${table_name} because it has a cleaned version"
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
		echo "exporting ${table_name}"
		query="${RESULTS_TABLE_EXPORT//\$\{table\}/$table_name}"
		query="${query//\$\{DATABASE_NAME\}/$DATABASE_NAME}"
		query="${query//\$\{EXPORT_FORMAT\}/$EXPORT_FORMAT}"
		"${TIME[@]}" clickhouse-client --user="${IRIS_CLICKHOUSE_USER}" --password="${IRIS_CLICKHOUSE_PASSWORD}" -mn <<EOF > "${export_file}"
${query}
EOF
	done
}

verify_free_space() {
	local treshold="$1"
	local used

	used=$(df -h . | awk 'NR==2 { print $5 }' | tr -d '%')
	if [[ ${used} -ge $((100 - treshold)) ]]; then
		echo "filesystem has less than ${treshold}% free space"
		exit 1
	fi
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
	POSITIONAL_ARGS=("$@")
}

main "$@"
