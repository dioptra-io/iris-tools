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
        -f, --force     upload tables even if they already exist in BigQuery
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

	# shellcheck disable=SC1090
	source "${IRIS_ENV}"

        echo "tables to upload: ${TABLES_TO_UPLOAD[*]}"
	for meas_uuid in "${POSITIONAL_ARGS[@]}"; do
		for table_prefix in "${TABLES_TO_UPLOAD[@]}"; do
			if [[ "${table_prefix}" != "results__" ]]; then
				echo "do not have query for uploading ${table_prefix} tables"
				return 1
			fi
			echo uploading "${meas_uuid}" "${table_prefix}" tables
			upload_tables "${meas_uuid}" "${table_prefix}"
			echo
		done
	done
}

upload_tables() {
        local meas_uuid="$1"
        local table_prefix="$2"
	local path
	local files=()
	local bq_iris_table

	files=("${EXPORT_DIR}"/*."${EXPORT_FORMAT}")
	if [[ ${#files[@]} -eq 0 ]]; then
		echo "no ${EXPORT_FORMAT} files in ${EXPORT_DIR}"
		return
	fi
	echo "${SCHEMA_RESULTS}" > "${SCHEMA_RESULTS_JSON}"
	for path in "${EXPORT_DIR}"/*"${meas_uuid//-/_}"*."${EXPORT_FORMAT}"; do
		filename="${path##*/}" # remove everything up to the last slash
		bq_iris_table="${BQ_DATASET}.${filename%.${EXPORT_FORMAT}}"
		# Check if the iris table is already uploaded.
		echo bq show --project_id="${GCP_PROJECT_ID}" "${bq_iris_table}"
		if ! ${FORCE} && bq show --project_id="${GCP_PROJECT_ID}" "${bq_iris_table}" > /dev/null 2>&1; then
			echo "${bq_iris_table} already uploaded"
			# XXX Add logic to avoid converting measurements twice.
		else
			# create the table with the schema file
			echo bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_RESULTS_JSON}" --table "${bq_iris_table}"
			"${TIME}" bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_RESULTS_JSON}" --table "${bq_iris_table}"

			# upload values into the iris table
			echo bq load --project_id="${GCP_PROJECT_ID}" --source_format=PARQUET "${bq_iris_table}" "${path}"
			"${TIME}" bq load --project_id="${GCP_PROJECT_ID}" --source_format=PARQUET "${bq_iris_table}" "${path}"
		fi

		# convert the iris table and create scamper1 table
		echo creating scamper1 table from iris table
		convert_and_create_scamper1 "${bq_iris_table}"
	done
}

convert_and_create_scamper1() {
	local bq_iris_table="$1"

	"${TIME}" bq query --project_id="${GCP_PROJECT_ID}" \
		--use_legacy_sql=false \
		--parameter="scamper1_table_name_param:STRING:${BQ_DATASET}.${BQ_TABLE}" \
		--parameter="table_name_param:STRING:${bq_iris_table}" \
		--parameter="host_param:STRING:asia-east1" \
		--parameter="version_param:STRING:1.1.5" \
		--parameter="user_id_param:STRING:8b891667-7d2c-4098-9f75-2b0379feb4e1" \
		--parameter="tool_param:STRING:diamond-miner" \
		--parameter="min_ttl_param:STRING:4" \
		--parameter="failure_probability_param:STRING:0.05" \
		< "${TABLE_CONVERSION_QUERY}"
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
