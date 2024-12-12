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
        local bq_public_dataset
	local bq_private_dataset
	local bq_dataset_table
	local meas_md_tmpfile
	local meas_uuid
	local table_prefix

	parse_args "$@"
	echo "sourcing ${CONFIG_FILE}"
	# shellcheck disable=SC1090
	source "${CONFIG_FILE}"
	# shellcheck disable=SC1090
	source "${IRIS_ENV}"

	# create the dataset for inserting measurement data in scamper1 format if it doesn't exist
        # shellcheck disable=SC2153
	bq_public_dataset="${BQ_PUBLIC_DATASET}"
	echo verifying "${bq_public_dataset}"
	verify_dataset "${bq_public_dataset}"

	# create the dataset for uploading temporary iris tables  if it doesn't exist
        # shellcheck disable=SC2153
	bq_private_dataset="${BQ_PRIVATE_DATASET}"
        echo verifying "${bq_private_dataset}"
        verify_dataset "${bq_private_dataset}"

	# create the table for inserting measurement data in scamper1 format  if it doesn't exist
	bq_dataset_table="${BQ_PUBLIC_DATASET}"."${BQ_TABLE}"
	if ! ${FORCE} && bq show --project_id="${GCP_PROJECT_ID}" "${bq_dataset_table}" > /dev/null 2>&1; then
		echo "${bq_dataset_table} already exists"
	else
		# create the $BQ_TABLE table with the schema file
		echo bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_SCAMPER1_JSON}" --clustering_fields "id" --time_partitioning_field "date" --time_partitioning_type DAY --table "${bq_dataset_table}"
		"${TIME}" bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_SCAMPER1_JSON}" --clustering_fields "id" --time_partitioning_field "date" --time_partitioning_type DAY --table "${bq_dataset_table}"
	fi

	echo "tables to upload: ${TABLES_TO_UPLOAD[*]}"
	meas_md_tmpfile="$(mktemp /tmp/upload_tables.XXXX)"
	for meas_uuid in "${POSITIONAL_ARGS[@]}"; do
		# first get the metadata of this measurement
		irisctl meas --uuid "${meas_uuid}" -o > "${meas_md_tmpfile}"
		# now upload this measurement's tables
		for table_prefix in "${TABLES_TO_UPLOAD[@]}"; do
			if [[ "${table_prefix}" != "results__" ]]; then
				echo "do not have query for uploading ${table_prefix} tables"
				return 1
			fi
			echo uploading "${meas_uuid}" "${table_prefix}" tables
			upload_tables "${meas_uuid}" "${meas_md_tmpfile}" "${table_prefix}"
			echo
		done
	done
	rm -f "${meas_md_tmpfile}"
}

verify_dataset() {
        local dataset="$1"

	# check if dataset exists
        if ! ${FORCE} && bq show --project_id="${GCP_PROJECT_ID}" "${dataset}" > /dev/null 2>&1; then
                echo "${dataset} already exists"
        else
                # create the dataset
                echo bq mk --project_id "${GCP_PROJECT_ID}" --dataset "${dataset}"
                "${TIME}" bq mk --project_id "${GCP_PROJECT_ID}" --dataset "${dataset}"
        fi
}

upload_tables() {
	local meas_uuid="$1"
	local meas_md_tmpfile="$2"
	local table_prefix="$3"
	local path
	local files=()
	local bq_tmp_table
	local clustering

	files=("${EXPORT_DIR}"/*."${EXPORT_FORMAT}")
	clustering="probe_dst_addr,probe_src_port,probe_ttl,reply_src_addr"
	if [[ ${#files[@]} -eq 0 ]]; then
		echo "no ${EXPORT_FORMAT} files in ${EXPORT_DIR}"
		return
	fi
	echo "${SCHEMA_RESULTS}" > "${SCHEMA_RESULTS_JSON}"
	for path in "${EXPORT_DIR}"/*"${meas_uuid//-/_}"*."${EXPORT_FORMAT}"; do
		filename="${path##*/}" # remove everything up to the last slash
		bq_tmp_table="${BQ_PRIVATE_DATASET}.${filename%.${EXPORT_FORMAT}}"
		# Check if the iris table is already uploaded.
		echo bq show --project_id="${GCP_PROJECT_ID}" "${bq_tmp_table}"
		if ! ${FORCE} && bq show --project_id="${GCP_PROJECT_ID}" "${bq_tmp_table}" > /dev/null 2>&1; then
			echo "${bq_tmp_table} already uploaded"
		else
			# create the temporary table with the schema file
			echo bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_RESULTS_JSON}" --clustering_fields="${clustering}" --table "${bq_tmp_table}"
			"${TIME}" bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_RESULTS_JSON}"  --clustering_fields="${clustering}" --table "${bq_tmp_table}"

			# upload values into the temporary table
			echo bq load --project_id="${GCP_PROJECT_ID}" --source_format=PARQUET "${bq_tmp_table}" "${path}"
			"${TIME}" bq load --project_id="${GCP_PROJECT_ID}" --source_format=PARQUET "${bq_tmp_table}" "${path}"
		fi

		echo building rows from the temporary table and inserting them into "$BQ_TABLE"
		convert_and_insert_values "${meas_uuid}" "${meas_md_tmpfile}" "${bq_tmp_table}"

		# delete temporary table after conversion
		bq rm -t -f --project_id="${GCP_PROJECT_ID}" "${bq_tmp_table}"
		echo table "${bq_tmp_table}" deleted
		# delete table from EXPORT_DIR after conversion
		rm -f "${path}"
  		echo "${path}" removed
	done
}

convert_and_insert_values() {
	local meas_uuid="$1"
	local meas_md_tmpfile="$2"
	local bq_tmp_table="$3"
	local agent
	local index
	local tool
	local start_time
	local md_fields
	local MD_FIELDS=()

	# get the metadata of this measurement
	agent="$(echo "${bq_tmp_table}" | sed -e 's/.*__\(........_...._...._...._............\)/\1/' | tr '_' '-')"
	index="$(jq --arg agent "$agent" '[.agents[].agent_uuid] | index($agent)' "${meas_md_tmpfile}")"
	# sanity check
	for v in agent index; do
		if [[ "${!v}" == "" ]]; then
			echo "error: failed to parse ${v}"
			return 1
		fi
	done

	tool="$(jq -r .tool "${meas_md_tmpfile}")"
	start_time="$(jq -r .start_time "${meas_md_tmpfile}")"
	# sanity check
	if [[ "${tool}" != "diamond-miner" && "${tool}" != "yarrp" ]]; then
		echo "error: invalid tool: ${tool}"
		return 1
	fi
	# order of these fields should match --parameter lines in the bq command below
	MD_FIELDS=(
	    .agent_parameters.hostname
	    .agent_parameters.version
	    .agent_parameters.min_ttl
	    .tool_parameters.failure_probability
	)
	md_fields="$(IFS=,; echo "${MD_FIELDS[*]}")"
	read -r -a MD_VALUES <<< "$(jq -r ".agents[${index}] | [${md_fields}] | @tsv" "${meas_md_tmpfile}")"
	# sanity check
	if [[ ${#MD_VALUES[@]} -ne ${#MD_FIELDS[@]} ]]; then
		echo "error: expected to parse ${#MD_FIELDS[@]} values, got ${#MD_VALUES[@]} values"
		return 1
	fi

	"${TIME}" bq query --project_id="${GCP_PROJECT_ID}" \
		--use_legacy_sql=false \
		--parameter="scamper1_table_name_param:STRING:${BQ_PUBLIC_DATASET}.${BQ_TABLE}" \
		--parameter="measurement_uuid_param:STRING:${meas_uuid}" \
		--parameter="table_name_param:STRING:${bq_tmp_table}" \
		--parameter="measurement_agent_param:STRING:${bq_tmp_table#*__}" \
		--parameter="tool_param:STRING:${tool}" \
		--parameter="start_time_param:STRING:${start_time}" \
		--parameter="host_param:STRING:${MD_VALUES[0]}" \
		--parameter="version_param:STRING:${MD_VALUES[1]}" \
		--parameter="min_ttl_param:STRING:${MD_VALUES[2]}" \
		--parameter="failure_probability_param:STRING:${MD_VALUES[3]}" \
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
