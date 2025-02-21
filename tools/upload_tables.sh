#!/bin/bash

set -eu
set -o pipefail
shopt -s nullglob
shellcheck "$0" # exits if shellcheck doesn't pass

readonly PROG_NAME="${0##*/}"
CONFIG_FILE="$(git rev-parse --show-toplevel)/conf/tables.conf" # --config
POSITIONAL_ARGS=()


usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} [-hf] [-c <config>] <uuid>...
	-h, --help      print help message and exit
	-c, --config    configuration file (default ${CONFIG_FILE})

	uuid: measurement uuid
EOF
	exit "${exit_code}"
}

main() {
	local bq_metadata_table
	local resources
	local resource
	local bq_public_table
	local meas_md_tmpfile
	local meas_uuid
	local table_prefix
	local query

	parse_args "$@"
	echo "sourcing ${CONFIG_FILE}"
	# shellcheck disable=SC1090
	source "${CONFIG_FILE}"
	# shellcheck disable=SC1090
	source "${IRIS_ENV}"
	# Check if GCP_INSTANCES is provided and the corresponding file exists
	[[ -n "${GCP_INSTANCES:-}" ]] && [[ ! -f "${GCP_INSTANCES}" ]] && { echo "error: File does not exist: ${GCP_INSTANCES}" >&2; exit 1; }

	# check if datasets and metadata table exist
	bq_metadata_table="${BQ_PUBLIC_DATASET}.${BQ_METADATA_TABLE:?unset BQ_METADATA_TABLE}"
	resources=("${BQ_PUBLIC_DATASET}" "${BQ_PRIVATE_DATASET}" "${bq_metadata_table}")
	echo "checking ${resources[*]}"
	for resource  in "${resources[@]}"; do
		if ! check_dataset_or_table "${resource}"; then
			echo "error: ${resource} does not exist"
			exit 1
		fi
	done

	# create the table for inserting measurement data in scamper1 format  if it doesn't exist
	bq_public_table="${BQ_PUBLIC_DATASET}"."${BQ_PUBLIC_TABLE:?unset BQ_PUBLIC_TABLE}"
	if check_dataset_or_table "${bq_public_table}"; then
		echo "${bq_public_table} already exists"
	else
		# create the $BQ_PUBLIC_TABLE table with the schema file
		echo "bq mk --project_id ${GCP_PROJECT_ID} --schema ${SCHEMA_SCAMPER1_JSON} --clustering_fields id --time_partitioning_field date --time_partitioning_type DAY --table ${bq_public_table}"
		"${TIME}" bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_SCAMPER1_JSON}" --clustering_fields "id" --time_partitioning_field "date" --time_partitioning_type DAY --table "${bq_public_table}"
	fi

	echo "tables to upload: ${TABLES_TO_UPLOAD[*]}"
	meas_md_tmpfile="$(mktemp /tmp/upload_tables.XXXX)"
	for meas_uuid in "${POSITIONAL_ARGS[@]}"; do
		# first check if meas_uuid is in BQ_METADATA_TABLE
		echo "checking ${meas_uuid} in ${bq_metadata_table}"
		if ! check_uuid_in_metadata "${meas_uuid}" "${bq_metadata_table}"; then
			echo "${meas_uuid} is not in ${bq_metadata_table}"
			exit 1
		fi
		# then  get the metadata of this measurement
		irisctl meas --uuid "${meas_uuid}" -o > "${meas_md_tmpfile}"
		# now upload this measurement's tables
		for table_prefix in "${TABLES_TO_UPLOAD[@]}"; do
			if [[ "${table_prefix}" != "results__" ]]; then
				echo "do not have query for uploading ${table_prefix} tables"
				return 1
			fi
			echo "uploading ${meas_uuid} ${table_prefix}" tables
			upload_tables "${meas_uuid}" "${meas_md_tmpfile}" "${table_prefix}"
			echo
		# finally, update where_published field in BQ_METADATA_TABLE
		query="${UPDATE_WHERE_PUBLISHED//\$\{meas_uuid\}/$meas_uuid}"
		echo "bq query --use_legacy_sql=false --project_id=${GCP_PROJECT_ID} ${query}"
		bq query --use_legacy_sql=false --project_id="${GCP_PROJECT_ID}" "${query}"
		done
	done
	rm -f "${meas_md_tmpfile}"
}

check_dataset_or_table() {
        local resource="$1"

	# check if resource exists
        if ! bq show --project_id="${GCP_PROJECT_ID}" "${resource}" > /dev/null 2>&1; then
                return 1
        fi
}

check_uuid_in_metadata() {
	local meas_uuid="$1"
	local bq_metadata_table="$2"
	local query
	local query_result

	query="SELECT COUNT(*) FROM \`${bq_metadata_table}\` WHERE id = '${meas_uuid}'"
	echo "${query}"
	query_result=$(bq query --use_legacy_sql=false --project_id="${GCP_PROJECT_ID}" --format=csv "${query}" | tail -n 1)
	echo "${query_result}"
	if [[ "${query_result}" == "0" ]]; then
		return 1
	fi
}

upload_tables() {
	local meas_uuid="$1"
	local meas_md_tmpfile="$2"
	local table_prefix="$3"
	local files=()
	local clustering
	local path
	local filename
	local bq_tmp_table

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
		# If iris table already exists, it might be empty, so delete it to ensure a fresh start before reloading data.
		if check_dataset_or_table "${bq_tmp_table}"; then
			echo "${bq_tmp_table} already uploaded"
			bq rm -t -f --project_id="${GCP_PROJECT_ID}" "${bq_tmp_table}"
			echo "temporary table ${bq_tmp_table} deleted"
		fi
		# create the temporary table with the schema file
		echo "bq mk --project_id ${GCP_PROJECT_ID} --schema ${SCHEMA_RESULTS_JSON} --clustering_fields=${clustering} --table ${bq_tmp_table}"
		"${TIME}" bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_RESULTS_JSON}"  --clustering_fields="${clustering}" --table "${bq_tmp_table}"

		# upload values into the temporary table
		echo "bq load --project_id=${GCP_PROJECT_ID} --source_format=PARQUET ${bq_tmp_table} ${path}"
		"${TIME}" bq load --project_id="${GCP_PROJECT_ID}" --source_format=PARQUET "${bq_tmp_table}" "${path}"

		echo "building rows from the temporary table and inserting them into ${BQ_PUBLIC_DATASET}.${BQ_PUBLIC_TABLE}"
		convert_and_insert_values "${meas_uuid}" "${meas_md_tmpfile}" "${bq_tmp_table}"

		# delete temporary table after conversion
		bq rm -t -f --project_id="${GCP_PROJECT_ID}" "${bq_tmp_table}"
		echo "table ${bq_tmp_table} deleted"
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
	local start_time
	local MD_FIELDS=()
	local md_fields
	local src_addr

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

	start_time="$(jq -r .start_time "${meas_md_tmpfile}")"
	# order of these fields should match --parameter lines in the bq command below
	MD_FIELDS=(
	    .agent_uuid
	    .agent_parameters.hostname
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

	if  [[ -n "${GCP_INSTANCES:-}" ]]; then
		if ! src_addr="$(grep "${MD_VALUES[1]}" "${GCP_INSTANCES}" | awk '{print $5}')" || [[ -z "${src_addr}" ]]; then
			echo "error: ${MD_VALUES[1]} is not in ${GCP_INSTANCES}"
			return 1
		fi
	else
		src_addr="$(jq -r ".agents[${index}].agent_parameters.external_ipv4_address" "${meas_md_tmpfile}")"
	fi

	"${TIME}" bq query --project_id="${GCP_PROJECT_ID}" \
		--use_legacy_sql=false \
		--parameter="scamper1_table_name_param:STRING:${BQ_PUBLIC_DATASET}.${BQ_PUBLIC_TABLE}" \
		--parameter="measurement_uuid_param:STRING:${meas_uuid}" \
		--parameter="iris_table_name_param:STRING:${bq_tmp_table}" \
		--parameter="start_time_param:STRING:${start_time}" \
		--parameter="agent_uuid_param:STRING:${MD_VALUES[0]}" \
		--parameter="host_param:STRING:${MD_VALUES[1]}" \
		--parameter="src_addr_param:STRING:${src_addr}" \
		--parameter="min_ttl_param:STRING:${MD_VALUES[2]}" \
		--parameter="failure_probability_param:STRING:${MD_VALUES[3]}" \
		< "${TABLE_CONVERSION_QUERY}"
}

parse_args() {
	local args
	local arg

	if ! args="$(getopt \
			--options "c:h" \
			--longoptions "config: help" \
			-- "$@")"; then
		usage 1
	fi
	eval set -- "${args}"

	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-c|--config) CONFIG_FILE="$1"; shift 1;;
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
