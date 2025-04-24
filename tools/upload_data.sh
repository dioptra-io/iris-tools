#!/bin/bash

set -euo pipefail
shopt -s nullglob
export SHELLCHECK_OPTS="--exclude=SC1090,SC2064"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
readonly TOPLEVEL="$(git rev-parse --show-toplevel)"
source "${TOPLEVEL}/tools/common.sh"

#
# Global variables to support command line flags and arguments.
#
CONFIG_FILE="${TOPLEVEL}/conf/settings.conf"	# --config
DRY_RUN=false                                   # --dry-run
VERBOSE=1					# --verbose
POSITIONAL_ARGS=()				# <uuid>...


#
# Print usage message and exit.
#
usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} --help
	${PROG_NAME} [-c <config>] [-v <n>] <uuid>...
	-c, --config    configuration file (default ${CONFIG_FILE})
	-n, --dry-run	enable the dry-run mode
	-h, --help      print help message and exit
	-v, --verbose	set the verbosity level (default: ${VERBOSE})

	uuid: measurement uuid
EOF
	exit "${exit_code}"
}

main() {
	local bq_metadata_table
	local resource
	local bq_public_table
	local meas_md_tmpfile
	local meas_uuid
	local table_prefix
	local query

	parse_cmdline_and_conf "$@"

	# Check if public and private datasets and the metadata table exist.
	bq_metadata_table="${BQ_PUBLIC_DATASET}.${BQ_METADATA_TABLE:?unset BQ_METADATA_TABLE}"
	log_info 1 "checking datasets and metadata table ${BQ_PUBLIC_DATASET} ${BQ_PRIVATE_DATASET} ${bq_metadata_table}"
	for resource in "${BQ_PUBLIC_DATASET}" "${BQ_PRIVATE_DATASET}" "${bq_metadata_table}"; do
		if ! exists_dataset_or_table "${resource}"; then
			fatal "${resource} does not exist"
		fi
	done

	# Create the public table for inserting measurement data in scamper1 format if it doesn't exist.
	bq_public_table="${BQ_PUBLIC_DATASET}"."${BQ_PUBLIC_TABLE:?unset BQ_PUBLIC_TABLE}"
	log_info 1 "checking the public data table ${bq_public_table}"
	if ! exists_dataset_or_table "${bq_public_table}"; then
		# Create the $BQ_PUBLIC_TABLE table with the schema file.
		log_info 1 "bq mk --project_id ${GCP_PROJECT_ID} --schema ${SCHEMA_SCAMPER1_JSON} --clustering_fields id --time_partitioning_field date --time_partitioning_type DAY --table ${bq_public_table}"
		if ! ${DRY_RUN}; then
			"${TIME}" bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_SCAMPER1_JSON}" --clustering_fields "id" --time_partitioning_field "date" --time_partitioning_type DAY --table "${bq_public_table}"
		fi
	fi

	# If $IRIS_PASSWORD is not set, authtenticate irisctl now by prompting the user.
	if [[ -z "${IRIS_PASSWORD+x}" ]]; then
		irisctl auth login
	fi

	log_info 2 "tables to upload: ${TABLES_TO_UPLOAD[*]}"
        meas_md_tmpfile="$(mktemp "/tmp/${PROG_NAME}.XXXX")"
	trap "rm -f ${meas_md_tmpfile}" EXIT
	for meas_uuid in "${POSITIONAL_ARGS[@]}"; do
		# First check if meas_uuid is in $BQ_METADATA_TABLE.
		log_info 1 "checking ${meas_uuid} exists in ${bq_metadata_table}"
		if ! check_uuid_in_metadata "${meas_uuid}" "${bq_metadata_table}"; then
			fatal "${meas_uuid} does not exist in ${bq_metadata_table}"
		fi

		# Then  get the metadata of this measurement.
		log_info 1 "irisctl meas --uuid ${meas_uuid} -o > ${meas_md_tmpfile}"
		irisctl meas --uuid "${meas_uuid}" -o > "${meas_md_tmpfile}"

		# Now upload this measurement's tables.
		for table_prefix in "${TABLES_TO_UPLOAD[@]}"; do
			if [[ "${table_prefix}" != "results__" ]]; then
				fatal "do not have query for uploading ${table_prefix} tables"
			fi
			log_info 1 "uploading ${meas_uuid} ${table_prefix}" tables
			upload_data "${meas_uuid}" "${meas_md_tmpfile}" "${table_prefix}"
			echo
		done

		# Finally, update the is_published field in $BQ_METADATA_TABLE.
		query="${UPDATE_IS_PUBLISHED//\$\{meas_uuid\}/$meas_uuid}"
		log_info 1 "bq query --use_legacy_sql=false --project_id=${GCP_PROJECT_ID} ${query}"
		if ! ${DRY_RUN}; then
			bq query --use_legacy_sql=false --project_id="${GCP_PROJECT_ID}" "${query}"
		fi
	done
}

#
# Return true (0) if the resource exists. Return failure (1) otherwise.
#
exists_dataset_or_table() {
	local resource="$1"

	# Check if $resource exists.
	log_info 1 bq show --project_id="${GCP_PROJECT_ID}" "${resource}"
	if ${DRY_RUN}; then
		return 0 # pretend it exists
	fi
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
	log_info 1 "bq query --use_legacy_sql=false --project_id=${GCP_PROJECT_ID} --format=csv ${query}"
	if ${DRY_RUN}; then
		return 0 # pretend it exists
	fi
	query_result=$(bq query --use_legacy_sql=false --project_id="${GCP_PROJECT_ID}" --format=csv "${query}" | tail -n 1)
	log_info 1 "${query_result}"
	if [[ "${query_result}" == "0" ]]; then
		return 1
	fi
}

upload_data() {
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
		echo "no ${EXPORT_FORMAT} files in ${EXPORT_DIR} for ${meas_uuid}" # XXX isn't this a fatal error?
		return
	fi
	echo "${SCHEMA_RESULTS}" > "${SCHEMA_RESULTS_JSON}"
	for path in "${EXPORT_DIR}"/*"${meas_uuid//-/_}"*."${EXPORT_FORMAT}"; do
		filename="${path##*/}" # remove everything up to the last slash
		bq_tmp_table="${BQ_PRIVATE_DATASET}.${filename%.${EXPORT_FORMAT}}"

		# If $bq_tmp_table table already exists, it might be empty; so delete it to ensure a fresh start before reloading data.
		if exists_dataset_or_table "${bq_tmp_table}"; then
			log_info 1 "${bq_tmp_table} already exists"
			log_info 1 "bq rm -t -f --project_id=${GCP_PROJECT_ID} ${bq_tmp_table}"
			if ! ${DRY_RUN}; then
				bq rm -t -f --project_id="${GCP_PROJECT_ID}" "${bq_tmp_table}"
			fi
			log_info 1 "temporary table ${bq_tmp_table} deleted"
		fi

		# Create the temporary table with the schema file.
		log_info 1 "bq mk --project_id ${GCP_PROJECT_ID} --schema ${SCHEMA_RESULTS_JSON} --clustering_fields=${clustering} --table ${bq_tmp_table}"
		"${TIME}" bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_RESULTS_JSON}"  --clustering_fields="${clustering}" --table "${bq_tmp_table}"

		# Upload values into the temporary table.
		log_info 1 "bq load --project_id=${GCP_PROJECT_ID} --source_format=PARQUET ${bq_tmp_table} ${path}"
		"${TIME}" bq load --project_id="${GCP_PROJECT_ID}" --source_format=PARQUET "${bq_tmp_table}" "${path}"

		log_info 1 "building rows from the temporary table and inserting them into ${BQ_PUBLIC_DATASET}.${BQ_PUBLIC_TABLE}"
		convert_and_insert_values "${meas_uuid}" "${meas_md_tmpfile}" "${bq_tmp_table}"

		# Delete temporary table after conversion.
		log_info 1 "bq rm -t -f --project_id=${GCP_PROJECT_ID} ${bq_tmp_table}"
		if ! ${DRY_RUN}; then
			bq rm -t -f --project_id="${GCP_PROJECT_ID}" "${bq_tmp_table}"
		fi
		log_info 1 "temporary table ${bq_tmp_table} deleted"

		# Delete table from $EXPORT_DIR after conversion.
		rm -f "${path}"
  		log_info 1 "${path} removed"
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

	# Get the metadata of this measurement.
	agent="$(echo "${bq_tmp_table}" | sed -e 's/.*__\(........_...._...._...._............\)/\1/' | tr '_' '-')"
	index="$(jq --arg agent "$agent" '[.agents[].agent_uuid] | index($agent)' "${meas_md_tmpfile}")"
	# Sanity check.
	for v in agent index; do
		if [[ "${!v}" == "" ]]; then
			fatal "failed to parse ${v}"
		fi
	done

	start_time="$(jq -r .start_time "${meas_md_tmpfile}")"
	# Order of these fields should match --parameter lines in the bq command below.
	MD_FIELDS=(
	    .agent_uuid
	    .agent_parameters.hostname
	    .agent_parameters.min_ttl
	    .tool_parameters.failure_probability
	)
	md_fields="$(IFS=,; echo "${MD_FIELDS[*]}")"
	read -r -a MD_VALUES <<< "$(jq -r ".agents[${index}] | [${md_fields}] | @tsv" "${meas_md_tmpfile}")"
	# Sanity check.
	if [[ ${#MD_VALUES[@]} -ne ${#MD_FIELDS[@]} ]]; then
		fatal "expected to parse ${#MD_FIELDS[@]} values, got ${#MD_VALUES[@]} values"
	fi

	# Get the external IPv4 address of the agent.
	if  [[ -n "${GCP_INSTANCES}" ]]; then
		if ! grep -q "${MD_VALUES[1]}" "${GCP_INSTANCES}"; then
			fatal "${MD_VALUES[1]} is not in ${GCP_INSTANCES}"
		fi
		src_addr="$(awk -v agent="${MD_VALUES[1]}" '$0 ~ agent {print $5}' "${GCP_INSTANCES}")"
	else
		src_addr="$(jq -r ".agents[${index}].agent_parameters.external_ipv4_address" "${meas_md_tmpfile}")"
	fi
	if [[ ! "${src_addr}" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]; then
		fatal "${src_addr} is not a valid external IPv4 address"
	fi

	"${TIME}" bq query --location=EU --project_id="${GCP_PROJECT_ID}" \
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

#
# Parse the command line and the configuration file.
#
parse_cmdline_and_conf() {
	local args
	local arg

	if ! args="$(getopt \
			--options "c:hnv:" \
			--longoptions "config: dry-run help verbose:" \
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
		-h|--help) usage 0;;
		-v|--verbose) VERBOSE="$1"; shift 1;;
		--) break;;
		*) fatal "panic: error parsing arg=${arg}";;
		esac
	done
	POSITIONAL_ARGS=("$@")

	log_info 1 "sourcing ${CONFIG_FILE}"
	source "${CONFIG_FILE}"
	log_info 1 "sourcing ${IRIS_ENV}"
	source "${IRIS_ENV}"

	if [[ ${#POSITIONAL_ARGS[@]} -lt 1 ]]; then
		fatal "specify at least one measurement uuid"
	fi

	# Check if $GCP_INSTANCES is provided and the corresponding file exists.
	if [[ -n "${GCP_INSTANCES}" && ! -f "${GCP_INSTANCES}" ]]; then
		 fatal "${GCP_INSTANCES} does not exist"
	fi
}

main "$@"
