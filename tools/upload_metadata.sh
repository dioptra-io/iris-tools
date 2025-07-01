#!/bin/bash

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC1090"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
readonly TOPLEVEL="$(git rev-parse --show-toplevel)"
source "${TOPLEVEL}/tools/common.sh"

#
# Global variables to support command line flags and arguments.
#
CONFIG_FILE="${TOPLEVEL}/conf/publish_settings.conf"	# --config
DRY_RUN=false						# --dry-run
VERBOSE=1						# --verbose
POSITIONAL_ARGS=()					# <uuid>...


#
# Print usage message and exit.
#
usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} --help
	${PROG_NAME} [-c <config>] [-v <n>] <uuid>...
	-c, --config	configuration file (default ${CONFIG_FILE})
	-h, --help	print help message and exit
	-v, --verbose	set the verbosity level (default: ${VERBOSE})

	uuid: measurement uuid
EOF
	exit "${exit_code}"
}

main() {
	local meas_uuid
	local metadata_string
	local bq_public_table

	parse_cmdline_and_conf "$@"

	# Check if $BQ_PUBLIC_DATASET exists.
	log_info 1 "checking public dataset ${BQ_PUBLIC_DATASET}"
	if ! check_dataset_or_table "${BQ_PUBLIC_DATASET:?unset BQ_PUBLIC_DATASET}"; then
		fatal "${BQ_PUBLIC_DATASET} does not exist"
	fi

	# If $IRIS_PASSWORD is not set, authtenticate irisctl now by prompting the user.
	if [[ -z "${IRIS_PASSWORD+x}" ]]; then
		irisctl auth login
	fi

	for meas_uuid in "${POSITIONAL_ARGS[@]}"; do
		# Fetch metadata of $meas_uuid.
		log_info 1 "fetching metadata for ${meas_uuid}"
		log_info 2 irisctl list --bq --uuid "${meas_uuid}"
		# XXX irisctl may be broken because it doesn't fail when the uuid is invalid
		if ! metadata_string=$(irisctl list --bq --uuid "${meas_uuid}"); then
			fatal "failed to execute irisctl list --bq --uuid ${meas_uuid}"
		fi
		if [[ "${metadata_string}" != "${meas_uuid}"* ]]; then
			fatal "metadata string does not look right: ${metadata_string}"
		fi
		log_info 1 "metadata_string=${metadata_string}"

		# Upload metadata to $BQ_METADATA_TABLE.
		bq_public_table="${BQ_PUBLIC_DATASET}"."${BQ_METADATA_TABLE:?unset BQ_METADATA_TABLE}"
		log_info 1 "uploading metadata to ${bq_public_table}"
		upload_public_metadata "${bq_public_table}" "${metadata_string}"
	done
}

#
# Return true (0) if the dataset or table exists. Return false (1) otherwise.
#
check_dataset_or_table() {
	local resource="$1"

	if ${DRY_RUN}; then
		log_info 1 bq show --project_id="${GCP_PROJECT_ID}" "${resource}"
		return 0 # pretend it exists
	fi

	if  ! bq show --project_id="${GCP_PROJECT_ID}" "${resource}" > /dev/null 2>&1; then
		return 1
	fi
}

upload_public_metadata() {
	local bq_public_table="$1"
	local metadata_string="$2"
	local metadata_array=()

	# Split the metadata_string into an array, using ',' as the delimiter.
	IFS=',' read -r -a metadata_array <<< "$(echo "${metadata_string}" | tail -n1)"
	log_info 1 "metadata: ${metadata_array[*]}"

	# Create BQ_METADATA_TABLE if it doesn't exist.
	if check_dataset_or_table "${bq_public_table}"; then
		log_info 1 "public table ${bq_public_table} already exists"
	else
		create_table "${SCHEMA_METADATA_JSON}" "${bq_public_table}"
	fi

	# If $meas_uuid is already in BQ_METADATA_TABLE, return.
	if is_uuid_in_metadata "${metadata_array[0]}" "${bq_public_table}"; then
		log_info 1 "${metadata_array[0]} already exists in ${bq_public_table}"
		return
	fi

	# Insert rows into BQ_METADATA_TABLE.
	# Construct the SQL query.
	SQL_QUERY="
INSERT INTO \`${bq_public_table}\`(
  id,
  start_time,
  duration,
  snapshot_status,
  snapshot_labels,
  num_agents,
  num_succesful_agents,
  sw_versions,
  IPv4,
  IPv6,
  is_published
)
VALUES(
  '${metadata_array[0]}',
  TIMESTAMP('${metadata_array[1]}'),
  TIMESTAMP_DIFF('${metadata_array[2]}', '${metadata_array[1]}', SECOND),
 '${metadata_array[3]}',
  ${SNAPSHOT_LABELS},
  CAST('${metadata_array[4]}' AS INT64),
  CAST('${metadata_array[5]}' AS INT64),
  STRUCT(
    COALESCE(NULLIF('${IRIS_VERSION}', ''), NULL) AS iris,
    COALESCE(NULLIF('${DIAMOND_MINER_VERSION}', ''), NULL) AS diamond_miner,
    COALESCE(NULLIF('${ZEPH_VERSION}', ''), NULL) AS zeph,
    COALESCE(NULLIF('${CARACAL_VERSION}', ''), NULL) AS caracal,
    COALESCE(NULLIF('${PARSER_VERSION}', ''), NULL) AS parser
  ),
  ${IPV4},
  ${IPV6},
  False);
"
	if ${DRY_RUN}; then
		log_info 1 bq query --use_legacy_sql=false --project_id="${GCP_PROJECT_ID}" "${SQL_QUERY}"
	else
		# execute the query using bq
		log_info 1 "inserting metadata into ${bq_public_table}"
		bq query --use_legacy_sql=false --project_id="${GCP_PROJECT_ID}" "${SQL_QUERY}"
	fi
}

create_table() {
	local schema="$1"
	local table="$2"

	log_info 1 bq mk --project_id "${GCP_PROJECT_ID}" --schema "${schema}" --table "${table}"
	bq mk --project_id "${GCP_PROJECT_ID}" --schema "${schema}" --table "${table}"
}


#
# Return true (0) if $meas_uuid already exists in the metadata table. Return false (1) otherwise.
#
is_uuid_in_metadata() {
	local meas_uuid="$1"
	local bq_metadata_table="$2"
	local query
	local query_result

	log_info 1 "checking if ${meas_uuid} already exists in ${bq_metadata_table}"
	query="SELECT COUNT(*) FROM \`${bq_metadata_table}\` WHERE id = '${meas_uuid}'"
	if ${DRY_RUN}; then
		log_info 1 bq query --use_legacy_sql=false --project_id="${GCP_PROJECT_ID}" --format=csv "${query}"
		return 1 # pretend it doesn't exist
	fi
	log_info 1 bq query --use_legacy_sql=false --project_id="${GCP_PROJECT_ID}" --format=csv "${query}"
	query_result=$(bq query --use_legacy_sql=false --project_id="${GCP_PROJECT_ID}" --format=csv "${query}" | tail -n 1)
	log_info 1 "${query_result}"
	if ! [[ "${query_result}" =~ ^[0-9]+$ ]]; then
		fatal "${query_result} is not an integer"
	fi
	if [[ "${query_result}" == "0" ]]; then
		return 1
	fi
	return 0
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

	# Parse flags.
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

	if [[ ${#POSITIONAL_ARGS[@]} -lt 1 ]]; then
		echo "specify at least one measurement uuid"
		usage 1
	fi
}

main "$@"
