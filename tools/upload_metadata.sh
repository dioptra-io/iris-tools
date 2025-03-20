#!/bin/bash

set -eu
set -o pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

CONFIG_FILE="$(git rev-parse --show-toplevel)/conf/settings.conf" # --config

main() {
	local meas_uuid="$1"
	local metadata_string
	local bq_public_table

	echo "sourcing ${CONFIG_FILE}"
	# shellcheck disable=SC1090
	source "${CONFIG_FILE}"

	# check if BQ_PUBLIC_DATASET exist
	echo "checking dataset"
	if ! check_dataset_or_table "${BQ_PUBLIC_DATASET:?unset BQ_PUBLIC_DATASET}"; then
		echo "error: ${BQ_PUBLIC_DATASET} does not exist"
		exit 1
	fi

	if [[ -z "${IRIS_PASSWORD+x}" ]]; then
		irisctl auth login
	else
		echo "irisctl will use IRIS_PASSWORD environment variable when invoked"
	fi
	
	# fetch metadata for selected measurement
	echo "fetching metadata for ${meas_uuid}"
	if ! metadata_string=$(irisctl list --bq --uuid "${meas_uuid}"); then
		echo "error: failed to execute irisctl list --bq --uuid ${meas_uuid}"
		exit 1
	fi

	# upload metadata to BQ_METADATA_TABLE
	bq_public_table="${BQ_PUBLIC_DATASET}"."${BQ_METADATA_TABLE:?unset BQ_METADATA_TABLE}"
	echo "uploading metadata to ${bq_public_table}"
	upload_public_metadata "${bq_public_table}" "${metadata_string}"
}

check_dataset_or_table() {
	local resource="$1"

	if  ! bq show --project_id="${GCP_PROJECT_ID}" "${resource}" > /dev/null 2>&1; then
		return 1
	fi
}

upload_public_metadata() {
	local bq_public_table="$1"
	local metadata_string="$2"
	local metadata_array=()

	# split the metadata_string into an array, using ',' as the delimiter
	IFS=',' read -r -a metadata_array <<< "$(echo "${metadata_string}" | tail -n1)"
	echo "metadata: ${metadata_array[*]}"

	# create BQ_METADATA_TABLE if it doesn't exist
	if check_dataset_or_table "${bq_public_table}"; then
		echo "${bq_public_table} already exists"
	else
		create_table "${SCHEMA_METADATA_JSON}" "${bq_public_table}"
	fi

	# if meas_uuid is already in BQ_METADATA_TABLE, return
	if ! check_uuid_in_metadata "${metadata_array[0]}" "${bq_public_table}"; then
		echo "${metadata_array[0]} is already in ${bq_public_table}"
		return
	fi

	# Insert rows into BQ_METADATA_TABLE.
	# construct the SQL query
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
	# execute the query using bq
	echo "inserting metadata into ${bq_public_table}"
	bq query --use_legacy_sql=false --project_id="${GCP_PROJECT_ID}" "${SQL_QUERY}"
}

create_table() {
	local schema="$1"
	local table="$2"

	echo bq mk --project_id "${GCP_PROJECT_ID}" --schema "${schema}" --table "${table}"
	bq mk --project_id "${GCP_PROJECT_ID}" --schema "${schema}" --table "${table}"
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
	if [[ "${query_result}" != "0" ]]; then
		return 1
	fi
}

main "$@"
