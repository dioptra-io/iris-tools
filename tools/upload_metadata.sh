#!/bin/bash

set -eu
set -o pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

CONFIG_FILE="$(git rev-parse --show-toplevel)/conf/metadata_table.conf" # --config
FORCE=false # --force

main() {
	local datasets
	local tmpfile
	local bq_tmp_table
	local bq_public_table

	echo "sourcing ${CONFIG_FILE}"
	# shellcheck disable=SC1090
	source "${CONFIG_FILE}"

	# check if the datasets exist
	echo "checking datasets"
	datasets=("${BQ_PUBLIC_DATASET}" "${BQ_PRIVATE_DATASET}")
	for dataset in "${datasets[@]}"; do
		if ! check_dataset_or_table "${dataset}"; then
			echo "error: ${dataset} does not exist"
			exit 1
		fi
	done

	if [[ -z "${IRIS_PASSWORD+x}" ]]; then
		irisctl auth login
	else
		echo "irisctl will use IRIS_PASSWORD environment variable when invoked"
	fi

	# First, create the measurement metadata file that includes all
	# measurements if it does not esists. This file will be passed to
	# `irisctl` in subsequent calls so speed up its execution.
	if ! ensure_file_exists; then
		echo "error: ${MEAS_MD_ALL_JSON} could not be created"
		exit 1
	fi
	# count measurements in MEAS_MD_ALL_JSON
	echo "$(jq .count "${MEAS_MD_ALL_JSON}" | uniq) total measurements in ${MEAS_MD_ALL_JSON}"

	# Next, select the measurements that we are interested in.
	echo "selecting measurements"
	select_measurements
	# count measurements in MEAS_MD_METADATA_TXT
	echo "$(wc -l < "${MEAS_MD_METADATA_TXT}") selected measurements in ${MEAS_MD_METADATA_TXT}"

	# Then, fetch metadata for selected measurements and save them to a temporary file.
	tmpfile="$(mktemp /tmp/metadata.XXXX)"
	echo "fetching metadata from ${MEAS_MD_METADATA_TXT} and saving them to ${tmpfile}"
	fetch_metadata "${tmpfile}"

	# Upload metadata to temporary metadata table.
	bq_tmp_table="${BQ_PRIVATE_DATASET}"."${BQ_TMP_TABLE:?unset BQ_TMP_TABLE}"
	echo "uploading metadata to ${bq_tmp_table} table"
	upload_tmp_metadata "${tmpfile}" "${bq_tmp_table}"

	# Upload metadata to BQ_PUBLIC_TABLE.
	bq_public_table="${BQ_PUBLIC_DATASET}"."${BQ_PUBLIC_TABLE:?unset BQ_PUBLIC_TABLE}"
	echo "uploading metadata to ${bq_public_table}"
	upload_public_metadata "${bq_public_table}" "${bq_tmp_table}"

	# Delete temporary table after uploading.
	bq rm -t -f --project_id="${GCP_PROJECT_ID}" "${BQ_PRIVATE_DATASET}"."${BQ_TMP_TABLE}"
	echo "temporary table ${BQ_PRIVATE_DATASET}.${BQ_TMP_TABLE} deleted"
}

check_dataset_or_table() {
	local resource="$1"

	if  ! bq show --project_id="${GCP_PROJECT_ID}" "${resource}" > /dev/null 2>&1; then
		return 1
	fi
}

ensure_file_exists() {
	local output
	local output_path

	if ! ${FORCE} && [[ -f "${MEAS_MD_ALL_JSON}" ]]; then
		echo "using existing ${MEAS_MD_ALL_JSON}"
	else
		echo "creating ${MEAS_MD_ALL_JSON}"
		if ! output="$(irisctl meas --all-users 2>&1 > /dev/null)"; then
			return 1
		fi
		output_path=$(echo "$output" | awk '/saving in/ {print $3}')
		mv "${output_path}" "${MEAS_MD_ALL_JSON}"
	fi
}

select_measurements() {
	local irisctl_cmd=("irisctl" "list")

	# add flags to the command line for selecing measurements
	[[ ${#MEAS_TAG[@]} -gt 0 ]] && for tag in "${MEAS_TAG[@]}"; do irisctl_cmd+=("--tag" "$tag"); done
	[[ -n "${MEAS_BEFORE}" ]] && irisctl_cmd+=("--before" "${MEAS_BEFORE}")
	[[ -n "${MEAS_AFTER}" ]] && irisctl_cmd+=("--after" "${MEAS_AFTER}")
	[[ ${#MEAS_STATES[@]} -gt 0 ]] && for state in "${MEAS_STATES[@]}"; do irisctl_cmd+=("--state" "$state"); done
	irisctl_cmd+=("${MEAS_MD_ALL_JSON}")
	echo "creating ${MEAS_MD_METADATA_TXT}"
	echo "${irisctl_cmd[*]} | awk '{print \$1}' > ${MEAS_MD_METADATA_TXT}"
	"${irisctl_cmd[@]}" | awk '{print $1}' > "${MEAS_MD_METADATA_TXT}"
}

fetch_metadata() {
	local tmpfile="$1"

	while read -r uuid; do
		irisctl list --bq --uuid "${uuid}" >> "${tmpfile}"
	done < "${MEAS_MD_METADATA_TXT}"
}

upload_tmp_metadata() {
	local tmpfile="$1"
	local bq_tmp_table="$2"

	# Create the table for uploading temporary metadata.
	# If it already exist, delete the existing table to prevent duplicate rows insertion.
	if check_dataset_or_table "${bq_tmp_table}"; then
		echo "${bq_tmp_table} already exists"
		bq rm -t -f --project_id="${GCP_PROJECT_ID}" "${bq_tmp_table}"
		echo "temporary table ${bq_tmp_table} deleted"
	fi
	create_table "${SCHEMA_TMP_METADATA_JSON}" "${bq_tmp_table}"

	# Load the metadata into the temporary metadata table.
	echo "loading ${tmpfile} into ${bq_tmp_table}"
	bq load --replace --project_id="${GCP_PROJECT_ID}" --source_format=CSV "${bq_tmp_table}" "${tmpfile}"
}

create_table() {
	local schema="$1"
	local table="$2"

	echo bq mk --project_id "${GCP_PROJECT_ID}" --schema "${schema}" --table "${table}"
	bq mk --project_id "${GCP_PROJECT_ID}" --schema "${schema}" --table "${table}"
}

upload_public_metadata() {
	local bq_public_table="$1"
	local bq_tmp_table="$2"

	# Create the public table for inserting metadata if it doesn't exist.
	if check_dataset_or_table "${bq_public_table}"; then
		echo "${bq_public_table} already exists"
	else
		create_table "${SCHEMA_METADATA_JSON}" "${bq_public_table}"
	fi

	# Insert rows into metadata table.
	# construct the SQL query
	SQL_QUERY="
INSERT INTO \`${bq_public_table}\`
SELECT
  id AS id,
  start_time AS start_time,
  TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration,
  snapshot_status AS snapshot_status,
  ${SNAPSHOT_LABELS} AS snapshot_labels,
  num_agents AS num_agents,
  num_succesful_agents AS num_succesful_agents,
  STRUCT(
    IF('${IRIS_VERSION}' = '', NULL, '${IRIS_VERSION}') AS iris,  -- If empty, set to NULL
    IF('${DIAMOND_MINER_VERSION}' = '', NULL, '${DIAMOND_MINER_VERSION}') AS diamond_miner,  -- If empty, set to NULL
    IF('${ZEPH_VERSION}' = '', NULL, '${ZEPH_VERSION}') AS zeph,  -- If empty, set to NULL
    IF('${CARACAL_VERSION}' = '', NULL, '${CARACAL_VERSION}') AS caracal,  -- If empty, set to NULL
    IF('${PARSER_VERSION}' = '', NULL, '${PARSER_VERSION}') AS parser -- If empty, set to NULL
  ) AS sw_versions,
  ${IPV4} AS IPv4,
  ${IPV6} AS IPv6,
  ${WHERE_PUBLISHED} AS where_published
FROM \`${bq_tmp_table}\` tmp_metadata
WHERE NOT EXISTS (
  SELECT 1
  FROM \`${bq_public_table}\` metadata
  WHERE tmp_metadata.id = metadata.id
)
"
	# execute the query using bq
	echo "inserting metadata into ${bq_public_table}"
	bq query --use_legacy_sql=false --project_id="${GCP_PROJECT_ID}" "${SQL_QUERY}"
}

main "$@"
