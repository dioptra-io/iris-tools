#!/bin/bash

set -eu
set -o pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

CONFIG_FILE="$(git rev-parse --show-toplevel)/conf/metadata_table.conf" # --config
FORCE=false # --force

main() {
	local output
	local bq_public_dataset
	local bq_private_dataset
	local output_path
	local flags
	local irisctl_cmd
	local bq_public_table
	local bq_tmp_table
	local tmpfile

	echo "sourcing ${CONFIG_FILE}"
	# shellcheck disable=SC1090
	source "${CONFIG_FILE}"

	# Check if the public dataset exists.
	# shellcheck disable=SC2153
	bq_public_dataset="${BQ_PUBLIC_DATASET}"
	echo verifying "${bq_public_dataset}"
	check_dataset "${bq_public_dataset}"

	# Check if the private dataset exists.
	# shellcheck disable=SC2153
	bq_private_dataset="${BQ_PRIVATE_DATASET}"
	echo verifying "${bq_private_dataset}"
	check_dataset "${bq_private_dataset}"

	if [[ -z "${IRIS_PASSWORD+x}" ]]; then
		irisctl auth login
	else
		echo "irisctl will use IRIS_PASSWORD environment variable when invoked"
	fi

	# First, see if the measurement metadata file that includes all
	# measurements needs to be created.  This file will be passed to
	# `irisctl` in subsequent calls so speed up its execution.
	if ! ${FORCE} && [[ -f "${MEAS_MD_ALL_JSON}" ]]; then
		echo using existing "${MEAS_MD_ALL_JSON}"
	else
		echo creating "${MEAS_MD_ALL_JSON}"
		if ! output="$(irisctl meas --all-users 2>&1 > /dev/null)"; then
			echo "${output}"
			exit 1
		fi
		output_path=$(echo "$output" | grep "saving in" | awk '{print $3}')
		mv "${output_path}" "${MEAS_MD_ALL_JSON}"
	fi
	echo "$(jq .count "${MEAS_MD_ALL_JSON}" | uniq) total measurements in ${MEAS_MD_ALL_JSON}"

	# Next, select the measurements that we are interested in.
	echo creating "${MEAS_MD_METADATA_TXT}"
	# Add flags for filtering
 	flags=()
	[ ${#MEAS_TAG[@]} -gt 0 ] && for tag in "${MEAS_TAG[@]}"; do flags+=("--tag" "$tag"); done
	[ -n "${MEAS_BEFORE}" ] && flags+=("--before" "${MEAS_BEFORE}")
	[ -n "${MEAS_AFTER}" ] && flags+=("--after" "${MEAS_AFTER}")
	[ ${#MEAS_STATES[@]} -gt 0 ] && for state in "${MEAS_STATES[@]}"; do flags+=("--state" "$state"); done
	# Debugging: Check if the flags array is empty
	if [[ ${#flags[@]} -eq 0 ]]; then
    echo "No flags provided. Running without flags."
	else
    echo "Flags provided: ${flags[*]}"
	fi
	# Construct the command
	if [[ ${#flags[@]} -eq 0 ]]; then
		irisctl_cmd=("irisctl" "list" "$MEAS_MD_ALL_JSON")  # No flags
	else
		irisctl_cmd=("irisctl" "list" "${flags[*]}" "${MEAS_MD_ALL_JSON}")  # With flags
	fi	
	echo "${irisctl_cmd[*]}"
	eval "${irisctl_cmd[*]}" | awk '{print $1}' > "${MEAS_MD_METADATA_TXT}"
	echo "$(wc -l < "${MEAS_MD_METADATA_TXT}") selected measurements in ${MEAS_MD_METADATA_TXT}"

	# Fetch metadata for selected measurements and save them to a temporary file.
	tmpfile="$(mktemp /tmp/metadata.XXXX)"
	echo fetching metadata from "${MEAS_MD_METADATA_TXT}" and saving them to "${tmpfile}"
	while read -r uuid; do
		irisctl list --bq --uuid "${uuid}" >> "${tmpfile}"
	done < "${MEAS_MD_METADATA_TXT}"
 
    # Create the public table for inserting metadata if it doesn't exist.
	bq_public_table="${BQ_PUBLIC_DATASET}"."${BQ_TABLE}"
	if ! ${FORCE} && bq show --project_id="${GCP_PROJECT_ID}" "${bq_public_table}" > /dev/null 2>&1; then
		echo "${bq_public_table} already exists"
	else
		# Create the table.
		echo bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_METADATA_JSON}" --table "${bq_public_table}"
		bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_METADATA_JSON}" --table "${bq_public_table}"

	fi
		
	# Create the table for uploading temporary metadata.
	# If it already exist, delete the existing table to prevent duplicate rows insertion
	# shellcheck disable=SC2153
	bq_tmp_table="${BQ_PRIVATE_DATASET}"."${BQ_TMP_TABLE}"
	if ! ${FORCE} && bq show --project_id="${GCP_PROJECT_ID}" "${bq_tmp_table}" > /dev/null 2>&1; then
		echo "${bq_tmp_table} already exists"
		bq rm -t -f --project_id="${GCP_PROJECT_ID}" "${bq_tmp_table}"
		echo temporary table "${bq_tmp_table}" deleted
	fi
	echo bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_TMP_METADATA_JSON}" --table "${bq_tmp_table}"
	bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_TMP_METADATA_JSON}" --table "${bq_tmp_table}"

	# Load the metadata into the temporary metadata table
	echo loading "${tmpfile}" into "${bq_tmp_table}"
	bq load --project_id="${GCP_PROJECT_ID}" --source_format=CSV "${bq_tmp_table}" "${tmpfile}"
	
	# Insert rows into metadata table.
	# Construct the SQL query.
	SQL_QUERY="
INSERT INTO \`${bq_public_table}\`
SELECT
  id 		 								   AS id,
  start_time 								   AS start_time,
  TIMESTAMP_DIFF(end_time, start_time, SECOND) AS duration,
  snapshot_status 							   AS snapshot_status,
  ${SNAPSHOT_LABELS} 						   AS snapshot_labels,
  num_agents 								   AS num_agents,
  num_succesful_agents 						   AS num_succesful_agents,
  STRUCT(
    IF('${IRIS_VERSION}' = '', NULL, '${IRIS_VERSION}') 				  AS iris,  -- If empty, set to NULL
    IF('${DIAMOND_MINER_VERSION}' = '', NULL, '${DIAMOND_MINER_VERSION}') AS diamond_miner,  -- If empty, set to NULL
    IF('${ZEPH_VERSION}' = '', NULL, '${ZEPH_VERSION}') 				  AS zeph,  -- If empty, set to NULL
    IF('${CARACAL_VERSION}' = '', NULL, '${CARACAL_VERSION}') 			  AS caracal,  -- If empty, set to NULL
    IF('${PARSER_VERSION}' = '', NULL, '${PARSER_VERSION}') 			  AS parser -- If empty, set to NULL
  ) 										   AS sw_versions,
  ${IPV4} 									   AS IPv4,  
  ${IPV6} 									   AS IPv6,
  ${WHERE_PUBLISHED} 						   AS where_published
FROM \`${bq_tmp_table}\` tmp_metadata
WHERE NOT EXISTS (
  SELECT 1
  FROM \`${bq_public_table}\` metadata
  WHERE tmp_metadata.id = metadata.id
)
"
	# Execute the query using bq.
	echo inserting metadata into "${bq_public_table}"
	bq query --use_legacy_sql=false --project_id="${GCP_PROJECT_ID}" "${SQL_QUERY}"

	# Delete temporary table after conversion.
	bq rm -t -f --project_id="${GCP_PROJECT_ID}" "${bq_tmp_table}"
	echo temporary table "${bq_tmp_table}" deleted

}

check_dataset() {
	local dataset="$1"

	# Check if dataset exists.
	if  ! bq show --project_id="${GCP_PROJECT_ID}" "${dataset}" > /dev/null 2>&1; then
		echo "error: ${dataset} does not exist"
		exit 1
	fi
}

main "$@"
