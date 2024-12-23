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
	local bq_dataset_table
	local bq_tmp_table
	local tmpfile
	local first_metadata

    echo "sourcing ${CONFIG_FILE}"
    # shellcheck disable=SC1090
    source "${CONFIG_FILE}"

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
		mv "${output/*saving in /}" "${MEAS_MD_ALL_JSON}"
	fi
	echo "$(jq .count "${MEAS_MD_ALL_JSON}" | uniq) total measurements in ${MEAS_MD_ALL_JSON}"

    # Next, select the measurements that we are interested in.  
	echo creating "${MEAS_MD_METADATA_TXT}"
	irisctl list --tag zeph-gcp --after 2024-09-20 --state finished --state canceled --state agent_failure "${MEAS_MD_ALL_JSON}" | awk '{print $1}' > "${MEAS_MD_METADATA_TXT}"
	echo "$(wc -l < "${MEAS_MD_METADATA_TXT}") selected measurements in ${MEAS_MD_METADATA_TXT}"

	# Create the dataset for inserting metadata if it doesn't exist.
    # shellcheck disable=SC2153
    bq_public_dataset="${BQ_PUBLIC_DATASET}"
    echo verifying "${bq_public_dataset}"
    verify_dataset "${bq_public_dataset}"
        
    # Create the dataset for uploading temporary metadata  if it doesn't exist.
    # shellcheck disable=SC2153
    bq_private_dataset="${BQ_PRIVATE_DATASET}"
    echo verifying "${bq_private_dataset}"
    verify_dataset "${bq_private_dataset}"

	# Create the table for inserting metadata if it doesn't exist.
    bq_dataset_table="${BQ_PUBLIC_DATASET}"."${BQ_TABLE}"
    echo verifying "${bq_dataset_table}"
    verify_table "${bq_dataset_table}"
        
	# Create the table for uploading temporary metadata if it doesn't exist.
    # shellcheck disable=SC2153
	bq_tmp_table="${BQ_PRIVATE_DATASET}"."${BQ_TMP_TABLE}"
    echo verifying "${bq_tmp_table}"
    verify_table "${bq_tmp_table}"

	# Fetch metadata for selected measurements and save them to a temporary file.
	tmpfile="$(mktemp /tmp/metadata.XXXX)"
	echo fetching metadata from "${MEAS_MD_METADATA_TXT}" and saving them to "${tmpfile}"
	while read -r uuid; do
		first_metadata=$(irisctl list --bq --uuid "${uuid}")
		echo "${first_metadata}",caracal >> "${tmpfile}"
	done < "${MEAS_MD_METADATA_TXT}"

	# Load the metadata into the temporary metadata table
	echo loading "${tmpfile}" into "${bq_tmp_table}"
	bq load --project_id="${GCP_PROJECT_ID}" --source_format=CSV "${bq_tmp_table}" "${tmpfile}" 
	
	# Insert rows into metadata table.
	# Construct the SQL query.
	SQL_QUERY="
	INSERT INTO \`${bq_dataset_table}\`
	SELECT
    id,
    start_time,
    duration,
    measurement_status,
    agents,
    succesful_agents,
    iris_version,
    tool,
    IPv4,
    COALESCE(IPv6, FALSE) AS IPv6,  -- Ensure IPv6 defaults to FALSE if NULL
    is_published,
    tags,
    caracal
	FROM \`${bq_tmp_table}\`
	WHERE NOT EXISTS (
    		SELECT 1
    		FROM \`${bq_dataset_table}\`
    		WHERE \`${bq_tmp_table}\`.id = \`${bq_dataset_table}\`.id
	)"

	# Execute the query using bq.
	echo inserting metadata into "${bq_dataset_table}"
	bq query --use_legacy_sql=false --project_id="${GCP_PROJECT_ID}" "${SQL_QUERY}"

	# Delete temporary table after conversion.
	bq rm -t -f --project_id="${GCP_PROJECT_ID}" "${bq_tmp_table}"
	echo temporary table "${bq_tmp_table}" deleted

}

verify_dataset() {
        local dataset="$1"
                
        # Check if dataset exists.
        if ! ${FORCE} && bq show --project_id="${GCP_PROJECT_ID}" "${dataset}" > /dev/null 2>&1; then
                echo "${dataset} already exists"
        else
                # Create the dataset.
                echo bq mk --project_id "${GCP_PROJECT_ID}" --dataset "${dataset}"
                bq mk --project_id "${GCP_PROJECT_ID}" --dataset "${dataset}"
        fi
}

verify_table() {
        local table="$1"
        
        # Check if table exists.
        if ! ${FORCE} && bq show --project_id="${GCP_PROJECT_ID}" "${table}" > /dev/null 2>&1; then
                echo "${table} already exists"
        else
                # Create the table.
                echo bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_METADATA_JSON}" --table "${table}"
                bq mk --project_id "${GCP_PROJECT_ID}" --schema "${SCHEMA_METADATA_JSON}" --clustering_fields start_time --table "${table}"

        fi
}


main "$@"
