#!/usr/bin/env bash

#
# This script provides a convenient way to run check_ratios.sh for
# multiple agents over a specified time period, while checking any desired
# variables.  It simplifies the process of running the script with different
# configurations.
#

set -eu
shellcheck "$0"

IRIS_AGENTS=(
	iris-asia-east1
	iris-asia-northeast1
	iris-asia-south1
	iris-asia-southeast1
	iris-europe-north1
	iris-europe-west6
	iris-me-central1
	iris-southamerica-east1
	iris-us-east4
	iris-us-west4
)


export START_DATE="2024-03-01T00:00:00Z"
export END_DATE="2025-03-01T00:00:00Z"
export TMP_DIR="/md1400-1a/DoS"
ls -las "${TMP_DIR}"
for agent in "${IRIS_AGENTS[@]}"; do
	echo "checking ratios for ${agent}"
	log_file="${TMP_DIR}/${agent}.txt"
	if [[ -f "${log_file}" ]]; then
		echo "${log_file} already exists"
		./check_ratios.sh -y -l "${log_file}" -e 'var2 / var1' packets_received pcap_received
	else
		date
		./check_ratios.sh -a "${agent}" -l "${log_file}" -e 'var2 / var1' packets_received pcap_received
		chmod 444 "${log_file}"
	fi
done
