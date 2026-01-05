#!/bin/bash

#
# This wrapper script is called by cron to possibly call the
# $PUBLISH_MESUREMENTS script which runs the IPRS publishing pipeline.
#

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC1090,SC2153"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
readonly TOPLEVEL="$(git rev-parse --show-toplevel)"
source "${TOPLEVEL}/tools/common.sh"
source "${TOPLEVEL}/conf/publish_settings.conf"


#
# Set $NOW to your desired datetime to make the publishing pipeline assume
# it's running since then.  Otherwise, set it to the empty string to publish
# the current measurements.  If you set $NOW to a time in the past, you
# should also:
#   - zero out conf/publish_metadata.conf
#   - zero out conf/publish_data.conf files if you are setting
#   - remove $ITERATION file
#
#readonly NOW="2025-03-21T03:00:00"
readonly NOW=""
readonly ITERATION="${TOPLEVEL}/cache/iteration"
readonly VERBOSE=1


cleanup() {
	log_info 1 "removing /tmp/irisctl-clickhouse-* /tmp/export_tables.* /tmp/publish_measurements.sh.*"
	rm -f /tmp/irisctl-clickhouse-* /tmp/export_tables.* /tmp/publish_measurements.sh.*
	log_info 1 "removing /tmp/${PROG_NAME}.$$.*"
	rm -f "/tmp/${PROG_NAME}.$$."*
	log_info 1 "exited"
	log_line
}
trap cleanup EXIT

main() {
	local cmd_line
	local iteration
	local new_now_sec

	log_info 1 "started VERBOSE=${VERBOSE}"

	#
	# Although $PUBLISH_MEASUREMENTS will exit itself if another instance
	# is running, let's not even call it if the lock file exists.
	#
	if [[ -f "${PUBLISH_LOCKFILE}" ]]; then
		log_lock_details
		log_info 1 "an instance of ${PUBLISH_MEASUREMENTS} is running (lock file exists)"
		exit 0
	fi

	#
	# Create the $PUBLISH_MEASUREMENTS command line to run.
	#
	cmd_line=("${PUBLISH_MEASUREMENTS}" -v 4)
	if [[ "${NOW}" != "" ]]; then
		# If $NOW is specified, $ITERATION must already exist unless this is the
		# very first time and needs to be created.
		if [[ ! -f "${ITERATION}" ]]; then
			echo 0 > "${ITERATION}"
		fi
		iteration=$(<"${ITERATION}")
		log_info 1 "iteration=${iteration}"
		new_now_sec=$(date -d "${NOW} UTC + $(( iteration * 6 )) hours" +%s)
		# Make sure we won't be calling $PUBLISH_MEASUREMENTS with a --now value
		# that is in the future (it's an expensive no-op).
		if [[ ${new_now_sec} -ge $(date +%s) ]]; then
			log_info 1 "NOW ($NOW) is in the future"
			exit 0
		fi
		cmd_line=(--now "$(date -d "@$((new_now_sec + iteration * 3600 * 6))" +%Y-%m-%dT%H:%M:%S)")
	fi

	#
	# Set up the environment for $PUBLISH_MEASUREMENTS.
	#
	export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin
	export SHELL=/bin/bash
	if [[ ! -f "${SECRETS_YML}" ]]; then
		log_info 1 "${SECRETS_YML} does not exist"
		exit 1
	fi
	IRIS_PASSWORD="$(sops -d "${SECRETS_YML}" | yq e '.services.production.api[0].pass' -)"
	if [[ "${IRIS_PASSWORD}" == "" ]]; then
		log_info 1 "failed to get IRIS_PASSWORD"
		exit 1
	fi
	export IRIS_PASSWORD

	#
	# Run $PUBLISH_MEASUREMENTS.
	#
	log_info 1 "${cmd_line[*]}"
	if "${cmd_line[@]}"; then
		log_info 1 "${PUBLISH_MEASUREMENTS} exited successfully"
	else
		log_error "${PUBLISH_MEASUREMENTS} did not exit successfully"
	fi
	if [[ "${NOW}" != "" ]]; then
		log_info 1 "incrementing iteration"
		echo $(( ++iteration )) > "${ITERATION}"
	fi

	#
	# Sanity check before exiting.
	#
	if [[ -f "${PUBLISH_LOCKFILE}" ]]; then
		log_lock_details
		log_error "${PUBLISH_MEASUREMENTS} exited but lock file still exists"
		log_info 1 "removing ${PUBLISH_LOCKFILE}"
		rm -f "${PUBLISH_LOCKFILE}"
	fi
}

main "$@"
