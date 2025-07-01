#!/bin/bash

#
# This wrapper script is called by cron to possibly call the
# $PUBLISH_MESUREMENTS script which runs the IPRS publishing pipeline.
#

set -euo pipefail
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
readonly TOPLEVEL="$(git rev-parse --show-toplevel)"

readonly SECRETS_YML="${TOPLEVEL}/../infrastructure/secrets.yml"
readonly PUBLISH_MEASUREMENTS="${TOPLEVEL}/tools/publish_measurements.sh"
readonly PUBLISH_LOCKFILE="${TOPLEVEL}/conf/publish.lock"
readonly ITERATION="${TOPLEVEL}/cache/iteration"
readonly LOG_FILE="${TOPLEVEL}/logs/publish_logs.txt"
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


main() {
	local cmd_line
	local iteration
	local new_now_sec

	#
	# Although $PUBLISH_MEASUREMENTS will exit itself if another instance
	# is running, let's not even call it if the lock file exists.
	#
	if [[ -f "${PUBLISH_LOCKFILE}" ]]; then
		log_lock_details
		log "exited because an instance of ${PUBLISH_MEASUREMENTS} is running (lock file exists)"
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
		log "iteration=${iteration}"
		new_now_sec=$(date -d "${NOW} UTC + $(( iteration * 6 )) hours" +%s)
		# Make sure we won't be calling $PUBLISH_MEASUREMENTS with a --now value
		# that is in the future (it's an expensive no-op).
		if [[ ${new_now_sec} -ge $(date +%s) ]]; then
			log "exited because NOW ($NOW) is in the future"
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
		log "exited because ${SECRETS_YML} does not exist"
		exit 1
	fi
	IRIS_PASSWORD="$(sops -d "${SECRETS_YML}" | yq e '.services.production.api[0].pass' -)"
	if [[ "${IRIS_PASSWORD}" == "" ]]; then
		log "exited because it failed to get IRIS_PASSWORD"
		exit 1
	fi
	export IRIS_PASSWORD

	#
	# Run $PUBLISH_MEASUREMENTS.
	#
	log "${cmd_line[*]}"
	if "${cmd_line[@]}"; then
		log "${PUBLISH_MEASUREMENTS} finished"
	else
		log "${PUBLISH_MEASUREMENTS} failed"
	fi
	if [[ "${NOW}" != "" ]]; then
		log "incrementing iteration"
		echo $(( ++iteration )) > "${ITERATION}"
	fi

	#
	# Clean up.
	#
	if [[ -f "${PUBLISH_LOCKFILE}" ]]; then
		log_lock_details
		log "[ERROR] ${PUBLISH_MEASUREMENTS} exited but lock file still exists"
		log "removing ${PUBLISH_LOCKFILE}"
		rm -f "${PUBLISH_LOCKFILE}"
	fi
	log "removing temporary files /tmp/irisctl-clickhouse-* /tmp/export_tables.* /tmp/publish_measurements.sh.*"
	rm -f /tmp/irisctl-clickhouse-* /tmp/export_tables.* /tmp/publish_measurements.sh.*
	log "exited"
}

log() {
	local msg="$1"
	local timestamp

	timestamp=$(date +'%Y-%m-%dT%H:%M:%SZ')
	{
		echo "${timestamp} ${PROG_NAME}: ${msg}"
		if [[ ${msg} == exited* ]]; then
			printf '%*s\n' 72 '' | tr ' ' '-'
		fi
	} >> "${LOG_FILE}"
}

log_lock_details() {
	{ ls -li "${PUBLISH_LOCKFILE}"; cat "${PUBLISH_LOCKFILE}"; } >> "${LOG_FILE}" # debugging support
}

main "$@"
