#!/usr/bin/env bash

set -eu

source "${TOPLEVEL}/conf/common_settings.sh"

#
# This function is called by cron wrappers to set up the environment
# before calling the scripts.  When cron starts a wrapper, PATH is
# /usr/bin:/bin and SHELL is /bin/sh.
#   - PATH
#   - SHELL
#   - IRIS_PASSWORD (for irisctl and iris-exporter-legacy)
#   - CLICKHOUSE_PASSWORD (for exporting and iris-exporter-legacy)
#
setup_environment() {
	export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin
	export SHELL=/bin/bash

	if [[ ! -f "${SECRETS_YML}" ]]; then
		log_fatal "${SECRETS_YML} does not exist or is not a regular file"
	fi

        if [[ -z "${IRIS_USERNAME:-}" ]]; then
                log_fatal "IRIS_USERNAME is unset or empty"
        fi
	IRIS_PASSWORD="$(sops -d "${SECRETS_YML}" | yq e ".services.production.api[] | select(.user == \"${IRIS_USERNAME}\") | .pass" -)"
	if [[ -z "${IRIS_PASSWORD}" ]]; then
		log_fatal "failed to get IRIS_PASSWORD"
	fi
	export IRIS_PASSWORD

        if [[ -z "${CLICKHOUSE_USERNAME:-}" ]]; then
                log_fatal "CLICKHOUSE_USERNAME is unset or empty"
        fi
	CLICKHOUSE_PASSWORD="$(sops -d "${SECRETS_YML}" | yq e ".services.production.clickhouse[] | select(.user == \"${CLICKHOUSE_USERNAME}\") | .pass" -)"
	if [[ -z "${CLICKHOUSE_PASSWORD}" ]]; then
		log_fatal "failed to get CLICKHOUSE_PASSWORD"
	fi
	export CLICKHOUSE_PASSWORD
}

#
# Acquire lock before proceeding to avoid running multiple
# instances of the caller.
#
acquire_lock() {
	local lock="$1"

	set -C
	if ! { exec 200>"${lock}"; } 2>/dev/null; then
		log_info 1 "another instance of ${PROG_NAME} must be running because ${lock} exists"
		return 1
	fi
	set +C
	if ! flock -n 200; then
		log_info 1 "another instance of ${PROG_NAME} must be running because ${lock} is locked"
		return 1
	fi
	echo "$$" >> "${lock}"
	log_info 1 "pid $$ acquired lock on ${lock}"
}

#
# Authenticate with Iris API if $IRIS_PASSWORD is unset or empty ("").
#
irisctl_auth() {
        if [[ -z "${IRIS_PASSWORD+x}" ]]; then
                log_fatal "IRIS_PASSWORD is unset"
        fi
	if [[ -z "${IRIS_PASSWORD}" ]]; then
		log_fatal "IRIS_PASSWORD is empty"
	fi
	log_info 1 "irisctl is using IRIS_PASSWORD environment variable to authenticate"
	irisctl auth login
}

#
# Log an informative message for easier tracking and debugging.
#
log_info() {
	local level="$1"

	if [[ ${level} -lt 0 || ${level} -gt 3 ]]; then
		log_fatal "invalid verbosity level: ${level}"
	fi
	if [[ ${level} -gt ${VERBOSE} ]]; then
		return
	fi
	shift 1
	log_message "INFO" "$*"
}

#
# Log an error message.
#
log_error() {
	log_message "ERROR" "$*"
}

#
# Log a fatal error message and terminate the program with a non-zero exit code.
#
log_fatal() {
	log_message "ERROR" "$*"
	exit 1
}

#
# Log a message (common code for INFO and ERROR).
#
log_message() {
	local type="$1"
	local prog_color
	local msg_color

	shift 1
	if [[ "${type}" != "ERROR" ]]; then
		prog_color="${START_MAGENTA}"
		msg_color="${START_BLUE}"
	else
		prog_color="${START_RED}"
		msg_color="${START_RED}"
	fi
	timestamp=$(date +'%Y-%m-%dT%H:%M:%SZ')
	(1>&2 echo -n -e "${prog_color}${timestamp} ${PROG_NAME}: ${END_COLOR}")
	(1>&2 echo -e "${msg_color}[${type}] $*${END_COLOR}")
}

#
# Log lock file details to aid in debugging.
#
log_lock_details() {
	local lock_file="$1"

	(1>&2 ls -li "${lock_file}")
	(1>&2 cat "${lock_file}")
}

#
# Log a separator line to visually distinguish between different sections of the logs.
#
log_line() {
	(printf '%*s\n' 72 '' | tr ' ' '-') 1>&2
}
