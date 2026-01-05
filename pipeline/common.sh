#!/bin/bash

set -eu

readonly START_RED="\033[1;31m"
readonly START_BLUE="\033[1;34m"
readonly START_MAGENTA="\033[1;35m"
readonly END_COLOR="\033[0m"

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
