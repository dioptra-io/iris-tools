#!/bin/bash

set -eu

readonly START_RED="\033[1;31m"
readonly START_BLUE="\033[1;34m"
readonly END_COLOR="\033[0m"

#
# Print informative messages for easier tracking and debugging.
#
log_info() {
	local level="$1"
	local timestamp

	if [[ ${level} -lt 0 || ${level} -gt 3 ]]; then
		fatal "invalid verbosity level: ${level}"
	fi
	if [[ ${level} -gt ${VERBOSE} ]]; then
		return
	fi
	shift 1
	timestamp=$(date +'%Y-%m-%dT%H:%M:%SZ')
	(1>&2 echo -n -e "${START_RED}${timestamp} ${PROG_NAME}: ${END_COLOR}")
	(1>&2 echo -e "${START_BLUE}[INFO] $*${END_COLOR}")
}

#
# Print lock file details to aid in debugging.
#
log_lock_details() {
	local lock_file="$1"

	(1>&2 ls -li "${lock_file}")
	(1>&2 cat "${lock_file}")
}

#
# Print a separator line to visually distinguish between different sections of the logs.
#
log_line() {
	(1>&2 printf '%*s\n' 72 '' | tr ' ' '-')
}

#
# Print the fatal error message and terminate the program with a non-zero exit code.
#
fatal() {
	local timestamp

	timestamp=$(date +'%Y-%m-%dT%H:%M:%SZ')
	(1>&2 echo "${timestamp} ${PROG_NAME}: [ERROR] $*")
	exit 1
}
