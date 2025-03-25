#!/bin/bash

set -eu

readonly START_RED="\033[1;31m"
readonly START_BLUE="\033[1;34m"
readonly END_COLOR="\033[0m"

#
# Log informative messages for easier tracking and debugging.
#
log_info() {
	local level="$1"

	if [[ ${level} -lt 0 || ${level} -gt 3 ]]; then
		fatal "invalid verbosity level: ${level}"
	fi
	if [[ ${level} -gt ${VERBOSE} ]]; then
		return
	fi
	shift 1
	(1>&2 echo -n -e "${START_RED}${PROG_NAME}: ${END_COLOR}")
	(1>&2 echo -e "${START_BLUE}[INFO] $*${END_COLOR}")
}

#
# Print the fatal error message and terminate the program with a non-zero exit code.
#
fatal() {
	(1>&2 echo "${PROG_NAME}: [ERROR] $*")
	exit 1
}
