#!/bin/bash

set -euo pipefail
shellcheck "$0"

readonly PROG_NAME="${0##*/}"

#
# Global variables to support command line flags and arguments.
#
FETCH_ONLY=false		# --fetch
DRY_RUN=false			# --dry-run
IS_ORG=false			# --org
OWNER=""			# arg 1
CONTAINER_NAME=""		# arg 2

API_BASE=""
VERSIONS_FILE=""

cleanup() {
	if ${FETCH_ONLY}; then
		echo "not removing ${VERSIONS_FILE}"
	else
		rm -f "${VERSIONS_FILE}"
	fi
}
trap cleanup EXIT

#
# Print usage message and exit.
#
usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} -h
	${PROG_NAME} [-n] [-o] <owner> <container-name>

	-f, --fetch	fetch all container versions, store them in a temporary file, and exit
	-h, --help	print help message and exit
	-n, --dry-run	enable dry-run mode (show what would be deleted)
	-o, --org	treat owner as an organization (default: user)

	owner		GitHub username or organization name
	container-name	name of the container (can include slashes)

examples:
	# For organization containers:
	${PROG_NAME} --dry-run --org dioptra-io iris/iris-worker

	# For user containers:
	${PROG_NAME} --dry-run myusername mycontainers

prerequisites:
	- gh (GitHub CLI) and jq commands
	- 'gh auth login' if not already authenticated
EOF
	exit "${exit_code}"
}

#
# Main function.
#
main() {
	check_prerequisites
	parse_cmdline "$@"
	if ${IS_ORG}; then
		API_BASE="/orgs/${OWNER}"
	else
		API_BASE="/users/${OWNER}"
	fi

	VERSIONS_FILE="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	fetch_all_versions
	if ! ${FETCH_ONLY}; then
		delete_untagged_containers
	fi
}

#
# Fetch all container versions.
#
fetch_all_versions() {
	local total_versions='.[].id'
	local untagged_versions='.[] | select((.metadata.container.tags // []) | length == 0) | .id'

	gh api --paginate "${API_BASE}/packages/container/${CONTAINER_NAME}/versions" > "${VERSIONS_FILE}"
	echo "$(jq -r "${total_versions}" "${VERSIONS_FILE}" | wc -l) total versions"
	echo "$(jq -r "${untagged_versions}" "${VERSIONS_FILE}" | wc -l) untagged versions"
}

#
# Delete untagged container images.
#
delete_untagged_containers() {
	local id
	local delete_cmd

	while read -r id; do
		if ${DRY_RUN}; then
			delete_cmd=("echo")
		else
			delete_cmd=("xxx")
		fi
		delete_cmd+=("gh" "api" "--method" "DELETE" "${API_BASE}/packages/container/${CONTAINER_NAME}/versions/${id}")
		if ! "${delete_cmd[@]}"; then
			echo "failed to execute ${delete_cmd[*]}" >&2
		fi
	done < <(jq -r '.[] | select((.metadata.container.tags // []) | length == 0) | .id' "${VERSIONS_FILE}")
}

#
# Check that the required commands are available and gh is authenticated.
#
check_prerequisites() {
	if ! command -v gh &> /dev/null; then
		echo "gh (GitHub CLI) is required but it's not in \$PATH" >&2
		return 1
	fi

	if ! command -v jq &> /dev/null; then
		echo "jq is required but it's not in \$PATH"
		return 1
	fi

	if ! gh auth status &> /dev/null; then
		echo "you must run: gh auth login"
		return 1
	fi
}

#
# Parse the command line flags and arguments.
#
parse_cmdline() {
	local getopt_cmd
	local args
	local arg

	if [[ "$(uname -s)" == "Darwin" ]]; then
		getopt_cmd="$(brew --prefix gnu-getopt)/bin/getopt"
	else
		getopt_cmd="$(command -v getopt)"
	fi
	if ! args="$("${getopt_cmd}" \
			--options "fhno" \
			--longoptions "fetch help dry-run org" \
			-- "$@")"; then
		usage 1
	fi
	eval set -- "${args}"

	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-f|--fetch) FETCH_ONLY=true;;
		-h|--help) usage 0;;
		-n|--dry-run) DRY_RUN=true;;
		-o|--org) IS_ORG=true;;
		--) break;;
		*) echo "panic: error parsing arg=${arg}" >&2; exit 1;;
		esac
	done
	if [[ $# -ne 2 ]]; then
		echo "specify exactly two positional arguments" >&2
		return 1
	fi
	OWNER="$1"
	CONTAINER_NAME="${2//\//%2f}" # url-encode the container name (replace / with %2f)
}

main "$@"
