#!/usr/bin/env bash

#
# Update the container specified by $CONTAINER_NAME if a new container
# image specified by $IMAGE_NAME is available.  Update the container
# only when it is idle (read the XXX comment in the script).
#

set -eu
shellcheck "$0" # exits if shellcheck doesn't pass

readonly PROG_NAME="${0##*/}"

#
# Global variables to support command line flags and arguments.
#
CONTAINER_NAME="iris-agent"					# --container
IMAGE_NAME="ghcr.io/dioptra-io/iris/iris-agent:production"	# --image
GCP_PROJECT_ID="mlab-edgenet"					# --project
DRY_RUN=false							# --dry-run
AGENTS=()							# arg...

cleanup() {
	echo rm -f "/tmp/${PROG_NAME}.$$."*
	rm -f "/tmp/${PROG_NAME}.$$."*
}
trap cleanup EXIT

usage() {
	local exit_code="$1"
	cat <<EOF
usage:
	${PROG_NAME} [-hn] [-c <container>] [-i <image>] [-p <project>] <agent>...
	-h, --help	print help message and exit
	-c, --container	configuration file (default: ${CONTAINER_NAME})
	-i, --image	image name (default: ${IMAGE_NAME})
	-n, --dry-run	enable dry-run mode
	-p, --project	GCP project ID (default: ${GCP_PROJECT_ID})

	agent:		agent name(s) (e.g., "iris-asia-east1") or "all"
EOF
	exit "${exit_code}"
}

main() {
	parse_cmdline "$@"
	update_agents
}

update_agents() {
	local tmp_file=""
	local hosts=()
	local zones=()
	local agent=""
	local host=""
	local zone=""
	local i

	#
	# Get the full list of all GCP VM hosts, their zones, and status.
	#
	tmp_file="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	echo gcloud compute instances list --format="table(name,zone.basename():label=REGION,status)"
	gcloud compute instances list --format="table(name,zone.basename():label=REGION,status)" > "${tmp_file}"

	#
	# Iris agents host names follow the "iris-<region>" naming
	# convention.  Parse out Iris agents and make sure we have
	# equal numbers of hosts and zones.
	#
	mapfile -t hosts < <(sort "${tmp_file}" | awk '/^iris-/ { print $1 }')
	mapfile -t zones < <(sort "${tmp_file}" | awk '/^iris-/ { print $2 }')
	if [[ ${#hosts[@]} -ne ${#zones[@]} ]]; then # sanity check
		echo "${#hosts[@]} hosts but ${#zones[@]} zones" >&2
		return 1
	fi
	if [[ ${#AGENTS[@]} -gt 0 ]]; then
		for agent in "${AGENTS[@]}"; do
			for ((i=0; i<${#zones[@]}; i++)); do
				zone="${zones[${i}]}"
				host="iris-${zone%-*}"
				if [[ "${agent}" == "${host}" ]]; then
					break
				fi
				echo "${agent} is not in the agent list" >&2
				return 1
			done
		done
	fi

	#
	# Now iterate through the hosts and:
	#   - skip hosts that do not follow Iris agent host naming
	#     convention (e.g., "iris-gcp-vm").
	#   - skip hosts that are not specified as command line arguments
	#
	for ((i=0; i<${#zones[@]}; i++)); do
		zone="${zones[${i}]}"
		host="iris-${zone%-*}"
		if [[ "${hosts[${i}]}" != "${host}" ]]; then
			echo skipping "${hosts[${i}]}"
			continue
		fi
		if [[ ${#AGENTS[@]} -eq 0 ]]; then
			update_agent "${host}" "${zone}"
			continue
		fi
		for agent in "${AGENTS[@]}"; do
			if [[ "${agent}" == "${host}" ]]; then
				update_agent "${host}" "${zone}"
				break
			fi
		done
	done
}

update_agent() {
	local host="$1"
	local zone="$2"

	if ${DRY_RUN}; then
		echo gcloud compute ssh --project "${GCP_PROJECT_ID}" --zone "${zone}" "${host}" --command="..."
		return
	fi

	gcloud compute ssh --project "${GCP_PROJECT_ID}" --zone "${zone}" "${host}" --command="
		readonly IMAGE_NAME=\"${IMAGE_NAME}\"
		readonly CONTAINER_NAME=\"${CONTAINER_NAME}\"

		#
		# Compare the current container against the latest container image.
		#
		CURRENT_IMAGE_ID=\$(sudo docker inspect --format=\"{{.Image}}\" \${CONTAINER_NAME})
		sudo docker pull \${IMAGE_NAME}
		LATEST_IMAGE_ID=\$(sudo docker inspect --format=\"{{.Id}}\" \${IMAGE_NAME})
		if [[ \"\${CURRENT_IMAGE_ID}\" == \"\${LATEST_IMAGE_ID}\" ]]; then
			echo \"\${CONTAINER_NAME} is up to date\"
			exit 0
		fi

		#
		# Check if the container is idle.
		#
		# XXX This logic is fragile because the agent could
		#     be idle between the rounds of measurements.  So,
		#     we need to make sure that the agent is not running
		#     any measurements.  The best way to do this is to
		#     query PostgreSQL or use irisctl.
		#
		LATEST_STATE=\$(sudo docker logs \${CONTAINER_NAME} 2>&1 | grep 'Setting agent state' | tail -1 | awk '{print \$NF}')
		if [[ \"\${LATEST_STATE}\" != \"AgentState.Idle\" ]]; then
			echo \"\${CONTAINER_NAME} is not currently idle, skipping update\"
			exit 0
		fi

		#
		# Stop and start the service to update the container.
		#
		echo \"\${CONTAINER_NAME} is idle, proceeding with update...\"
		sudo systemctl stop \${CONTAINER_NAME}
		sudo systemctl start \${CONTAINER_NAME}
		echo \"\${CONTAINER_NAME} updated successfully\""
	echo
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
			--options "hnc:i:p:" \
			--longoptions "help dry-run container: image: project:" \
			-- "$@")"; then
		usage 1
	fi
	eval set -- "${args}"

	# Parse flags.
	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-h|--help) usage 0;;
		-n|--dry-run) DRY_RUN=true;;
		-c|--container) CONTAINER_NAME="$1"; shift;;
		-i|--image) IMAGE_NAME="$1"; shift;;
		-p|--project) GCP_PROJECT_ID="$1"; shift;;
		--) break;;
		*) echo "panic: error parsing arg=${arg}" >&2; exit 1;;
		esac
	done

	# Parse postional arguments.
	if [[ $# -lt 1 ]]; then
		echo "specify \"all\" or specific agent name(s)" >&2
		return 1
	fi
	while [[ $# -gt 0 ]]; do
		if [[ "$1" == "all" ]]; then
			if [[ ${#AGENTS[@]} -ne 0 ]]; then
				echo "cannot specify both \"all\" and specific agents names" >&2
				return 1
			fi
		else
			AGENTS+=("$1")
		fi
		shift
	done
}

main "$@"
