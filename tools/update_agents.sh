#!/bin/bash

#
# Update the container specified by $CONTAINER_NAME if a new container
# image specified by $IMAGE_NAME is available.  Update the container
# only when it is idle (read the XXX comment in the script).
#

set -eu
shellcheck "$0" # exits if shellcheck doesn't pass

readonly PROG_NAME="${0##*/}"

: "${GCP_PROJECT_ID:="mlab-edgenet"}"
: "${IMAGE_NAME="ghcr.io/dioptra-io/iris/iris-agent:production"}"
: "${CONTAINER_NAME="iris-agent"}"

cleanup() {
        echo rm -f "/tmp/${PROG_NAME}.$$."*
        rm -f "/tmp/${PROG_NAME}.$$."*
}
trap cleanup EXIT

main() {
	local tmp_file=""
	local host=""
	local hosts=()
	local zone=""
	local zones=()

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
	# To parse out hosts and zones, we could have used mapfile
	# but it's not available in older bash versions on macOS.
	# mapfile -t hosts < <(sort "${tmp_file}" | awk '/^iris-/ { print $1 }')
	# mapfile -t zones < <(sort "${tmp_file}" | awk '/^iris-/ { print $2 }')
	#
        while read -r host; do
		hosts+=("${host}")
        done < <(sort "${tmp_file}" | awk '/^iris-/ { print $1 }')
        while read -r zone; do
		zones+=("${zone}")
        done < <(sort "${tmp_file}" | awk '/^iris-/ { print $2 }')
	if [[ ${#hosts[@]} -ne ${#zones[@]} ]]; then # sanity check
		echo "${#hosts[@]} hosts but ${#zones[@]} zones"
		return 1
	fi

	#
	# Now iterate through the hosts and skip the ones that do
	# not follow Iris agent host naming convention (e.g.,
	# "iris-gcp-vm").
	#
	for ((i=0; i<${#zones[@]}; i++)); do
		zone="${zones[${i}]}"
		host="iris-${zone%-*}"
		if [[ "${hosts[${i}]}" != "${host}" ]]; then
			echo skipping "${hosts[${i}]}"
			continue
		fi
		update_agent "${host}" "${zone}"
		return 0
	done
}

update_agent() {
	local host="$1"
	local zone="$2"

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

main "$@"
