#!/bin/bash

#
# Update the container specified by $CONTAINER_NAME if a new container
# image specified by $IMAGE_NAME is available.  Update the container
# only when it is idle (read the XXX comment in the script).
#

set -eu
shellcheck "$0" # exits if shellcheck doesn't pass

HOSTS=(
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
ZONES=(
	asia-east1-a
	asia-northeast1-a
	asia-south1-a
	asia-southeast1-a
	europe-north1-a
	europe-west6-a
	me-central1-a
	southamerica-east1-a
	us-east4-a
	us-west4-a
)

main() {
	for ((i=0; i<${#HOSTS[@]}; i++)); do
	    echo "${HOSTS[${i}]}"
	    gcloud compute ssh --project mlab-edgenet --zone "${ZONES[${i}]}" "${HOSTS[${i}]}" --command="
			readonly IMAGE_NAME=\"ghcr.io/dioptra-io/iris/iris-agent:production\"
			readonly CONTAINER_NAME=\"iris-agent\"

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
			#     any measurements.
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
	done
}

main "$@"
