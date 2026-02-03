#!/bin/bash

set -eu

PROG_NAME="${0##*/}"

INSTANCE=""		# -i
PROJECT="mlab-edgenet"	# -p
ZONE=""			# -z
EXPLAIN=false		# explain


cleanup() {
	rm -f "/tmp/${PROG_NAME}.$$."*
}
trap cleanup EXIT

usage() {
	local exit_code="$1"

	cat <<EOF
Usage:
	${PROG_NAME} [-h|--help]
	${PROG_NAME} [explain]
	${PROG_NAME} [-i <instance>] [-p <project>] [-z <zone>]
	-h, --help	print help and exit
	-i, --instance	instance (default: ${INSTANCE})
	-p, --project	project (default: ${PROJECT})
	-z, --zone	zone (default: ${ZONE})
Examples:
	${PROG_NAME} --help
	${PROG_NAME} explain
	${PROG_NAME} -i iris-asia-east1 -z asia-east1-b
	${PROG_NAME} -i retina-gcp-zurich1 -z europe-west6-c
EOF
	exit "${exit_code}"
}

main() {
	parse_cmdline "$@"
	stderr_color ">>> ZONE=\"${ZONE}\" INSTANCE=\"${INSTANCE}\" PROJECT=\"${PROJECT}\""
	if ${EXPLAIN}; then
		explain
		return 0
	fi

	#
	# Show project-wide access info.
	#
	audit
	iam_roles
	project_ssh_keys

	#
	# Show instance-specific access info.
	#
	if [[ -n "${INSTANCE}" ]]; then
		instance_ssh_keys
		instance_local_users
	fi
}

explain() {
	cat <<EOF
* GCP'S ACCESS METHODS
  GCP’s access methods behave differently when it comes to cleanup.
  Here is the standard procedure to ensure those users are truly gone.

 - Layer: IAM (GCP)	
   Action: Remove user from the Google Organization/Project.
   Result: Revokes their permission to request new access or use the "SSH" button.
 - Layer: Metadata
   Action: Remove their SSH key from Project or Instance metadata.
   Result: Prevokes their ability to connect via existing SSH keys.
 - Layer: OS (Local)
   Action: Manually delete the user account from the VM disk.
	sudo pkill -u <user> 
	sudo deluser --remove-home <user>
   Result: Removes their files (/home/username) and local UID.

* LEFTOVER USERS
  To avoid the "leftover user" problem, OS Login should be enabled
  because it has the following advantages:

  - Access is tied directly to the Google Identity.  The moment
    a user is deleted from Google Workspace or IAM role, they
    can no longer log in.
  - There will be no need to manage a giant list of SSH keys
    in Terraform/Metadata.
  - The UID/GID is consistent across all VMs in the project.

  Note that even if OS Login is enabled, the user account is virtual
  and usually disappears from the "authorized" list immediately, but
  their home directory might persist.

* AUTHORITATIVE PROJECT METADATA - This replaces both
  'google_compute_project_metadata_item' and any manual settings. It
  is the single source of truth for VM-level access.

  resource "google_compute_project_metadata" "project_access_config" {
    project = "mlab-edgenet"
    
    metadata = {
      # This enables OS Login project-wide. 
      # It makes manual SSH keys (below) less critical over time.
      enable-oslogin = "TRUE"
  
      # AUTHORITATIVE SSH KEYS: Any key NOT in this list will be 
      # automatically DELETED from the GCP project on the next 'terraform apply'.
      ssh-keys = <<EOT
        admin_bob:ssh-rsa AAAAB3Nza... bob@mlab.com
        dev_alice:ssh-rsa AAAAB3Nza... alice@mlab.com
      EOT
    }
  }

* AUTHORITATIVE IAM BINDINGS - This ensures only specific people have
  the 'Owner' role.  Anyone else who was added manually will be REMOVED.

  resource "google_project_iam_binding" "owners" {
    project = "mlab-edgenet"
    role    = "roles/owner"
  
    members = [
      "user:laiyi@measurementlab.net",
      "user:pavlos@measurementlab.net",
    ]
  }

* AUTHORITATIVE EDITORS (Optional but recommended)- Based on audit,
  there are many 'Editors'.  Use this to trim that list down to only
  current staff.

  resource "google_project_iam_binding" "editors" {
    project = "mlab-edgenet"
    role    = "roles/editor"
  
    members = [
      "user:saied.lip6@gmail.com",
      "user:timur.friedman.work@gmail.com",
    ]
  }

  How this works for the "Ghost User" problem

  - Metadata Consolidation:
    By using google_compute_project_metadata (plural) instead of
    metadata_item (singular), Terraform now "owns" the entire
    metadata shelf. If a ghost user's key is on that shelf but not
    in the code, Terraform throws it away.
  - Binding vs. Member:
    By using google_project_iam_binding, we are telling GCP: "The
    only members of this role are these people." The iam_member
    resource we were likely using before says: "Add this person to
    the role," but it doesn't care if 10 other people are already
    there.
  - OS Login Requirement:
    By setting enable-oslogin = "TRUE", we are shifting the goalposts.
    Even if a ghost user somehow kept a local SSH key, OS Login
    forces the VM to check IAM permissions in real-time.  Since
    we've removed them from the IAM bindings (Step 2 & 3), they are
    blocked at the door.
EOF
}

audit() {
	stderr_color "\n>>> asset audit:"
	stderr_color ">>> gcloud asset search-all-iam-policies --scope=projects/mlab-edgenet --format=json"

	gcloud asset search-all-iam-policies --scope=projects/mlab-edgenet --format=json |
	jq -r '.[] | .resource as $res | .policy.bindings[] | .role as $role | .members[] | "\(.) | \($role) | \($res)"' |
	column -t -s "|"
}

iam_roles() {
	stderr_color "\n>>> iam roles:"

	stderr_color ">>> gcloud projects get-iam-policy ${PROJECT}"
	gcloud projects get-iam-policy "${PROJECT}" |
	sed -e 's/roles\/.*[^ ]/\x1b[31m&\x1b[0m/g' # highlight roles for easier visual inspection
}

project_ssh_keys() {
	stderr_color "\n>>> project ssh keys:"

	stderr_color ">>> gcloud compute project-info describe --project ${PROJECT} --format=\"get(commonInstanceMetadata.items)\""
	gcloud compute project-info describe --project "${PROJECT}" --format="get(commonInstanceMetadata.items)" |
	sed -e "s/'/\"/g" |
	jq .value |
	sed -e "s/\"//g" -e 's/\\n/\n\n/g'
}

instance_ssh_keys() {
	local tmp_file

	stderr_color "\n>>> instance ssh keys:"

	tmp_file="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	stderr_color ">>> gcloud compute instances describe ${INSTANCE} --project ${PROJECT} --zone ${ZONE} --format=\"get(metadata.items)\""
	gcloud compute instances describe "${INSTANCE}" --project "${PROJECT}" --zone "${ZONE}" --format="get(metadata.items)" |
	sed -e 's/};{/}\n{/g' -e "s/'/\"/g" -e 's/\\n/\n/g' > "${tmp_file}"
	cat "${tmp_file}"
	if ! grep -q "os-login" "${tmp_file}"; then
		echo -e "\nOS Login (os-login) is not enabled"
	fi
}

instance_local_users() {
	local system_users=(
		root daemon bin sys sync games man lp mail news uucp proxy
		www-data backup list irc gnats nobody _apt systemd-network
		systemd-resolve messagebus systemd-timesync tss pollinate
		sshd fwupd-refresh _chrony syslog uuidd ubuntu
	)

	stderr_color "\n>>> local users:"

	stderr_color ">>> gcloud compute ssh ${INSTANCE} --project ${PROJECT} --zone ${ZONE} --command \"cut -d: -f1 /etc/passwd\""
	# shellcheck disable=SC2046
	gcloud compute ssh "${INSTANCE}" --project "${PROJECT}" --zone "${ZONE}" --command "cut -d: -f1 /etc/passwd" |
	grep -v $(printf ' -e %q' "${system_users[@]}")
}

stderr_color() {
	echo -e "\033[35m$*\033[0m" >&2
}

parse_cmdline() {
        local args
	local getopt_cmd

	if [[ "$(uname -s)" == "Darwin" ]]; then
		getopt_cmd="$(brew --prefix gnu-getopt)/bin/getopt"
	else
		getopt_cmd="$(command -v getopt)"
	fi
        if ! args="$("${getopt_cmd}" \
                        --options "hi:p:z:" \
                        --longoptions "help instance: zone: project:" \
                        -- "$@")"; then
                usage 1
        fi
        eval set -- "${args}"

        while :; do
                arg="$1"
                shift
                case "${arg}" in
                -h|--help) usage 0;;
                -i|--instance) INSTANCE="$1"; shift 1;;
                -p|--project) PROJECT="$1"; shift 1;;
                -z|--zone) ZONE="$1"; shift 1;;
                --) break;;
                *) echo "internal error parsing arguments!"; usage 1;;
                esac
        done

	if [[ $# -gt 0 ]]; then
		if [[ $# -gt 1 ]]; then
			usage 1
		fi
		if [[ "$1" != "explain" ]]; then
			echo "invalid argument: $1" >&2
			usage 1
		fi
		EXPLAIN=true
	fi
}
main "$@"
