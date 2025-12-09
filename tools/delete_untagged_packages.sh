#!/bin/bash

set -euo pipefail

readonly PROG_NAME="${0##*/}"

# Color codes for output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly NC='\033[0m'

# Global variables to support command line flags and arguments
DRY_RUN=false
IS_ORG=false
OWNER=""
PACKAGE_NAME=""
PACKAGE_NAME_ENCODED=""
API_BASE=""
ALL_VERSIONS=()

#
# Print usage message and exit.
#
usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} -h
	${PROG_NAME} [-n] [-o] <owner> <package-name>

	-n, --dry-run	enable dry-run mode (show what would be deleted)
	-o, --org	treat owner as an organization (default: user)
	-h, --help	print help message and exit

	owner		GitHub username or organization name
	package-name	name of the container package (can include slashes)

examples:
	# For organization packages:
	${PROG_NAME} --org dioptra-io iris/iris-worker
	${PROG_NAME} --dry-run --org dioptra-io iris/iris-worker

	# For user packages:
	${PROG_NAME} myusername mypackage
	${PROG_NAME} --dry-run myusername mypackage

prerequisites:
	- GitHub CLI (gh) must be installed and authenticated
	- Run 'gh auth login' if not already authenticated

note:
	Package names with slashes are automatically URL-encoded
EOF
	exit "${exit_code}"
}

#
# Check if required commands are available.
#
check_prerequisites() {
	if ! command -v gh &> /dev/null; then
		echo -e "${RED}Error: GitHub CLI (gh) is required but not installed${NC}"
		echo "Install it from: https://cli.github.com/"
		exit 1
	fi

	if ! command -v jq &> /dev/null; then
		echo -e "${RED}Error: jq is required but not installed${NC}"
		echo "Install it with: brew install jq (macOS) or apt install jq (Linux)"
		exit 1
	fi

	if ! gh auth status &> /dev/null; then
		echo -e "${RED}Error: Not authenticated with GitHub CLI${NC}"
		echo "Please run: gh auth login"
		exit 1
	fi
}

#
# Parse command line arguments.
#
parse_cmdline() {
	# Parse all arguments
	while [[ $# -gt 0 ]]; do
		case "$1" in
		-n|--dry-run)
			DRY_RUN=true
			shift
			;;
		-o|--org)
			IS_ORG=true
			shift
			;;
		-h|--help)
			usage 0
			;;
		-*)
			echo -e "${RED}Error: Unknown option $1${NC}"
			usage 1
			;;
		*)
			# Positional argument
			if [[ -z "${OWNER}" ]]; then
				OWNER="$1"
			elif [[ -z "${PACKAGE_NAME}" ]]; then
				PACKAGE_NAME="$1"
			else
				echo -e "${RED}Error: Too many arguments${NC}"
				usage 1
			fi
			shift
			;;
		esac
	done

	# Validate required arguments
	if [[ -z "${OWNER}" ]] || [[ -z "${PACKAGE_NAME}" ]]; then
		echo -e "${RED}Error: Missing required arguments (owner and package-name)${NC}"
		usage 1
	fi

	# URL-encode the package name (replace / with %2F)
	PACKAGE_NAME_ENCODED="${PACKAGE_NAME//\//%2F}"

	# Determine API path based on owner type
	if ${IS_ORG}; then
		API_BASE="/orgs/${OWNER}"
	else
		API_BASE="/users/${OWNER}"
	fi
}

#
# Print script header.
#
print_header() {
	echo "================================================"
	echo "GitHub Untagged Package Cleanup Script"
	echo "================================================"
	echo "Owner: ${OWNER} $(${IS_ORG} && echo "(organization)" || echo "(user)")"
	echo "Package: ${PACKAGE_NAME}"
	echo "Mode: $(${DRY_RUN} && echo "DRY RUN" || echo "LIVE DELETE")"
	echo "================================================"
	echo ""
}

#
# Fetch all package versions from GitHub API.
#
fetch_all_versions() {
	local page=1
	local per_page=100
	local response
	local versions

	echo "Fetching package versions..."

	while true; do
		response=$(gh api \
			"${API_BASE}/packages/container/${PACKAGE_NAME_ENCODED}/versions?per_page=${per_page}&page=${page}" \
			2>&1)

		# Check for errors
		if echo "${response}" | grep -q "HTTP 404"; then
			echo -e "${RED}Error: Package not found. Check the owner and package name.${NC}"
			exit 1
		fi

		if echo "${response}" | jq -e '.message' > /dev/null 2>&1; then
			local error_msg
			error_msg=$(echo "${response}" | jq -r '.message')
			echo -e "${RED}Error from GitHub API: ${error_msg}${NC}"
			exit 1
		fi

		versions=$(echo "${response}" | jq -c '.[]' 2>/dev/null)

		if [[ -z "${versions}" ]]; then
			break
		fi

		while IFS= read -r version; do
			ALL_VERSIONS+=("${version}")
		done <<< "${versions}"

		page=$((page + 1))
	done

	echo "Found ${#ALL_VERSIONS[@]} total versions"
	echo ""
}

#
# Delete untagged package versions.
#
delete_untagged_versions() {
	local version_json
	local version_id
	local tags
	local created_at
	local sha_full
	local sha_short
	local untagged_count=0
	local deleted_count=0
	local error_count=0

	for version_json in "${ALL_VERSIONS[@]}"; do
		version_id=$(echo "${version_json}" | jq -r '.id')
		tags=$(echo "${version_json}" | jq -r '.metadata.container.tags | length')

		# Check if version has no tags
		if [[ "${tags}" == "0" ]]; then
			untagged_count=$((untagged_count + 1))
			created_at=$(echo "${version_json}" | jq -r '.created_at')
			# Get the SHA256 digest (name field contains sha256:...)
			sha_full=$(echo "${version_json}" | jq -r '.name')
			# Extract just the first 12 characters after "sha256:" for display
			sha_short=$(echo "${sha_full}" | sed 's/sha256://' | cut -c1-12)

			if ${DRY_RUN}; then
				echo -e "${YELLOW}[DRY RUN]${NC} Would delete: ${sha_short} (ID: ${version_id}, created: ${created_at})"
			else
				echo -n "Deleting: ${sha_short} (ID: ${version_id}, created: ${created_at})... "

				if gh api -X DELETE \
					"${API_BASE}/packages/container/${PACKAGE_NAME_ENCODED}/versions/${version_id}" \
					> /dev/null 2>&1; then
					echo -e "${GREEN}✓ Deleted${NC}"
					deleted_count=$((deleted_count + 1))
				else
					echo -e "${RED}✗ Failed${NC}"
					error_count=$((error_count + 1))
				fi
			fi
		fi
	done

	# Print summary
	echo ""
	echo "================================================"
	echo "Summary"
	echo "================================================"
	echo "Total versions found: ${#ALL_VERSIONS[@]}"
	echo "Untagged versions: ${untagged_count}"

	if ${DRY_RUN}; then
		echo -e "${YELLOW}Mode: DRY RUN - No deletions performed${NC}"
		echo "Run without --dry-run to actually delete these versions"
	else
		echo "Successfully deleted: ${deleted_count}"
		if [[ ${error_count} -gt 0 ]]; then
			echo -e "${RED}Failed to delete: ${error_count}${NC}"
		fi
	fi
	echo "================================================"
}

#
# Main function.
#
main() {
	parse_cmdline "$@"
	check_prerequisites
	print_header
	fetch_all_versions
	delete_untagged_versions
}

main "$@"
