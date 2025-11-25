#!/usr/bin/env bash

#
# Scan repositories for leaked passwords referenced in $SECRETS_YML.
#

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC2016,SC2028"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"


ONLY_NAMES=			# -l
DRY_RUN=false			# -n
ONLY_LEAKS=			# -o
SECRETS_YML="./secrets.yml"	# -s
THOROUGH=false			# -t
USER_PASS=false			# -u
VERBOSE=false			# -v
REPOS=(".")			# positional arguments
PASSWORDS=()
declare -A USERNAME_PASSWORD

usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} [--help]
	${PROG_NAME} [--dry-run] [--only-leaks] [--secrets <path>] [--thorough] [--verbose] [<repo>...]
	-h, --help		print help message and exit
	-l, --only-names	print only the filenames where a leak was found
	-n, --dry-run		enable dry-run mode (default: ${DRY_RUN})
	-o, --only-leaks	print only leaked passwords
	-s, --secrets		path to secrets file (default: ${SECRETS_YML})
	-t, --thorough		enable thorough mode
	-u, --user-pass		print all username/password pairs and exit
	-v, --verbose		enable verbose mode

example:
	To list all leaked passwords in repo1 and repo2:
	${PROG_NAME} -o -s "/path/to/secrets.yml" -t /path/to/repo1 /path/to/repo2 2> /dev/null | sed -e 's/.*://' | sort | uniq
EOF

	exit "${exit_code}"
}

main() {
	local repo

	parse_cmdline "$@"

	read_passwords
	if ${VERBOSE} || ${USER_PASS}; then
		print_user_pass
		if ${USER_PASS}; then
			return
		fi
	fi

	for repo in "${REPOS[@]}"; do
		if [[ ! -d "${repo}/.git" ]]; then
			echo "${repo} is not a git repo" >&2
			continue
		fi
		echo "--- checking ${repo} for leaked passwords ---" >&2
		(cd "${repo}" && scan_light)
		if ${THOROUGH}; then
			(cd "${repo}" && scan_thorough)
		fi
		if ${VERBOSE}; then
			echo
		fi
	done
}

read_passwords() {
	local cmd
	local line
	local key
	local val

	if [[ ! -f "${SECRETS_YML}" ]]; then
		echo "${SECRETS_YML} does not exist or is not a regular file" >&2
		return 1
	fi

	#cmd='sops -d "${SECRETS_YML}" | yq e -r '\''.. |
	#	select(has("pass") and (.pass != "none") and (has("user") or has("name"))) |
	#	((path | map(tostring) |
	#	map(select(. | test("^[0-9]+$") | not)) |
	#	join(".")) + "." + ((.user // .name) | tostring) + ": " + (.pass))'\'' -'
	#
	# Extract username/password pairs from nested YAML structure.
	# The yq query:
	#   - recursively searches for objects with a "pass" field
	#     (not "none") and either "user" or "name".
	#   - builds a key from the YAML path and the username.
	#   - outputs lines in the format: <path>.<user>: <pass>.
	#
	cmd=$(cat <<'EOF'
sops -d "${SECRETS_YML}" | yq e -r '
.. |
  select(has("pass") and (.pass != "none") and (has("user") or has("name"))) |
  ((path | map(tostring) |
  map(select(. | test("^[0-9]+$") | not)) |
  join(".")) + "." + ((.user // .name) | tostring) + ": " + (.pass))' -
EOF
)
	while IFS= read -r line; do
		if [[ -z "${line}" ]]; then
			continue
		fi
		key=${line%%: *}
		if [[ "${line}" == *": "* ]]; then
			val=${line#*: }
		else
			val=""
		fi
		PASSWORDS+=("${val}")
		USERNAME_PASSWORD["${key}"]="${val}"
	done < <(eval "${cmd}")
}

print_user_pass() {
	local key

	for key in $(printf '%s\n' "${!USERNAME_PASSWORD[@]}" | sort); do
		printf '%-56s%s\n' "${key}" "${USERNAME_PASSWORD[${key}]}"
	done
}

scan_light() {
	if ${VERBOSE}; then
		echo "performing a light scan" >&2
		echo "checking current HEAD" >&2
	fi
	if ${VERBOSE} || ${DRY_RUN} ; then
		echo "git --no-pager grep ${ONLY_NAMES} ${ONLY_LEAKS} -f <(printf \"%s\n\" \"\${PASSWORDS[@]}\")" >&2
	fi
	if ${DRY_RUN}; then
		return
	fi
	git --no-pager grep ${ONLY_NAMES} ${ONLY_LEAKS} -f <(printf "%s\n" "${PASSWORDS[@]}") || :
}

scan_thorough() {
	if ${VERBOSE}; then
		echo "performing a thorough scan" >&2
		echo "checking all local branches" >&2
	fi
	if ${VERBOSE} || ${DRY_RUN}; then
		echo "git --no-pager grep ${ONLY_NAMES} ${ONLY_LEAKS} -f <(printf \"%s\n\" \"\${PASSWORDS[@]}\") \$(git for-each-ref --format='%(refname)' refs/heads/)" >&2
	fi
	if ! ${DRY_RUN}; then
		# shellcheck disable=SC2046
		git --no-pager grep ${ONLY_NAMES} ${ONLY_LEAKS} -f <(printf "%s\n" "${PASSWORDS[@]}") $(git for-each-ref --format='%(refname)' refs/heads/) || :
	fi

	if ${VERBOSE}; then
		echo "checking all remote branches" >&2
	fi
	if ${VERBOSE} || ${DRY_RUN}; then
		echo "git --no-pager grep ${ONLY_NAMES} ${ONLY_LEAKS} -f <(printf \"%s\n\" \"\${PASSWORDS[@]}\") \$(git for-each-ref --format='%(refname)' refs/remotes/)" >&2
	fi
	if ! ${DRY_RUN}; then
		# shellcheck disable=SC2046
		git --no-pager grep ${ONLY_NAMES} ${ONLY_LEAKS} -f <(printf "%s\n" "${PASSWORDS[@]}") $(git for-each-ref --format='%(refname)' refs/remotes/) || :
	fi

	if ${VERBOSE}; then
		echo "checking all commits (any branch, any history)" >&2
	fi
	if ${VERBOSE} || ${DRY_RUN}; then
		echo "git rev-list --all | xargs git --no-pager grep ${ONLY_NAMES} ${ONLY_LEAKS} -f <(printf \"%s\n\" \"\${PASSWORDS[@]}\")" >&2
	fi
	if ! ${DRY_RUN}; then
		git rev-list --all | xargs git --no-pager grep ${ONLY_NAMES} ${ONLY_LEAKS} -f <(printf "%s\n" "${PASSWORDS[@]}") || :
	fi
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
			--options "hlnos:tuv" \
			--longoptions "help only-names dry-run only-leaks secrets: thorough user-pass verbose" \
			-- "$@")"; then
		usage 1
	fi
	eval set -- "${args}"

	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-h|--help) usage 0;;
		-l|--only-names) ONLY_NAMES="-l";;
		-n|--dry-run) DRY_RUN=true;;
		-o|--only-leaks) ONLY_LEAKS="-o";;
		-s|--secrets) SECRETS_YML="$1"; shift 1;;
		-t|--thorough) THOROUGH=true;;
		-u|--user-pass) USER_PASS=true;;
		-v|--verbose) VERBOSE=true;;
		--) break;;
		*) echo "internal error parsing arguments!" >&2; usage 1;;
		esac
	done

	if [[ $# -eq 0 ]]; then
		REPOS=(".")
	else
		REPOS=("$@")
	fi
}

main "$@"
