#!/bin/bash

#
# This shell script backs up newly generated ClickHouse tables and
# works as follows:
#
#   1. If specific measurement UUIDs are not specified on the command
#      line, it uses `irisctl` to generate the list of all measurements
#      that have finished.
#   2. It checks each measurement in the above list against already
#      backedup measurements in $BACKEDUP_MEAS_UUIDS to create a new
#      measurement list to back up.
#   3. For each new measurement in $BACKUP_UUIDS, it runs
#      $CLICKHOUSE_CLIENT to back up the ClickHouse tables of the
#      measurement to $BACKUP_LOCAL_DIR.
#

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC1090,SC2002"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
readonly TOPLEVEL="$(git rev-parse --show-toplevel)"
source "${TOPLEVEL}/conf/backup_settings.conf"
source "${TOPLEVEL}/pipeline/common.sh"

#
# Global variables to support command line flags and arguments.
#
DRY_RUN=false		# --dry-run
MAX_MEAS=1		# --max-meas
ONLY_BACKUP_UUIDS=false	# --only-backup_uuids
ONLY_ORGANIZE=false	# --only-organize
TAG=""			# --tag
VERBOSE=1		# --verbose
CMDLINE_UUIDS=()	# <meas-uuid>...

#
# Other global variables.
#
readonly UUID_RE='^[0-9a-f]{8}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{4}[-_][0-9a-f]{12}$'
MEAS_MD_ALL_JSON=""
MEAS_MD_ALL_TXT=""
ALL_UUIDS=()
BACKUP_UUIDS=()


#
# Print usage message and exit.
#
usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} --help
	${PROG_NAME} [common-flags] <meas-uuid>...
	${PROG_NAME} [common-flags] --only-organize
	${PROG_NAME} [common-flags] [--only-backup-uuids] [-t <tag>]

	-h, --help		print help message and exit
	-n, --dry-run		enable dry-run mode
	-m, --max-meas		if non-zero, maximum number of measurements to back up (default: ${MAX_MEAS})
	    --meas-md-all-json	use this JSON file that contains metadata of all measurements
	    --only-backup-uuids	just generate the list of measurement UUIDs to back up and exit
	    --only-organize	just organize files in \$BACKUP_LOCAL_DIR and exit
	-t, --tag		measurement tag (e.g., zeph-gcp-daily, ipv6-hitlist)
	-v, --verbose		set the verbosity level (default: ${VERBOSE})

	common-flags: [--meas-md-all-json <file>] [-n] [-v <n>]
EOF
	exit "${exit_code}"
}

cleanup() {
	log_info 1 "removing ${BACKUP_LOCKFILE} /tmp/${PROG_NAME}*.$$.*"
	rm -f "${BACKUP_LOCKFILE}" "/tmp/${PROG_NAME}"*".$$."*
}
trap cleanup EXIT

#
# Back up to $BACKUP_LOCAL_DIR all $TAG measurements that have finished
# and are not backed up.
#
main() {
	local uuid
	local n

	parse_cmdline "$@"
	validate_vars
	if ! acquire_lock "${BACKUP_LOCKFILE}"; then
		return 0
	fi

	# Authenticate with Iris API so we can run irisctl.
	irisctl_auth
	create_meas_md_all_json

	if ${ONLY_ORGANIZE}; then
		organize_backup_dir
		return 0
	fi

	generate_all_uuids
	generate_backup_uuids
	if ${ONLY_BACKUP_UUIDS}; then
		if [[ "${#BACKUP_UUIDS[@]}" -eq 0 ]]; then
			echo "no measurements to back up"
		else
			echo "${BACKUP_UUIDS[@]}" | tr ' ' '\n'
		fi
		return 0
	fi
	if [[ "${#BACKUP_UUIDS[@]}" -eq 0 ]]; then
		log_info 1 "no measurements to back up"
		return 0
	fi
	n=0
	for uuid in "${BACKUP_UUIDS[@]}"; do
		if backup_meas "${uuid}"; then
			organize_backup_dir
			((n++)) || :
			if [[ ${MAX_MEAS} -gt 0 && ${n} -ge ${MAX_MEAS} ]]; then
				break
			fi
		fi
	done
}

#
# Organize backed up tables in the root directory of $BACKUP_LOCAL_DIR by
# placing them in the appropriate subdirectory:
# <yyyy>/<mm>/<dd>/<meas_uuid>
#
organize_backup_dir() {
	local zip_file
	local meas_uuid
	local meas_date
	local meas_yy
	local meas_mm
	local meas_dd
	local meas_dir

	if [[ -z "${MEAS_MD_ALL_TXT}" ]]; then
		log_fatal "MEAS_MD_ALL_TXT is not set"
	fi
	if [[ ! -f "${MEAS_MD_ALL_TXT}" ]]; then
		log_fatal "${MEAS_MD_ALL_TXT} does not exist"
	fi

	while read -r zip_file; do
		log_info 2 "sudo chmod 444 ${zip_file}"
		sudo chmod 444 "${zip_file}"
		meas_uuid="${zip_file#*__}"
		meas_uuid="${meas_uuid%%__*}"
		meas_uuid="${meas_uuid%.zip}"
		meas_uuid="${meas_uuid//_/-}"
		meas_date="$(awk -v uuid="${meas_uuid}" '$0 ~ uuid { sub(/\..*/, "", $4); print $4 }' "$MEAS_MD_ALL_TXT")"
		IFS='-' read -r meas_yy meas_mm meas_dd <<< "${meas_date}"
		if [[ 10#${meas_yy} -lt 21 || 10#${meas_yy} -gt 30 ]]; then # future proof until 2030
			log_fatal "${meas_yy}: invalid year"
		fi
		if [[ 10#${meas_mm} -lt 1 || 10#${meas_mm} -gt 12 ]]; then
			log_fatal "${meas_mm}: invalid month"
		fi
		if [[ 10#${meas_dd} -lt 1 || 10#${meas_dd} -gt 32 ]]; then
			log_fatal "${meas_dd}: invalid day"
		fi
		meas_dir="${BACKUP_LOCAL_DIR}/20${meas_yy}/${meas_mm}/${meas_dd}/${meas_uuid}"
		log_info 2 mkdir -p "${meas_dir}"
		mkdir -p "${meas_dir}"
		log_info 2 mv "${zip_file}" "${meas_dir}"
		mv "${zip_file}" "${meas_dir}"
		echo
	done < <(find "${BACKUP_LOCAL_DIR}" -maxdepth 1 -name '*.zip' -print)
}

#
# Create the JSON file that contains the metadata of all measurements.
#
create_meas_md_all_json() {
	local output
	local output_path

	if [[ -n "${MEAS_MD_ALL_JSON}" ]]; then
		log_info 1 "using MEAS_MD_ALL_JSON ${MEAS_MD_ALL_JSON}"
		return 0
	fi

	MEAS_MD_ALL_JSON="$(mktemp "/tmp/${PROG_NAME}.mdall.$$.XXXX")"
	log_info 1 "creating MEAS_MD_ALL_JSON ${MEAS_MD_ALL_JSON}"
	log_info 1 irisctl meas --all-users
	if ! output="$(irisctl meas --all-users 2>&1 > /dev/null)"; then
		log_error "irisctl meas --all-users failed (output=${output})"
		return 1
	fi
	output_path=$(echo "${output}" | awk '/saving in/ {print $3}')
	log_info 1 mv "${output_path}" "${MEAS_MD_ALL_JSON}"
	mv "${output_path}" "${MEAS_MD_ALL_JSON}"
}

#
# Generate $MEAS_MD_ALL_JSON, $MEAS_MD_ALL_TXT files, and the list of
# all measurements to consider for backing up.
#
generate_all_uuids() {
	local cmd
	local uuid

	if [[ -n "${TAG}" ]]; then
		MEAS_MD_ALL_TXT="$(mktemp "/tmp/${PROG_NAME}.${TAG}.all.$$.XXXX")"
	else
		MEAS_MD_ALL_TXT="$(mktemp "/tmp/${PROG_NAME}.all.$$.XXXX")"
	fi
	cmd=("irisctl" "list" "-o" "-s" "finished" "-s" "canceled" "-s" "agent_failure") # skip ongoing measurements
	if [[ -n "${TAG}" ]]; then
		cmd+=("--tag" "${TAG}")
	fi
	cmd+=("${MEAS_MD_ALL_JSON}")
	log_info 1 "creating MEAS_MD_ALL_TXT ${MEAS_MD_ALL_TXT}"
	#
	# Create $MEAS_MD_ALL_TXT in reverse order so the newest
	# measurements are backed up before historical measurements.
	#
	log_info 1 "${cmd[*]} | tac > ${MEAS_MD_ALL_TXT}"
	if ! "${cmd[@]}" | tac > "${MEAS_MD_ALL_TXT}"; then
		log_fatal "${cmd[*]} failed"
	fi
	log_info 1 "wc -l ${MEAS_MD_ALL_TXT}"
	wc -l "${MEAS_MD_ALL_TXT}"
	log_info 1 "head ${MEAS_MD_ALL_TXT}"
	head "${MEAS_MD_ALL_TXT}"

	if [[ ${#CMDLINE_UUIDS[@]} -gt 0 ]]; then
		for uuid in "${CMDLINE_UUIDS[@]}"; do
			if ! grep -q "${uuid}" "${MEAS_MD_ALL_TXT}" ; then
				log_fatal "${uuid}: invalid measurement uuid"
			fi
			ALL_UUIDS+=("${uuid}")
		done
	else
		mapfile -t ALL_UUIDS < <(awk '/^[0-9a-f][0-9a-f]*/ { print $1 }' "${MEAS_MD_ALL_TXT}")
	fi
	log_info 2 "number of measurements to consider:" "${#ALL_UUIDS[@]}"
}

#
# Generate the list of measurements that have finished but have not been
# backed up (i.e., they are not in $BACKEDUP_MEAS_UUIDS).
#
generate_backup_uuids() {
	local uuid
	
	log_info 2 "generating measurement uuids that can be backed up"
	for uuid in "${ALL_UUIDS[@]}"; do
		if ! grep -q "${uuid}" "${BACKEDUP_MEAS_UUIDS}" ; then
			BACKUP_UUIDS+=("${uuid}")
		fi
	done
	log_info 2 "number of measurements that can be backed up:" "${#BACKUP_UUIDS[@]}"
}

backup_meas() {
	local uuid="$1"
	local tables=()
	local table

	log_info 1 "backing up measurement ${uuid}"

	log_info 1 "irisctl analyze tables --meas-uuid ${uuid} ${MEAS_MD_ALL_JSON} | awk ' /links|prefixes|probes|results/ { print $1 }'"
	mapfile -t tables < <(irisctl analyze tables --meas-uuid "${uuid}" "${MEAS_MD_ALL_JSON}" | awk ' /links|prefixes|probes|results/ { print $1 }')
	log_info 1 "${#tables[@]} tables"
	if [[ ${#tables[@]} -eq 0 ]]; then
		chmod 644 "${BACKEDUP_MEAS_UUIDS}"
		echo "${uuid} # no tables" >> "${BACKEDUP_MEAS_UUIDS}"
		chmod 444 "${BACKEDUP_MEAS_UUIDS}"
		return 1
	fi

	for table in "${tables[@]}"; do
		backup_table "${table}"
	done
	chmod 644 "${BACKEDUP_MEAS_UUIDS}"
	echo "${uuid} # ${#tables[@]} tables" >> "${BACKEDUP_MEAS_UUIDS}"
	chmod 444 "${BACKEDUP_MEAS_UUIDS}"
	log_info 1 "successfully backed up measurement ${uuid}"
}

backup_table() {
	local table="$1"
	local query
	local real_flags
	local safe_flags

	log_info 2 "backing up table ${table}"

	query="BACKUP TABLE ${table} TO Disk('${BACKUP_DISK}', '${table}.zip')"
	real_flags=(
		--user "${CLICKHOUSE_USERNAME}"
		--password "${CLICKHOUSE_PASSWORD}"
		--database "${CLICKHOUSE_DATABASE}"
		--query "${query}"
	)
	safe_flags=(
		--user "${CLICKHOUSE_USERNAME}"
		--password "\${CLICKHOUSE_PASSWORD}"
		--database "${CLICKHOUSE_DATABASE}"
		--query "${query}"
	)
	if ${DRY_RUN}; then
		echo clickhouse-client "${safe_flags[@]}"
	else
		log_info 2 clickhouse-client "${safe_flags[@]}"
		if ! "${TIME[@]}" clickhouse-client "${real_flags[@]}"; then
			log_fatal "failed to back up table ${table}"
		fi
		if [[ ! -f "${BACKUP_LOCAL_DIR}/${table}.zip" ]]; then
			log_fatal "back up succeeded, but ${BACKUP_LOCAL_DIR}/${table}.zip does not exist"
		fi
	fi
}

#
# Parse the command line and the configuration file.
#
parse_cmdline() {
	local args
	local arg

	if ! args="$(getopt \
			--options "hm:nt:v:" \
			--longoptions "help max-meas: meas-md-all-json: dry-run only-backup-uuids tag: verbose:" \
			-- "$@")"; then
		return 1
	fi
	eval set -- "${args}"
	while :; do
		arg="$1"
		shift 1
		case "${arg}" in
		-h|--help) usage 0;;
		-m|--max-meas) MAX_MEAS="$1"; shift 1;;
		   --meas-md-all-json) MEAS_MD_ALL_JSON="$1"; shift 1;;
		-n|--dry-run) DRY_RUN=true;;
		   --only-backup-uuids) ONLY_BACKUP_UUIDS=true;;
		-t|--tag) TAG="$1"; shift 1;;
		-v|--verbose) VERBOSE="$1"; shift 1;;
		--) break;;
		*) log_fatal "panic: error parsing arg=${arg}";;
		esac
	done
	if [[ $# -ne 0 ]]; then
		if [[ -n "${TAG}" ]]; then
			log_fatal "cannot specify both --tag and <meas-uuid>"
		fi
		if ${ONLY_BACKUP_UUIDS}; then
			log_fatal "cannot specify both --only-backup-uuids and <meas-uuid>"
		fi
		CMDLINE_UUIDS=("$@")
	fi
	if [[ ${MAX_MEAS} -lt 0 ]]; then
		log_fatal "${MAX_MEAS}: invalid value for --max-meas"
	fi
}

#
# Validate script variables initialized either from the configuration
# file or from the command line.
#
validate_vars() {
	local uuid

	if [[ ! -f "${BACKEDUP_MEAS_UUIDS}" ]]; then
		log_fatal "${BACKEDUP_MEAS_UUIDS} does not exist"
	fi
	if [[ -n "${MEAS_MD_ALL_JSON}" && ! -f "${MEAS_MD_ALL_JSON}" ]]; then
		log_fatal "${MEAS_MD_ALL_JSON} does not exist"
	fi
	for uuid in "${CMDLINE_UUIDS[@]}"; do
		if [[ ! ${uuid} =~ ${UUID_RE} ]]; then
			log_fatal "${uuid}: invalid argument"
		fi
	done
}

main "$@"
