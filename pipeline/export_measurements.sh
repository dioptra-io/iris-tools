#!/bin/bash

#
# This shell script exports newly obtained Iris measurement data and
# works as follows:
#
#   1. Using `irisctl`, it generates the list of all $TAG measurements
#      that finished successfully.  To skip this step, set
#      $EXPORT_ALL_MEAS to your desired all measurements file.
#   2. It checks each measurement in the above list against already
#      exported measurements recorded in $EXPORTED_MEAS, or in $INDEX_MD
#      if $EXPORTED_MEAS does not exist, to create a new measurement
#      list.  To skip this step, set $EXPORT_NEW_MEAS to your desired
#      new measurements file.
#   3. For each new measurement in $NEW_UUIDS, it runs the container
#      image $IRIS_EXPORTER_IMAGE to export the data of the measurement
#      to $EXPORTS_DIR_TMP.
#   4. It generates the new INDEX.md file in $EXPORTS_DIR_TMP.
#   5. It moves files from $EXPORTS_DIR_TMP to $EXPORTS_DIR.
#

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC1090,SC2002"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
readonly TOPLEVEL="$(git rev-parse --show-toplevel)"
source "${TOPLEVEL}/pipeline/common.sh"
source "${TOPLEVEL}/conf/export_settings.conf"

#
# Global variables to support command line flags and arguments.
#
ONLY_MOVE=false		# --only-move
NO_MOVE=false		# --no-move
ONLY_INDEX_TMP=false	# --only-index-tmp
ONLY_NEW_MEAS=false	# --only-new-meas
VERBOSE=1		# --verbose

#
# Other global variables.
#
NEW_UUIDS=()


#
# Print usage message and exit.
#
usage() {
        local exit_code="$1"

        cat <<EOF
usage:
        ${PROG_NAME} -h
        -h, --help		print help message and exit
            --no-move		do not move files from "${EXPORTS_DIR_TMP}" to "${EXPORTS_DIR}"
            --only-move		just move files from "${EXPORTS_DIR_TMP}" to "${EXPORTS_DIR}"
	    --only-index-tmp	just generate and update "${INDEX_MD_TMP}"
            --only-new-meas	just generate the list of new measurements to export and exit
	-v, --verbose		set the verbosity level (default: ${VERBOSE})
EOF
        exit "${exit_code}"
}

cleanup() {
	log_info 1 "removing ${EXPORT_LOCKFILE} /tmp/${PROG_NAME}.${TAG}.all.$$.*"
	rm -f "${EXPORT_LOCKFILE}" "/tmp/${PROG_NAME}.${TAG}.all.$$."*
}
trap cleanup EXIT

#
# Export to $EXPORTS_DIR all unexported $TAG measurements that finished successfully.
#
main() {
	local files

	parse_cmdline "$@"
	if ! acquire_lock "${EXPORT_LOCKFILE}"; then
		return
	fi
	print_vars # debugging support

	if ${ONLY_INDEX_TMP}; then
		generate_index
		update_index
		return
	fi

	if ${ONLY_MOVE}; then
		move_files
		return
	fi

	if [[ ! -f "${EXPORTED_MEAS}" ]]; then
		if [[ ! -f "${INDEX_MD}" ]]; then
			log_fatal "neither ${EXPORTED_MEAS} nor ${INDEX_MD} exists"
		fi
		log_info 1 "${EXPORTED_MEAS} does not exist; using ${INDEX_MD} to generate new measurement list"
	fi

	generate_new_meas_list
	if ${ONLY_NEW_MEAS}; then
		return
	fi

	export_new_meas
	generate_index
	update_index
	if ! ${NO_MOVE}; then
		move_files
	fi
}

#
# Print variables and their values that are used by this script.
#
print_vars() {
	log_info 1 "IRIS_EXPORTER_IMAGE=${IRIS_EXPORTER_IMAGE}"
	log_info 1 "EXPORTS_DIR=${EXPORTS_DIR}"
	log_info 1 "EXPORTS_DIR_TMP=${EXPORTS_DIR_TMP}"
	log_info 1 "TAG=${TAG}"
	log_info 1 "EXPORT_ALL_MEAS=${EXPORT_ALL_MEAS}"
	log_info 1 "EXPORT_NEW_MEAS=${EXPORT_NEW_MEAS}"
	log_info 1 "EXPORTED_MEAS=${EXPORTED_MEAS}"
	log_info 1 "INDEX_MD=${INDEX_MD}"
}

#
# Generate the list of new $TAG measurements that finished successfully
# since the last export.  The `irisctl` command takes about 2-3 minutes.
#
generate_new_meas_list() {
	local all_uuids=()
	local all_meas_file
	local stderr
	local tmp_file
	local uuid
	local uuid_to_ignore
	
	log_info 1 "getting new UUIDs"

	# If we already have the list of all $TAG measurements, just read it.
	# Otherwise, use `irisctl` to generate it.
	if [[ "${EXPORT_ALL_MEAS}" != "" ]]; then
		if [[ ! -f "${EXPORT_ALL_MEAS}" ]]; then
			log_fatal "${EXPORT_ALL_MEAS} does not exist"
		fi
		log_info 1 using existing "${EXPORT_ALL_MEAS}"
		mapfile -t all_uuids < <(awk '/^[0-9a-f][0-9a-f]*/ { print $1 }' "${EXPORT_ALL_MEAS}")
	else
		all_meas_file="$(mktemp "/tmp/${PROG_NAME}.${TAG}.all.$$.XXXX")"
		log_info 1 "irisctl list --all-users --tag ${TAG} --state finished -o > ${all_meas_file}"
		if ! { stderr="$(irisctl list --all-users --tag "${TAG}" --state finished -o 3>&1 1>"${all_meas_file}" 2>&3)"; } 3>&1; then
			log_fatal "irisctl failed"
		fi
		tmp_file=$(echo "${stderr}" | awk '/saving in/ {print $3}')
		log_info 1 "removing ${tmp_file}"
		rm "${tmp_file}"
		mapfile -t all_uuids < <(awk '/^[0-9a-f][0-9a-f]*/ { print $1 }' "${all_meas_file}")
	fi
	log_info 2 all measurements: "${#all_uuids[@]}"
	
	# If we already have the list of new measurements to export, just read it
	# into $NEW_UUIDS.  Otherwise, use $all_uuids to set $NEW_UUIDS.
	if [[ "${EXPORT_NEW_MEAS}" != "" ]]; then
		if [[ ! -f "${EXPORT_NEW_MEAS}" ]]; then
			log_fatal "${EXPORT_NEW_MEAS} does not exist"
		fi
		log_info 1 "using existing new measurements file ${EXPORT_NEW_MEAS}"
		mapfile -t NEW_UUIDS < <(cat "${EXPORT_NEW_MEAS}")
	else
		log_info 2 "generating new measurements list"
		for uuid in "${all_uuids[@]}"; do
			for uuid_to_ignore in "${UUIDS_TO_IGNORE[@]}"; do
				if [[ "${uuid}" == "${uuid_to_ignore}" ]]; then
					continue 2
				fi
			done
			if [[ -f "${EXPORTED_MEAS}" ]]; then
				if ! grep -q "${uuid}" "${EXPORTED_MEAS}" ; then
					NEW_UUIDS+=("${uuid}")
				fi
			else
				if ! grep -q "${uuid:0:8}" "${INDEX_MD}" ; then
					NEW_UUIDS+=("${uuid}")
				fi
			fi
		done
	fi
	log_info 2 "new measurements:" "${#NEW_UUIDS[@]}"
}

export_new_meas() {
	local uuid
	local files

	log_info 1 "exporting new measurements"

	if [[ "${#NEW_UUIDS[@]}" -eq 0 ]]; then
		log_info 1 "no new measurements to export"
		return
	fi

	if [[ ! -f "${EXPORTED_MEAS}" ]]; then
		log_fatal "${EXPORTED_MEAS} does not exist"
	fi
	if [[ "${IRIS_PASSWORD}" == "" ]]; then
		log_fatal "IRIS_PASSWORD is not set"
	fi
	if [[ "${CLICKHOUSE_PASSWORD}" == "" ]]; then
		log_fatal "${CLICKHOUSE_PASSWORD} is not set"
	fi

	chmod 644 "${EXPORTED_MEAS}"
	for uuid in "${NEW_UUIDS[@]}"; do
		date
		export_data "${uuid}"
		echo "${uuid}" >> "${EXPORTED_MEAS}"
	done
	chmod 444 "${EXPORTED_MEAS}"

	files="$(find "${EXPORTS_DIR_TMP}" -type f -print)"
	if [[ "${files}" == "${EXPORTS_DIR_TMP}/*" ]]; then
		log_fatal "${EXPORTS_DIR_TMP} is empty"
	fi
}

#
# Export data of the measurement specified by its UUID to $EXPORTS_DIR_TMP.
#
export_data() {
	local uuid="$1"

	log_info 1 "exporting data"

	export_cmd=(export --host clickhouse --user iris --password "${CLICKHOUSE_PASSWORD}" --database iris --destination /exports --uuid "${uuid}")
	docker run \
		--rm \
		--env IRIS_BASE_URL="${IRIS_BASE_URL}" \
		--env IRIS_USERNAME="${IRIS_USERNAME}" \
		--env IRIS_PASSWORD="${IRIS_PASSWORD}" \
		--network iris_default \
		--volume "${EXPORTS_DIR_TMP}":/exports \
		"${IRIS_EXPORTER_IMAGE}" "${export_cmd[@]}"
}

#
# Generate the INDEX.md file for the newly exported measurements.
#
generate_index() {
	log_info 1 "generating ${INDEX_MD_TMP}"

	if ! find "${EXPORTS_DIR_TMP}" -type f -print -quit | grep -q .; then
		log_info 1 "no measurements in ${EXPORTS_DIR_TMP} to generate ${INDEX_MD_TMP}"
		return
	fi

	log_info 2 docker run --rm --volume "${EXPORTS_DIR_TMP}":/today "${IRIS_EXPORTER_IMAGE}" index --destination /today
	docker run --rm --volume "${EXPORTS_DIR_TMP}":/today "${IRIS_EXPORTER_IMAGE}" index --destination /today
}

#
# Update the INDEX.md file in $EXPORTS_DIR directory by appending the
# newly generated INDEX.md file to it.
#
update_index() {
	log_info 1 "updating ${INDEX_MD_TMP}"

	if [[ ! -f "${INDEX_MD_TMP}" ]]; then
		log_info 1 "${INDEX_MD_TMP} does not exist"
		return
	fi

	{ cat "${INDEX_MD}"; sed -n '3,$p' "${INDEX_MD_TMP}"; } > "${INDEX_MD_TMP}.new"
	mv -f "${INDEX_MD_TMP}.new" "${INDEX_MD_TMP}"
}

#
# We have to use `find` in this function (instead of <dir>/*) because the
# argument list can be too long.
#
move_files() {
	log_info 1 "moving files from ${EXPORTS_DIR_TMP} to ${EXPORTS_DIR}"

	if ! find "${EXPORTS_DIR_TMP}" -type f -print -quit | grep -q .; then
		log_info 1 "no files in ${EXPORTS_DIR_TMP} to move to ${EXPORTS_DIR}"
		return
	fi
	find "${EXPORTS_DIR_TMP}" -type f -exec sudo chmod 444 {} \;
	log_info 1 sudo rsync -a --progress --remove-source-files "${EXPORTS_DIR_TMP}/" "${EXPORTS_DIR}/"
	sudo rsync -a --progress --remove-source-files "${EXPORTS_DIR_TMP}/" "${EXPORTS_DIR}/"
}

#
# Parse the command line and the configuration file.
#
parse_cmdline() {
	local args
	local arg

	if ! args="$(getopt \
			--options "hv:" \
			--longoptions "help only-move no-move only-index-tmp only-new-meas verbose:" \
			-- "$@")"; then
		return 1
	fi
	eval set -- "${args}"
	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-h|--help) usage 0;;
		   --only-move) ONLY_MOVE=true;;
		   --no-move) NO_MOVE=true;;
		   --only-new-meas) ONLY_NEW_MEAS=true;;
		   --only-index-tmp) ONLY_INDEX_TMP=true;;
		-v|--verbose) VERBOSE="$1"; shift 1;;
		--) break;;
		*) log_fatal "panic: error parsing arg=${arg}";;
		esac
	done
	if [[ $# -ne 0 ]]; then
		log_fatal "invalid command line"
	fi
}

main "$@"
