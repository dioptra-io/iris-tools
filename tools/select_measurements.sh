#!/bin/bash

set -euo pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

readonly PROG_NAME="${0##*/}"

#
# Command line flags.
#
CONFIG_FILE="$(git rev-parse --show-toplevel)/conf/settings.conf" # --config
LIST_PUBLISH_VARS=false		# --list
NOW=""				# --now
QUIET=false			# --quiet
RESTORE_PUBLISH_CONF=false	# --restore
USE_CACHE=false			# --use-cache
ZERO_PUBLISH_CONF=false		# --zero

#
# These global variables will be initialized with values in
# $PUBLISH_METADATA_CONF and $PUBLISH_DATA_CONF in source_publish_conf().
#
readonly METADATA_PUBLISHED_VARS=(
	"METADATA_PUBLISHED_LAST_UUID"		# uuid of the last published measurement metadata
	"METADATA_PUBLISHED_LAST_DATETIME"	# datetime of the last published measurement metadata
)
readonly DATA_PUBLISHED_VARS=(
	"DATA_PUBLISHED_TOT_NUM"	# total number of published data measurements
	"DATA_PUBLISHED_CUR_SET"	# number of data measurements published in current set
	"DATA_PUBLISHED_LAST_UUID"	# uuid of the last published measurement data
	"DATA_PUBLISHED_LAST_DATETIME"	# datetime of the last published measurement data
	"DATA_NUM_DAYS_TO_WAIT"		# number of days to wait before publishing next set
)

METADATA_NEW_UUID="" # initialized by select_new_metadata()
DATA_NEW_UUID=""     # initialized by select_new_data()
# The following three variables are for debugging purposes.
METADATA_ALL="$(git rev-parse --show-toplevel)/conf/metadata_all"
DATA_ALL="$(git rev-parse --show-toplevel)/conf/data_all"
MEAS_MD_ALL_TXT="$(git rev-parse --show-toplevel)/cache/meas_md_all.txt"


usage() {
	cat <<EOF
usage:
	${PROG_NAME} --help
	${PROG_NAME} [--list] [--restore]
	${PROG_NAME} [-c <config>] [-n <now>] [-uz]
	-c, --config	configuration file (default ${CONFIG_FILE})
	-h, --help	print help message and exit
	-l, --list	list publishing variables and their values, then exit
	-n, --now	assume now is the given arg in the format yyyy-mm-ddThh:mm:ss
	-q, --quiet	do not print informational messages
	-r, --restore	restore \${PUBLISH_METADATA_CONF} and \${PUBLISH_DATA_CONF}, then exit
	-u, --use-cache	use the cached \${MEAS_MD_ALL_JSON} file
	-z, --zero	zero out \${PUBLISH_METADATA_CONF} and \${PUBLISH_DATA_CONF} before selecting
EOF
}

main() {
	parse_args "$@"

	if ${RESTORE_PUBLISH_CONF}; then
		restore_publish_conf
	fi
	if ${LIST_PUBLISH_VARS}; then
		source_publish_conf
		list_publish_conf "all"
	fi
	if ${RESTORE_PUBLISH_CONF} || ${LIST_PUBLISH_VARS}; then
		return
	fi
	if ${PUBLISH_METADATA_DISABLED} && ${PUBLISH_DATA_DISABLED}; then
		echo "publishing both metadata and data is disabled"
		return
	fi

	select_measurement
}

list_publish_conf() {
	local what="$1"
	local var

	if [[ "${what}" == "all" ]]; then
		for var in PUBLISH_METADATA_DISABLED PUBLISH_DATA_DISABLED; do
			printf "%-36s %s\n" "${var}" "${!var}"
		done
	fi
	if [[ "${what}" == "all" || "${what}" == "metadata" ]]; then
		for var in "${METADATA_PUBLISHED_VARS[@]}"; do
			printf "%-36s %s\n" "${var}" "${!var}"
		done
	fi
	if [[ "${what}" == "all" || "${what}" == "data" ]]; then
		for var in "${DATA_PUBLISHED_VARS[@]}"; do
			printf "%-36s %s\n" "${var}" "${!var}"
		done
	fi
}

source_publish_conf() {
	local var
	local file

	# Set to an invalid value for sanity check after sourcing.
	for var in "${METADATA_PUBLISHED_VARS[@]}" "${DATA_PUBLISHED_VARS[@]}"; do
		eval "${var}=X"
	done

	for file in "${PUBLISH_METADATA_CONF}" "${PUBLISH_DATA_CONF}"; do
		info "sourcing ${file}"
		# shellcheck disable=SC1090
		source "${file}"
	done

	# sanity checks
	for var in "${METADATA_PUBLISHED_VARS[@]}" "${DATA_PUBLISHED_VARS[@]}"; do
		if [[ "${!var}" == "X" ]]; then
			error "${var} is missing from the configuration files"
		fi
	done
	if [[ ${DATA_PUBLISHED_CUR_SET} -gt ${MAX_NUM_PER_SET} ]]; then
		error "DATA_PUBLISHED_CUR_SET is greater than MAX_NUM_PER_SET"
	fi
	if [[ ${DATA_PUBLISHED_CUR_SET} -gt ${DATA_PUBLISHED_TOT_NUM} ]]; then
		error "DATA_PUBLISHED_CUR_SET is greater than DATA_PUBLISHED_TOT_NUM"
	fi
	if [[ ${DATA_PUBLISHED_TOT_NUM} -gt 0 && "${DATA_PUBLISHED_LAST_UUID}" == "" ]]; then
		error "DATA_PUBLISHED_LAST_UUID is not set"
	fi
	if [[ ${DATA_PUBLISHED_TOT_NUM} -gt 0 && "${DATA_PUBLISHED_LAST_DATETIME}" == "" ]]; then
		error "DATA_PUBLISHED_LAST_DATETIME is not set"
	fi
}

select_measurement() {
	if ${ZERO_PUBLISH_CONF}; then
		zero_publish_conf
	fi

	source_publish_conf
	if ! ${USE_CACHE} && ! create_meas_md_all_json; then
		error "failed to create ${MEAS_MD_ALL_JSON}"
	fi

	publish_metadata
	list_publish_conf "metadata"

	publish_data
	list_publish_conf "data"
}

create_meas_md_all_json() {
	local output
	local output_path

	info "creating ${MEAS_MD_ALL_JSON}"
	if ! output="$(irisctl meas --all-users 2>&1 > /dev/null)"; then
		return 1
	fi
	output_path=$(echo "$output" | awk '/saving in/ {print $3}')
	mv "${output_path}" "${MEAS_MD_ALL_JSON}"
}

publish_metadata() {
	if ${PUBLISH_METADATA_DISABLED}; then
		echo "publishing metadata is disabled"
		return
	fi

	# Find a measurement to publish its metadata.
	info "checking if a new measurement is ready to publish its metadata"
	select_new_metadata
	if [[ "${METADATA_NEW_UUID}" == "" ]] ; then
		if [[ "${METADATA_PUBLISHED_LAST_UUID}" == "" ]]; then
			echo "could not find a measurement to publish its metadata"
		else
			echo "could not find a measurement after ${METADATA_PUBLISHED_LAST_UUID} to publish its metadata"
		fi
		return
	fi

	# Publish the measurement's metadata.
	info "publishing metadata of ${METADATA_NEW_UUID}"
	info "${PROCESS_MEASUREMENTS} upload_metadata ${METADATA_NEW_UUID}"
	if ! "${PROCESS_MEASUREMENTS}" upload_metadata "${METADATA_NEW_UUID}"; then
		error "failed to publish metadata of ${METADATA_NEW_UUID}"
	fi

	# Update the configuration file.
	if ! update_publish_conf_metadata; then
		error "failed to update ${PUBLISH_METADATA_CONF} but successfully published metadata of ${METADATA_NEW_UUID}"
	fi
	info "successfully published metadata of ${METADATA_NEW_UUID} and updated ${PUBLISH_METADATA_CONF}"
}

publish_data() {
	local now_secs
	local data_published_last_secs
	local diff_days
	local r

	if ${PUBLISH_DATA_DISABLED}; then
		echo "publishing data is disabled"
		return
	fi

	#
	# If we have published a complete set and are waiting, check
	# if we have waited at least $DATA_NUM_DAYS_TO_WAIT days and
	# whether can publish again.  Otherwise, there's nothing to do.
	#
	if [[ ${DATA_PUBLISHED_CUR_SET} -gt 0 && ${DATA_PUBLISHED_CUR_SET} -eq ${MAX_NUM_PER_SET} ]]; then
		data_published_last_secs=$(date -u -d "${DATA_PUBLISHED_LAST_DATETIME}" +%s)
		now_secs=$(date -u -d "${NOW}" +%s)
		diff_days=$(( (now_secs - data_published_last_secs) / (24 * 60 * 60) ))
		info "${diff_days} day(s) since the last measurement published on ${DATA_PUBLISHED_LAST_DATETIME}"
		if [[ ${diff_days} -lt ${DATA_NUM_DAYS_TO_WAIT} ]]; then
			echo "${DATA_NUM_DAYS_TO_WAIT} days have not yet passed to publish the next set"
			return
		fi
	fi

	#
	# We can publish because we are either at the beginning or in
	# the middle of publishing a set.
	#
	# If a new measurement has finished since the last data
	# publication, its data should be published.
	#
	info "checking if a new measurement is ready to publish its data"
	select_new_data
	if [[ "${DATA_NEW_UUID}" == "" ]] ; then
		if [[ "${DATA_PUBLISHED_LAST_UUID}" == "" ]]; then
			echo "could not find a measurement to publish its data"
		else
			echo "could not find a measurement after ${DATA_PUBLISHED_LAST_UUID} to publish its data"
		fi
		return
	fi

	info "found ${DATA_NEW_UUID} to publish its data"
	if [[ "${DATA_NEW_UUID}" != "${METADATA_PUBLISHED_LAST_UUID}" ]]; then
		info "new data uuid is different from the last metadata uuid"
		info "${DATA_NEW_UUID} ${METADATA_PUBLISHED_LAST_UUID}"
	fi

	# Publish the new measurement's data.
	info "publishing data of ${DATA_NEW_UUID}"
	if ! "${PROCESS_MEASUREMENTS}" upload_data "${DATA_NEW_UUID}"; then
		error "failed to publish data of ${DATA_NEW_UUID}"
	fi

	# Update publish data configuration file.
	if ! update_publish_conf_data; then
		error "failed to update ${PUBLISH_DATA_CONF} but successfully published data of ${DATA_NEW_UUID}"
	fi
	info "successfully published data of ${DATA_NEW_UUID} and updated ${PUBLISH_DATA_CONF}"
}

zero_publish_conf() {
	info "zeroing out ${PUBLISH_METADATA_CONF} and ${PUBLISH_DATA_CONF}"
	sed -i.bak \
		-e "s/METADATA_PUBLISHED_LAST_UUID.*/METADATA_PUBLISHED_LAST_UUID=/" \
		-e "s/METADATA_PUBLISHED_LAST_DATETIME.*/METADATA_PUBLISHED_LAST_DATETIME=/" \
		"${PUBLISH_METADATA_CONF}"
	sed -i.bak \
		-e "s/DATA_PUBLISHED_TOT_NUM.*/DATA_PUBLISHED_TOT_NUM=0/" \
		-e "s/DATA_PUBLISHED_CUR_SET.*/DATA_PUBLISHED_CUR_SET=0/" \
		-e "s/DATA_PUBLISHED_LAST_UUID.*/DATA_PUBLISHED_LAST_UUID=/" \
		-e "s/DATA_PUBLISHED_LAST_DATETIME.*/DATA_PUBLISHED_LAST_DATETIME=/" \
		-e "s/DATA_NUM_DAYS_TO_WAIT.*/DATA_NUM_DAYS_TO_WAIT=0/" \
		"${PUBLISH_DATA_CONF}"
}

update_publish_conf_metadata() {
	METADATA_PUBLISHED_LAST_UUID="${METADATA_NEW_UUID}"
	METADATA_PUBLISHED_LAST_DATETIME="${NOW}"

	info "updating ${PUBLISH_METADATA_CONF}"
	sed -i.bak \
		-e "s/METADATA_PUBLISHED_LAST_UUID.*/METADATA_PUBLISHED_LAST_UUID=${METADATA_PUBLISHED_LAST_UUID}/" \
		-e "s/METADATA_PUBLISHED_LAST_DATETIME.*/METADATA_PUBLISHED_LAST_DATETIME=${METADATA_PUBLISHED_LAST_DATETIME}/" \
		"${PUBLISH_METADATA_CONF}"
	echo "${NOW}   ${METADATA_PUBLISHED_LAST_UUID}" >> "${METADATA_ALL}"
}

update_publish_conf_data() {
	_=$(( DATA_PUBLISHED_TOT_NUM++ ))
	_=$(( DATA_PUBLISHED_CUR_SET++ ))
	if [[ ${DATA_PUBLISHED_CUR_SET} -gt ${MAX_NUM_PER_SET} ]]; then
		error "internal error: ${DATA_PUBLISHED_CUR_SET} > ${MAX_NUM_PER_SET}"
	fi
	DATA_PUBLISHED_LAST_UUID="${DATA_NEW_UUID}"
	DATA_PUBLISHED_LAST_DATETIME="${NOW}"
	if [[ ${DATA_PUBLISHED_CUR_SET} -eq ${MAX_NUM_PER_SET} ]]; then
		r=$(od -An -N2 -i /dev/urandom | tr -d ' ')
		DATA_NUM_DAYS_TO_WAIT=$(( r % (MAX_DAYS_TO_WAIT - MIN_DAYS_TO_WAIT + 1) + MIN_DAYS_TO_WAIT ))
	else
		DATA_NUM_DAYS_TO_WAIT=0
	fi

	info "updating ${PUBLISH_DATA_CONF}"
	sed -i.bak \
		-e "s/DATA_PUBLISHED_TOT_NUM.*/DATA_PUBLISHED_TOT_NUM=${DATA_PUBLISHED_TOT_NUM}/" \
		-e "s/DATA_PUBLISHED_CUR_SET.*/DATA_PUBLISHED_CUR_SET=${DATA_PUBLISHED_CUR_SET}/" \
		-e "s/DATA_PUBLISHED_LAST_UUID.*/DATA_PUBLISHED_LAST_UUID=${DATA_PUBLISHED_LAST_UUID}/" \
		-e "s/DATA_PUBLISHED_LAST_DATETIME.*/DATA_PUBLISHED_LAST_DATETIME=${DATA_PUBLISHED_LAST_DATETIME}/" \
		-e "s/DATA_NUM_DAYS_TO_WAIT.*/DATA_NUM_DAYS_TO_WAIT=${DATA_NUM_DAYS_TO_WAIT}/" \
		"${PUBLISH_DATA_CONF}"
	echo "${NOW}   $(grep ${DATA_PUBLISHED_LAST_UUID} "${MEAS_MD_ALL_TXT}")" >> "${DATA_ALL}"
	if [[ ${DATA_NUM_DAYS_TO_WAIT} -ne 0 ]]; then
		echo "wait ${DATA_NUM_DAYS_TO_WAIT} days" >> "${DATA_ALL}"
	fi
}

restore_publish_conf() {
	local file

	for file in "${PUBLISH_METADATA_CONF}" "${PUBLISH_DATA_CONF}"; do
		if [[ ! -f "${file}.bak" ]]; then
			info "${file}.bak does not exist"
			continue
		fi
		info "restoring file ${file}"
		mv "${file}.bak" "${file}"
	done
}

#
# If METADATA_PUBLISHED_LAST_UUID is not set, select the most recent
# measurement.  This happens the very first time we start publishing
# metadata or when we manually clear METADATA_PUBLISHED_LAST_UUID.
#
select_new_metadata() {
	local irisctl_cmd=()
	local tmp_file="/tmp/${PROG_NAME}.$$"

	irisctl_cmd=(
		"irisctl" "list"
		"-t" "${MEAS_TAG}"
		"-s" "finished" "-s" "agent_failure" "-s" "canceled"
		"--before" "${NOW}.000000"
		"${MEAS_MD_ALL_JSON}"
	)
	info "${irisctl_cmd[@]}"
	if ! "${irisctl_cmd[@]}" > "${tmp_file}"; then
		rm -f "${tmp_file}"
		error "failed to execute ${irisctl_cmd[*]}"
	fi

	if [[ "${METADATA_PUBLISHED_LAST_UUID:-}" == "" ]]; then
		info "METADATA_PUBLISHED_LAST_UUID is not set; selecting the most recent ${MEAS_TAG} measurement"
		METADATA_NEW_UUID="$(tail -n 1 "${tmp_file}" | awk '{ print $1 }')"
	else
		info "selecting the measurement after METADATA_PUBLISHED_LAST_UUID"
		METADATA_NEW_UUID="$(awk -v pat="${METADATA_PUBLISHED_LAST_UUID}" '$0 ~ pat { if (getline) print $1; else exit }' "${tmp_file}")"
	fi
	rm -f "${tmp_file}"
}

#
# XXX This function does not yet check for agent and worker failures.
#
select_new_data() {
	local irisctl_cmd=("irisctl" "list" "-t" "${MEAS_TAG}" "-s" "finished" "--before" "${NOW}.000000")
	local tmp_file="/tmp/${PROG_NAME}.$$"
	local after_datetime

	if [[ "${DATA_PUBLISHED_LAST_DATETIME}" != "" && ${DATA_PUBLISHED_CUR_SET} -eq ${MAX_NUM_PER_SET} ]]; then
		after_datetime=$(date -d "${DATA_PUBLISHED_LAST_DATETIME} UTC + ${DATA_NUM_DAYS_TO_WAIT} days" +%Y-%m-%dT%H:%M:%S)
		irisctl_cmd+=("--after" "${after_datetime}")
	fi
	irisctl_cmd+=("${MEAS_MD_ALL_JSON}")
	info "${irisctl_cmd[@]}"
	if ! "${irisctl_cmd[@]}" > "${tmp_file}"; then
		rm -f "${tmp_file}"
		error "failed to execute ${irisctl_cmd[*]}"
	fi

	if [[ "${DATA_PUBLISHED_LAST_DATETIME}" == "" ]]; then
		#
		# Case 1: The very first time.
		# Select the most recent successful measurement.
		#
		info "DATA_PUBLISHED_LAST_UUID is not set; selecting the most recent measurement"
		DATA_NEW_UUID="$(tail -n 1 "${tmp_file}" | awk '{ print $1 }')"
	elif [[ ${DATA_PUBLISHED_CUR_SET} -lt ${MAX_NUM_PER_SET} ]]; then
		#
		# Case 2: In the middle of a set.
		# Select the first successful measurement after the last one published.
		#
		info "selecting the first successful measurement after ${DATA_PUBLISHED_LAST_UUID}"
		DATA_NEW_UUID="$(awk -v pat="${DATA_PUBLISHED_LAST_UUID}" '$0 ~ pat { if (getline) print $1; else exit }' "${tmp_file}")"
	else
		#
		# Case 3: At the beginning of a new set.
		# Select the most recent successful measurement that is
		# at least DATA_NUM_DAYS_TO_WAIT after the last one published.
		#
		DATA_PUBLISHED_CUR_SET=0
		info "selecting the most recent successful measurement that is at least ${DATA_NUM_DAYS_TO_WAIT} after ${DATA_PUBLISHED_LAST_DATETIME}"
		DATA_NEW_UUID="$(head -n 1 "${tmp_file}" | awk '{ print $1 }')"
	fi
	rm -f "${tmp_file}"
}

parse_args() {
	local args
	local arg

	if ! args="$(getopt \
			--options "c:hln:qruz" \
			--longoptions "config: help list now: quiet restore use-cache zero" \
			-- "$@")"; then
		return 1
	fi
	eval set -- "${args}"
	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-c|--config) CONFIG_FILE="$1"; shift 1;;
		-h|--help) usage; exit 0;;
		-l|--list) LIST_PUBLISH_VARS=true;;
		-n|--now) NOW="$1"; shift 1;;
		-r|--restore) RESTORE_PUBLISH_CONF=true;;
		-q|--quiet) QUIET=true;;
		-u|--use-cache) USE_CACHE=true;;
		-z|--zero) ZERO_PUBLISH_CONF=true;;
		--) break;;
		*) error "internal error parsing arg=${arg}";;
		esac
	done
	if [[ $# -ne 0 ]]; then
		error "extra command line arguments: $*"
	fi

	info "sourcing ${CONFIG_FILE}"
	# shellcheck disable=SC1090
	source "${CONFIG_FILE}"
	if ${USE_CACHE}; then
		if [[ ! -f "${MEAS_MD_ALL_JSON}" ]]; then
			error "${MEAS_MD_ALL_JSON} does not exist but --use-cache is specified"
		fi
		info "using cached ${MEAS_MD_ALL_JSON}"
	fi

	if [[ "${NOW}" != "" ]]; then
		echo "assuming now is ${NOW} UTC"
	else
		NOW="$(date -u +%Y-%m-%dT%H:%M:%S)"
		echo "now is ${NOW} UTC"
	fi
}

info() {
	if ! ${QUIET}; then
		(1>&2 echo -n -e "\033[1;31m$PROG_NAME: \033[0m")
		(1>&2 echo -e "\033[1;34minfo: $*\033[0m")
	fi
}

error() {
	(1>&2 echo "error: $*")
	exit 1
}

main "$@"
