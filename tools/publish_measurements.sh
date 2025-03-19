#!/bin/bash

set -euo pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

readonly PROG_NAME="${0##*/}"
readonly TOPLEVEL="$(git rev-parse --show-toplevel)"

#
# Global variables to support command line flags.
#
CONFIG_FILE="${TOPLEVEL}/conf/settings.conf"	# --config
LIST_PUBLISH_VARS=false				# --list
NOW=""						# --now
RESTORE_PUBLISH_CONF=false			# --restore
USE_CACHE=false					# --use-cache
VERBOSE=1					# --verbose
ZERO_PUBLISH_CONF=false				# --zero

#
# Global variables to support logging and debugging.
#
METADATA_ALL="${TOPLEVEL}/conf/metadata_all"
DATA_ALL="${TOPLEVEL}/conf/data_all"
MEAS_MD_ALL_TXT="${TOPLEVEL}/cache/meas_md_all.txt"
START_RED="\033[1;31m"
START_BLUE="\033[1;34m"
END_COLOR="\033[0m"

#
# Global variables in the configuration files.
#
readonly METADATA_PUBLISHED_VARS=(
	"METADATA_PUBLISHED_LAST_UUID"		# uuid of the last published measurement metadata
	"METADATA_PUBLISHED_LAST_DATETIME"	# datetime of the last published measurement metadata
)
readonly DATA_PUBLISHED_VARS=(
	"DATA_PUBLISHED_TOT_NUM"	# total number of published data measurements
	"DATA_PUBLISHED_CUR_SET"	# the current set of data measurements that can be published
	"DATA_PUBLISHED_LAST_UUID"	# uuid of the last published measurement data
	"DATA_PUBLISHED_LAST_DATETIME"	# datetime of the last published measurement data
	"DATA_NUM_DAYS_TO_WAIT"		# number of days to wait before publishing next set
	"DATA_CONSIDERED_LAST_UUID"	# the last measurement UUID considered for publishing
)

#
# Global variables to save the original values in.
#
export METADATA_PUBLISHED_LAST_UUID_ORIG=""
export METADATA_PUBLISHED_LAST_DATETIME_ORIG=""
export DATA_PUBLISHED_TOT_NUM_ORIG=0
export DATA_PUBLISHED_CUR_SET_ORIG=()
export DATA_PUBLISHED_LAST_UUID_ORIG=""
export DATA_PUBLISHED_LAST_DATETIME_ORIG=""
export DATA_NUM_DAYS_TO_WAIT_ORIG=0
export DATA_LAST_UUID_CONSIDERED_ORIG=""

#
# Other global variables.
#
METADATA_UUID=""	# initialized by select_metadata_uuid()
CONSIDER_UUID=""	# initialized by select_data_uuid()
CONSIDER_STAT=""	# initialized by select_data_uuid()


usage() {
	cat <<EOF
usage:
	${PROG_NAME} --help
	${PROG_NAME} [-v <n>] [-c <config>] {--list | --restore}
	${PROG_NAME} [-v <n>] [-c <config>] [-n <now>] [-uz]
	-c, --config	configuration file (default ${CONFIG_FILE})
	-h, --help	print help message and exit
	-l, --list	list publishing variables and their values, then exit
	-n, --now	assume now is the given arg in the format yyyy-mm-ddThh:mm:ss
	-r, --restore	restore \${PUBLISH_METADATA_CONF} and \${PUBLISH_DATA_CONF}, then exit
	-u, --use-cache	use the cached \${MEAS_MD_ALL_JSON} file
	-v, --verbose	set the verbosity level (default: 1)
	-z, --zero	zero out \${PUBLISH_METADATA_CONF} and \${PUBLISH_DATA_CONF} before selecting
EOF
}

main() {
        local tmp_file

        tmp_file="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	# shellcheck disable=SC2064
	trap "rm -f ${tmp_file}" EXIT

	parse_cmd_line "$@"

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

	if ${ZERO_PUBLISH_CONF}; then
		zero_publish_conf
	fi
	source_publish_conf
	publish_measurements "${tmp_file}"
}

#
# Source the publish configuration files, perform sanity checks, and
# save the value of each variable in its <NAME>_ORIG conunterpart.
#
source_publish_conf() {
	local var
	local file

	# Set to an invalid value for sanity checks after sourcing.
	for var in "${METADATA_PUBLISHED_VARS[@]}" "${DATA_PUBLISHED_VARS[@]}"; do
		if [[ "${var}" == "DATA_PUBLISHED_CUR_SET" ]]; then
			DATA_PUBLISHED_CUR_SET=("X")
		else
			eval "${var}=X"
		fi
	done

	for file in "${PUBLISH_METADATA_CONF}" "${PUBLISH_DATA_CONF}"; do
		info 2 "sourcing ${file}"
		# shellcheck disable=SC1090
		source "${file}"
	done

	# Sanity checks.
	for var in "${METADATA_PUBLISHED_VARS[@]}" "${DATA_PUBLISHED_VARS[@]}"; do
		if [[ "${var}" == "DATA_PUBLISHED_CUR_SET" ]]; then
			if [[ "${DATA_PUBLISHED_CUR_SET[*]}" == *"X"* ]]; then
				fatal "${var} is missing from the configuration files"
			fi
		else
			if [[ "${!var}" == "X" ]]; then
				fatal "${var} is missing from the configuration files"
			fi
		fi
	done
	if [[ ${#DATA_PUBLISHED_CUR_SET[@]} -gt ${DATA_SET_SIZE} ]]; then
		fatal "DATA_PUBLISHED_CUR_SET has greater than DATA_SET_SIZE entries"
	fi

	# Save original values in the <NAME>_ORIG counterparts.
	for var in "${METADATA_PUBLISHED_VARS[@]}" "${DATA_PUBLISHED_VARS[@]}"; do
		if [[ "${var}" == "DATA_PUBLISHED_CUR_SET" ]]; then
			DATA_PUBLISHED_CUR_SET_ORIG=("${DATA_PUBLISHED_CUR_SET[@]}")
		else
			orig="${var}_ORIG"
			eval "${orig}=${!var}"
		fi
	done
}

#
# List the values of variables in the publish configuration files.
#
list_publish_conf() {
	local what="$1"
	local var
	local uuid

	if [[ "${what}" == "all" ]]; then
		# These variables are initialized in $CONFIG_FILE.
		for var in PUBLISH_METADATA_DISABLED PUBLISH_DATA_DISABLED; do
			printf "%-36s %s\n" "${var}" "${!var}"
		done
		echo
	fi

	if [[ "${what}" == "all" || "${what}" == "metadata" ]]; then
		for var in "${METADATA_PUBLISHED_VARS[@]}"; do
			printf "%-36s %s\n" "${var}" "${!var}"
		done
		if [[ "${what}" == "all" ]]; then
			echo
		fi
	fi

	if [[ "${what}" == "all" || "${what}" == "data" ]]; then
		for var in "${DATA_PUBLISHED_VARS[@]}"; do
			if [[ "${var}" == "DATA_PUBLISHED_CUR_SET" ]]; then
				printf "%s\n" "${var}"
				for uuid in "${DATA_PUBLISHED_CUR_SET[@]}"; do
					printf "    %s\n" "${uuid}"
				done
			else
				printf "%-36s %s\n" "${var}" "${!var}"
			fi
		done
	fi
}

#
# Restore publish configuration files from their backups if they exist.
#
restore_publish_conf() {
	local file

	for file in "${PUBLISH_METADATA_CONF}" "${PUBLISH_DATA_CONF}"; do
		if [[ ! -f "${file}.bak" ]]; then
			echo "${file}.bak does not exist"
			continue
		fi
		info 2 "restoring file ${file}"
		mv "${file}.bak" "${file}"
	done
}

#
# Zero out publish configuration files.
#
zero_publish_conf() {
	info 2 "zeroing out ${PUBLISH_METADATA_CONF} and ${PUBLISH_DATA_CONF}"
	mv "${PUBLISH_METADATA_CONF}" "${PUBLISH_METADATA_CONF}.bak"
	mv "${PUBLISH_DATA_CONF}" "${PUBLISH_DATA_CONF}.bak"

	echo "METADATA_PUBLISHED_LAST_UUID=" >> "${PUBLISH_METADATA_CONF}"
	echo "METADATA_PUBLISHED_LAST_DATETIME=" >> "${PUBLISH_METADATA_CONF}"

	# shellcheck disable=SC2129
	echo "DATA_PUBLISHED_TOT_NUM=0" >> "${PUBLISH_DATA_CONF}"
	echo "DATA_PUBLISHED_CUR_SET=()" >> "${PUBLISH_DATA_CONF}"
	echo "DATA_PUBLISHED_LAST_UUID=" >> "${PUBLISH_DATA_CONF}"
	echo "DATA_PUBLISHED_LAST_DATETIME=" >> "${PUBLISH_DATA_CONF}"
	echo "DATA_NUM_DAYS_TO_WAIT=0" >> "${PUBLISH_DATA_CONF}"
	echo "DATA_CONSIDERED_LAST_UUID=" >> "${PUBLISH_DATA_CONF}"
}

#
# Update publish metadata configuration file if any of its variables
# has been modified.
#
update_publish_conf_metadata() {
	local modified
	local var
	local orig

	modified=false
	for var in "${METADATA_PUBLISHED_VARS[@]}"; do
		orig="${var}_ORIG"
		if [[ "${!var}" != "${!orig}" ]]; then
			modified=true
			info 2 "${var} has been modified ${!var} => ${!orig}"
		fi
	done
	if ! ${modified}; then
		return
	fi

	info 2 "updating ${PUBLISH_METADATA_CONF}"
	mv "${PUBLISH_METADATA_CONF}" "${PUBLISH_METADATA_CONF}.bak"
	for var in "${METADATA_PUBLISHED_VARS[@]}"; do
		echo "${var}=${!var}" >> "${PUBLISH_METADATA_CONF}"
	done
	# Debugging support.
	echo "${NOW}   ${METADATA_PUBLISHED_LAST_UUID}" >> "${METADATA_ALL}"
}

#
# Return 0 (true) if any of the publish data variables has been modified,
# and 1 (false) otherwise.
#
is_publish_conf_data_modified() {
	local modified
	local var
	local orig

	modified=false
	for var in "${DATA_PUBLISHED_VARS[@]}"; do
		if [[ "${var}" == "DATA_PUBLISHED_CUR_SET" ]]; then
			if [[ "${DATA_PUBLISHED_CUR_SET[*]}" != "${DATA_PUBLISHED_CUR_SET_ORIG[*]}" ]]; then
				modified=true
				info 2 "${var} has been modified"
				info 2 "was ${DATA_PUBLISHED_CUR_SET_ORIG[*]}"
				info 2 "is  ${DATA_PUBLISHED_CUR_SET[*]}"
			fi
			continue
		fi
		orig="${var}_ORIG"
		if [[ "${!var}" != "${!orig}" ]]; then
			modified=true
			info 2 "${var} has modified ${!orig} => ${!var}"
		fi
	done
	if ${modified}; then
		return 0
	fi
	return 1
}

#
# Update publish data configuration file.  The caller must have checked
# that this update is needed (i.e., some variables have been modified).
#
update_publish_conf_data() {
	local var
	local uuid

	info 2 "updating ${PUBLISH_DATA_CONF}"
	mv "${PUBLISH_DATA_CONF}" "${PUBLISH_DATA_CONF}.bak"
	for var in "${DATA_PUBLISHED_VARS[@]}"; do
		if [[ "${var}" == "DATA_PUBLISHED_CUR_SET" ]]; then
			echo "DATA_PUBLISHED_CUR_SET=(" >> "${PUBLISH_DATA_CONF}"
			for uuid in "${DATA_PUBLISHED_CUR_SET[@]}"; do
				echo "    ${uuid}" >> "${PUBLISH_DATA_CONF}"
			done
			echo ")" >> "${PUBLISH_DATA_CONF}"
			continue
		fi
		echo "${var}=${!var}" >> "${PUBLISH_DATA_CONF}"
	done
}

#
# Publish metadata and data measurements.
#
publish_measurements() {
	local tmp_file="$1"

	if ! ${USE_CACHE} && ! create_meas_md_all_json; then
		fatal "failed to create ${MEAS_MD_ALL_JSON}"
	fi

	if ${PUBLISH_METADATA_DISABLED}; then
		info 1 "publishing metadata is disabled"
	elif publish_metadata "${tmp_file}"; then
		list_publish_conf "metadata"
	fi

	if ${PUBLISH_DATA_DISABLED}; then
		info 1 "publishing data is disabled"
		return
	fi
	if should_wait; then
		return
	fi
	try_to_publish_data "${tmp_file}"
	if is_publish_conf_data_modified; then
		# Update publish data configuration file.
		if ! update_publish_conf_data; then
			fatal "failed to update ${PUBLISH_DATA_CONF}"
		fi
		info 2 "successfully updated ${PUBLISH_DATA_CONF}"
		list_publish_conf "data"
	fi
}

#
# Create the JSON file that contains the metadata of all measurements.
#
create_meas_md_all_json() {
	local output
	local output_path

	info 2 "creating ${MEAS_MD_ALL_JSON}"
	if ! output="$(irisctl meas --all-users 2>&1 > /dev/null)"; then
		return 1
	fi
	output_path=$(echo "${output}" | awk '/saving in/ {print $3}')
	mv "${output_path}" "${MEAS_MD_ALL_JSON}"
}

#
# Publish the metadata of every $MEAS_TAG measurement whether it
# successfully finished or not.
#
publish_metadata() {
	local tmp_file="$1"

	# Select a measurement to publish its metadata.
	info 1 "checking if a new measurement is ready to publish its metadata"
	select_metadata_uuid "${tmp_file}"
	if [[ "${METADATA_UUID}" == "" ]] ; then
		if [[ "${METADATA_PUBLISHED_LAST_UUID}" == "" ]]; then
			echo "no measurement to publish its metadata"
		else
			echo "no measurement after ${METADATA_PUBLISHED_LAST_UUID} to publish its metadata"
		fi
		return 1
	fi
	info 1 "selected ${METADATA_UUID} to publish its metadata"

	# Publish the measurement's metadata.
	info 1 "${PROCESS_MEASUREMENTS} upload_metadata ${METADATA_UUID}"
	if ! "${PROCESS_MEASUREMENTS}" upload_metadata "${METADATA_UUID}"; then
		fatal "failed to publish metadata of ${METADATA_UUID}"
	fi

	# Update the configuration file.
	METADATA_PUBLISHED_LAST_UUID="${METADATA_UUID}"
	# shellcheck disable=SC2034
	METADATA_PUBLISHED_LAST_DATETIME="$(irisctl meas --uuid "${METADATA_PUBLISHED_LAST_UUID}" -o |  awk -F'"' '/^  "creation_time":/ {print $4}')"
	if ! update_publish_conf_metadata; then
		fatal "failed to update ${PUBLISH_METADATA_CONF} but successfully published metadata of ${METADATA_UUID}"
	fi
	info 2 "successfully published metadata of ${METADATA_UUID} and updated ${PUBLISH_METADATA_CONF}"
	return 0
}

#
# Select the UUID of the next measurement to publish its metadata.
# If METADATA_PUBLISHED_LAST_UUID is not set, select the most recent
# measurement.  This happens the very first time we start publishing
# metadata or when we manually clear METADATA_PUBLISHED_LAST_UUID.
#
select_metadata_uuid() {
	local tmp_file="$1"
	local irisctl_cmd=(
		"irisctl" "list"
		"-t" "${MEAS_TAG}"
		"-s" "finished" "-s" "agent_failure" "-s" "canceled"
		"--before" "${NOW}.000000"
		"${MEAS_MD_ALL_JSON}"
	)

	info 1 "${irisctl_cmd[@]}"
	if ! "${irisctl_cmd[@]}" > "${tmp_file}"; then
		fatal "failed to execute ${irisctl_cmd[*]}"
	fi

	if [[ "${METADATA_PUBLISHED_LAST_UUID:-}" == "" ]]; then
		info 1 "METADATA_PUBLISHED_LAST_UUID is not set; selecting the most recent ${MEAS_TAG} measurement"
		METADATA_UUID="$(tail -n 1 "${tmp_file}" | awk '{ print $1 }')"
	else
		info 1 "selecting the measurement after ${METADATA_PUBLISHED_LAST_UUID}"
		METADATA_UUID="$(awk -v pat="${METADATA_PUBLISHED_LAST_UUID}" '$0 ~ pat { if (getline) print $1; else exit }' "${tmp_file}")"
	fi
}

#
# Return 0 (true) if we should wait, 1 (false) otherwise.
#
# If we have already published a complete set, we must wait for
# $DATA_NUM_DAYS_TO_WAIT days to pass since the last publishing.
# If we must still wait, there's nothing to do.
#
should_wait() {
	local last_secs
	local now_secs
	local hours
	local days

	if [[ ${DATA_NUM_DAYS_TO_WAIT} -eq 0 || ${#DATA_PUBLISHED_CUR_SET[@]} -ne ${DATA_SET_SIZE} ]]; then
		return 1
	fi

	last_secs=$(date -u -d "${DATA_PUBLISHED_LAST_DATETIME}" +%s)
	now_secs=$(date -u -d "${NOW}" +%s)
	hours=$(( (now_secs - last_secs) / 3600 ))
	if [[ ${hours} -ge $(( DATA_NUM_DAYS_TO_WAIT * 24 )) ]]; then
		info 1 "should not wait because ${hours} hours ($(( hours / 24 )) days) have elapsed since the creation time of the last published measurement"
		return 1
	fi

	hours=$(( (DATA_NUM_DAYS_TO_WAIT * 24) - hours ))
	days=$((hours / 24))
	hours=$(( hours - (days * 24) ))
	echo "should wait ${days} day(s) and ${hours} hour(s) before starting a new set to publish"
	return 0
}

#
# Try to publish the current set.
#
try_to_publish_data() {
	local tmp_file="$1"
	local iteration

	info 1 "try_to_publish_data(): checking if a new measurement can be added to the current set"
	iteration=0
	while :; do
		# Debugging support.
		_=$(( iteration++ ))
		echo "try_to_publish_data(): iteration ${iteration} ..................."
		if [[ ${iteration} -gt 10 ]]; then
			exit 1 # XXX
		fi
		# Create the list of measurements that we need to consider.
		create_meas_to_consider "${tmp_file}"
		select_data_uuid "${tmp_file}" "${iteration}"
		info 1 "try_to_publish_data(): CONSIDER_UUID=${CONSIDER_UUID} CONSIDER_STAT=${CONSIDER_STAT}"
		if [[ "${CONSIDER_STAT}" == "no_more" ]]; then
			return
		fi
		if [[ "${CONSIDER_STAT}" == "good" ]]; then
			break
		fi
		if [[ "${CONSIDER_STAT}" == "agent_failure" || "${CONSIDER_STAT}" == "too_late" || "${CONSIDER_STAT}" == "worker_failure" ]]; then
			if [[ ${#DATA_PUBLISHED_CUR_SET[@]} -gt 0 && ${#DATA_PUBLISHED_CUR_SET[@]} -ne ${DATA_SET_SIZE} ]]; then
				# If we have gathered a few consecutive successful measurements
				# that are candidates for publishing but are not published yet,
				# we unfortunately have to ignore them because this measurement's
				# failure breaks the consecutivity of the set.
				info 1 "try_to_publish_data(): resetting the current set of ${#DATA_PUBLISHED_CUR_SET[@]} measurements"
				DATA_PUBLISHED_CUR_SET=()
			fi
			continue
		fi
		fatal "try_to_publish_data(): panic: invalid CONSIDER_STAT=${CONSIDER_STAT}"
	done
	# Sanity check.
	if [[ "${CONSIDER_STAT}" != "good" ]]; then
		fatal "try_to_publish_data(): panic: CONSIDER_STAT=${CONSIDER_STAT}, expected good"
	fi
	info 1 "try_to_publish_data(): selected ${CONSIDER_UUID} to add to the current set"
	if [[ ${#DATA_PUBLISHED_CUR_SET[@]} -eq ${DATA_SET_SIZE} ]]; then
		DATA_PUBLISHED_CUR_SET=("${CONSIDER_UUID}")
	else
		DATA_PUBLISHED_CUR_SET+=("${CONSIDER_UUID}")
	fi
	DATA_NUM_DAYS_TO_WAIT=0

	# If the current set is complete, publish it.
	if [[ ${#DATA_PUBLISHED_CUR_SET[@]} -eq ${DATA_SET_SIZE} ]]; then
		publish_cur_set
	fi
}

#
# Select the UUID of the next measurement to be considered for publishing
# its data.  Note that unlike measurement metadata, where we publish all,
# we publish a subset of measurement data in sets.
#
select_data_uuid() {
	local tmp_file="$1"
	local iteration="$2"
	local num_agents
	local num_state
	local t_cur
	local t_cur_secs
	local t_prev
	local t_prev_secs

	# Debugging support.
	if [[ ${iteration} -ge 3 ]]; then
		set -x
	else
		set +x
	fi

	# Is there a measurement to consider?
	CONSIDER_UUID="$(awk -v pat="${DATA_CONSIDERED_LAST_UUID}" '$0 ~ pat { if (getline) print $1; else exit }' "${tmp_file}")"
	# Sanity check.
	if [[ "${CONSIDER_UUID}" == "${DATA_CONSIDERED_LAST_UUID}" ]]; then
		fatal "select_data_uuid(): panic: CONSIDER_UUID=${CONSIDER_UUID} == DATA_CONSIDERED_LAST_UUID"
	fi
	if [[ "${CONSIDER_UUID}" == "" ]]; then
		CONSIDER_UUID="$(head -n 1 "${tmp_file}" | awk '{ print $1 }')"
	fi
	if [[ "${CONSIDER_UUID}" == "" || "${CONSIDER_UUID}" == "${DATA_CONSIDERED_LAST_UUID}" ]]; then
		CONSIDER_UUID=""
		CONSIDER_STAT="no_more"
		return
	fi

	# There is a measurement to consider.  Assume it is good unless
	# proven otherwise.
	info 3 "select_data_uuid(): setting DATA_CONSIDERED_LAST_UUID to ${CONSIDER_UUID}"
	DATA_CONSIDERED_LAST_UUID="${CONSIDER_UUID}"
	CONSIDER_STAT="good"

	# Get the metadata of this measurement to qualify it.
	irisctl_cmd=("irisctl" "meas" "--uuid" "${CONSIDER_UUID}" "-o")
	info 1 "${irisctl_cmd[*]} > ${tmp_file}"
	if ! "${irisctl_cmd[@]}" > "${tmp_file}"; then
		fatal "select_data_uuid(): failed to execute ${irisctl_cmd[*]}"
	fi

	# Ensure that all agents succeeded.
	info 3 "select_data_uuid(): making sure all agents succeeded"
	num_agents="$(grep -c '"tool_parameters"' "${tmp_file}")"
	num_state="$(grep -c '"state": "finished"' "${tmp_file}")"
	if [[ $((num_state - 1)) -ne ${num_agents} ]]; then
		echo "ignoring ${CONSIDER_UUID} because only $((num_state - 1)) of ${num_agents} agents successfully finished"
		CONSIDER_STAT="agent_failure"
		return
	fi
	info 3 "select_data_uuid(): all agents succeeded"

	# If a previous measurement exists in the current set, ensure
	# that the current measurement is no more than 6.5 hours after
	# the previous one.
	if [[ ${#DATA_PUBLISHED_CUR_SET[@]} -gt 0 && ${#DATA_PUBLISHED_CUR_SET[@]} -lt ${DATA_SET_SIZE} ]]; then
		info 3 "select_data_uuid(): making sure the current measurement is no more than 6.5 hours after the previous one"
		t_cur="$(awk -F'"' '/^  "creation_time":/ {print $4}' "${tmp_file}")"
		t_cur_secs=$(date -d "${t_cur}" +"%s")
		t_prev=$(irisctl meas --uuid "${DATA_PUBLISHED_CUR_SET[-1]}" -o |  awk -F'"' '/^  "creation_time":/ {print $4}')
		t_prev_secs=$(date -d "${t_prev}" +"%s")
		# Sanity check.
		if [[ ${t_cur_secs} -le ${t_prev_secs} ]]; then
			echo "previous measurement: ${DATA_PUBLISHED_CUR_SET[-1]}"
			echo "current measurement: ${CONSIDER_UUID}"
			echo "t_prev=${t_prev} t_cur=${t_cur}"
			fatal "select_data_uuid(): panic: t_cur_secs=${t_cur_secs} is not greater than t_prev_secs=${t_prev_secs}"
		fi
		if [[ $(( t_cur_secs - t_prev_secs )) -gt $(( (6 * 3600) + 1800 )) ]]; then
			echo "ignoring ${CONSIDER_UUID} because it was not created within 6.5 hours after ${DATA_PUBLISHED_CUR_SET[-1]}"
			CONSIDER_STAT="too_late"
			return
		fi
	else
		info 3 "select_data_uuid(): no need to make sure the current measurement is no more than 6.5 hours after the previous one"
	fi

	# Ensure there was no worker failure.
	info 3 "select_data_uuid(): making sure there was no worker failure"
	if worker_failed "${CONSIDER_UUID}"; then
		echo "ignoring ${CONSIDER_UUID} because of worker failure"
		CONSIDER_STAT="worker_failure"
		return
	fi
}

#
# Create a text file containing the UUIDs of measurements that can be
# considered for publishing.
#
create_meas_to_consider() {
	local tmp_file="$1"

	# Debugging support. XXX
	list_publish_conf "data"

	# If this is the very first time, assume that the last considered
	# measurement is the most recent measurement.
	if [[ "${DATA_CONSIDERED_LAST_UUID}" == "" ]]; then
		info 3 "create_meas_to_consider(): this is the very first time because DATA_CONSIDERED_LAST_UUID is empty"
		irisctl_cmd=("irisctl" "list" "-t" "${MEAS_TAG}" "-s" "finished" "--before" "${NOW}.000000" "${MEAS_MD_ALL_JSON}")
		info 1 "${irisctl_cmd[*]} > ${tmp_file}"
		if ! "${irisctl_cmd[@]}" > "${tmp_file}"; then
			fatal "create_meas_to_consider(): failed to execute ${irisctl_cmd[*]}"
		fi
		DATA_CONSIDERED_LAST_UUID="$(tail -n 1 "${tmp_file}" | awk '{ print $1 }')"
		info 3 "create_meas_to_consider(): DATA_CONSIDERED_LAST_UUID initialized to ${DATA_CONSIDERED_LAST_UUID}"
	fi

	# Get the creation time of $DATA_CONSIDERED_LAST_UUID.
	irisctl_cmd=("irisctl" "meas" "--uuid" "${DATA_CONSIDERED_LAST_UUID}" "-o")
	info 1 "${irisctl_cmd[*]} > ${tmp_file}"
	if ! "${irisctl_cmd[@]}" > "${tmp_file}"; then
		fatal "create_meas_to_consider(): failed to execute ${irisctl_cmd[*]}"
	fi
	creation_time="$(awk -F'"' '/^  "creation_time":/ {print $4}' "${tmp_file}")"
	if [[ "${creation_time}" == "" ]]; then
		fatal "create_meas_to_consider(): failed to parse out creation_time"
	fi

	# Get the list of measurements to consider.
	if [[ "${DATA_CONSIDERED_LAST_UUID}" == "${DATA_PUBLISHED_LAST_UUID}" ]]; then
		hours=$(( DATA_NUM_DAYS_TO_WAIT * 24 ))
	else
		hours=1
	fi
	after=$(date -d "${creation_time} UTC + ${hours} hours" +%Y-%m-%dT%H:%M:%S)
	irisctl_cmd=("irisctl" "list" "-t" "${MEAS_TAG}" "-s" "finished" "--before" "${NOW}.000000" "--after" "${after}" "${MEAS_MD_ALL_JSON}")
	info 1 "${irisctl_cmd[*]} > ${tmp_file}"
	if ! "${irisctl_cmd[@]}" > "${tmp_file}"; then
		fatal "create_meas_to_consider(): failed to execute ${irisctl_cmd[*]}"
	fi

	info 3 "create_meas_to_consider(): $(wc -l "${tmp_file}") measurements to consider after ${DATA_CONSIDERED_LAST_UUID}"
	# Debugging support. XXX
	cat "${tmp_file}"
}

#
# Publish the current set of consecutive measurements and generate a
# random number of days to wait before starting to build the next set.
#
publish_cur_set() {
	local uuid

	info 1 "publishing the current set"
	for uuid in "${DATA_PUBLISHED_CUR_SET[@]}"; do
		info 2 "${PROCESS_MEASUREMENTS}" upload_data "${uuid}"
		if ! "${PROCESS_MEASUREMENTS}" upload_data "${uuid}"; then
			fatal "failed to publish data of ${uuid}"
		fi
		_=$(( DATA_PUBLISHED_TOT_NUM++ ))
		info 2 "successfully published data of ${uuid}"
		# Debugging support.
		echo "${NOW}   $(grep "${uuid}" "${MEAS_MD_ALL_TXT}")" >> "${DATA_ALL}"
	done
	DATA_PUBLISHED_LAST_UUID="${DATA_PUBLISHED_CUR_SET[-1]}"
	DATA_PUBLISHED_LAST_DATETIME="$(irisctl meas --uuid "${DATA_PUBLISHED_LAST_UUID}" -o |  awk -F'"' '/^  "creation_time":/ {print $4}')"
	# Sanity check.
	if [[ "${DATA_PUBLISHED_LAST_DATETIME}" == "" ]]; then
		error "publish_cur_set(): failed to parse out DATA_PUBLISHED_LAST_DATETIME"
	fi
	DATA_NUM_DAYS_TO_WAIT=$(random_int "${MIN_DAYS_TO_WAIT}" "${MAX_DAYS_TO_WAIT}")
	# Debugging support.
	echo "wait ${DATA_NUM_DAYS_TO_WAIT} days" >> "${DATA_ALL}"
}

#
# Generate a random integer within the specified minimum and maximum
# range.
#
random_int() {
	local min="$1"
	local max="$2"
	local r

	r=$(od -An -N2 -i /dev/urandom | tr -d ' ')
	echo $(( r % (max - min + 1) + min ))
}

#
# Scan the Iris worker container logs to find failures occurring during
# the time that specified measurement was running.
#
worker_failed() {
	local uuid="$1"
	local tmp_file
	local irisctl_cmd
        local start_datetime
        local end_datetime
        local logcli_addr

	# First get the start and end datetimes of the measurement.
        tmp_file="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	# shellcheck disable=SC2064
	#trap "rm -f ${tmp_file}" EXIT
	irisctl_cmd=("irisctl" "meas" "--uuid" "${uuid}" "-o")
	info 1 "${irisctl_cmd[*]} > ${tmp_file}"
	if ! "${irisctl_cmd[@]}" > "${tmp_file}"; then
		fatal "failed to execute ${irisctl_cmd[*]}"
	fi
	start_datetime="$(awk -F'"' '/^  "start_time":/ {print $4}' "${tmp_file}")"
	if [[ "${start_datetime}" == "" ]]; then
		error "worker_failed(): failed to parse out start_time"
	fi
	start_datetime=$(date -d "${creation_time} UTC - 2 hours" +%Y-%m-%dT%H:%M:%S)
	if [[ "${start_datetime}" != *Z ]]; then
		start_datetime="${start_datetime}Z"
	fi
	end_datetime="$(awk -F'"' '/^  "end_time":/ {print $4}' "${tmp_file}")"
	if [[ "${end_datetime}" == "" ]]; then
		error "worker_failed(): failed to parse out end_time"
	fi
	end_datetime=$(date -d "${creation_time} UTC + 2 hours" +%Y-%m-%dT%H:%M:%S)
	if [[ "${end_datetime}" != *Z ]]; then
		end_datetime="${end_datetime}Z"
	fi
	
	# Now execute logcli to get the container logs.
        logcli_addr=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' iris_loki_1)
	info 1 logcli --addr="http://${logcli_addr}:3100" query "{container_name=\"iris_worker_1\"}" --from="${start_datetime}" --to="${end_datetime}" --limit 1000000 "> ${tmp_file} 2> /dev/null"
	if ! logcli --addr="http://${logcli_addr}:3100" query "{container_name=\"iris_worker_1\"}" --from="${start_datetime}" --to="${end_datetime}" --limit 1000000 > "${tmp_file}" 2> /dev/null; then
		error "worked_failed(): failed to execute logcli"
	fi

	# Finally see if there was a worker failure for this measurement.
	info 3 grep -i "failed .* watch_measurement_agent ${uuid}" "${tmp_file}"
	if grep -q -i "failed .* watch_measurement_agent ${uuid}" "${tmp_file}"; then
		info 2 "worker_failed(): there was a worker failure for measurement ${uuid}"
		return 0
	fi

	# Sanity check.
	if grep -q "${uuid}" ../cache/worker_failures.txt; then
		error "worked_failed(): panic: ${uuid} is in ../cache/worker_failures.txt"
	fi
	info 2 "worker_failed(): no worker failures found for measurement ${uuid}"
	rm -f "${tmp_file}"
	return 1
}

#
# Parse the command line flags and arguments.
#
parse_cmd_line() {
	local args
	local arg

	if ! args="$(getopt \
			--options "c:hln:ruv:z" \
			--longoptions "config: help list now: restore use-cache verbose: zero" \
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
		-u|--use-cache) USE_CACHE=true;;
		-v|--verbose) VERBOSE="$1"; shift 1;;
		-z|--zero) ZERO_PUBLISH_CONF=true;;
		--) break;;
		*) fatal "panic: error parsing arg=${arg}";;
		esac
	done
	if [[ $# -ne 0 ]]; then
		fatal "extra command line arguments: $*"
	fi

	if [[ "${NOW}" != "" ]]; then
		echo "assuming now is ${NOW} UTC"
	else
		NOW="$(date -u +%Y-%m-%dT%H:%M:%S)"
		echo "now is ${NOW} UTC"
	fi

	info 2 "sourcing ${CONFIG_FILE}"
	# shellcheck disable=SC1090
	source "${CONFIG_FILE}"
	if ${USE_CACHE}; then
		if [[ ! -f "${MEAS_MD_ALL_JSON}" ]]; then
			fatal "${MEAS_MD_ALL_JSON} does not exist but --use-cache is specified"
		fi
		info 2 "using cached ${MEAS_MD_ALL_JSON}"
	fi
}

#
# Log informative messages for easier tracking and debugging.
#
# Log level 9 is a special case where the provided arguments are executed
# rather than logged or printed.
#
info() {
	local level="$1"

	if [[ "${level}" -gt "${VERBOSE}" ]]; then
		return
	fi
	shift 1
	(1>&2 echo -n -e "${START_RED}${PROG_NAME}: ${END_COLOR}")
	(1>&2 echo -e "${START_BLUE}[INFO] $*${END_COLOR}")
}

#
# Print the fatal error message and terminate the program with a non-zero exit code.
#
fatal() {
	(1>&2 echo "[ERROR] $*")
	exit 1
}

main "$@"
