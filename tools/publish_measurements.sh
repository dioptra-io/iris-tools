#!/bin/bash

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC1090,SC2034,SC2064,SC2129"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
readonly TOPLEVEL="$(git rev-parse --show-toplevel)"
source "${TOPLEVEL}/tools/common.sh"

#
# Global variables to support command line flags and arguments.
#
CONFIG_FILE="${TOPLEVEL}/conf/publish_settings.conf"	# --config
DRY_RUN=false						# --dry-run
LIST_PUBLISH_VARS=false					# --list
NOW=""							# --now
RESTORE_PUBLISH_CONF=false				# --restore
USE_CACHE=false						# --use-cache
VERBOSE=1						# --verbose
ZERO_PUBLISH_CONF=false					# --zero
POSITIONAL_ARGS=()					# to pass $PROCESS_MEASUREMENTS (e.g., --dry-run)

#
# Global variables to support logging and debugging.
#
METADATA_ALL="${TOPLEVEL}/cache/published_metadata.txt"
DATA_ALL="${TOPLEVEL}/cache/published_data.txt"
MEAS_WORKER_FAILURES="${TOPLEVEL}/cache/meas_worker_failures.txt" # XXX should be kept current

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
FILES_TO_REMOVE=()


#
# Print usage message and exit.
#
usage() {
	cat <<EOF
usage:
	${PROG_NAME} --help
	${PROG_NAME} [-c <config>] [-v <n>] {--list | --restore}
	${PROG_NAME} [-c <config>] [-n <now>] [-v <n>] [-uz] -- [<pass-down>]
	-c, --config	configuration file (default ${CONFIG_FILE})
	-n, --dry-run	enable dry-run mode
	-h, --help	print help message and exit
	-l, --list	list publishing variables and their values, then exit
	    --now	assume now is the given arg in the format yyyy-mm-ddThh:mm:ss
	-r, --restore	restore \${PUBLISH_METADATA_CONF} and \${PUBLISH_DATA_CONF}, then exit
	-u, --use-cache	use the cached \${MEAS_MD_ALL_JSON} file
	-v, --verbose	set the verbosity level (default: ${VERBOSE})
	-z, --zero	zero out \${PUBLISH_METADATA_CONF} and \${PUBLISH_DATA_CONF} before selecting
EOF
}

cleanup() {
	log_info 1 rm -f "${FILES_TO_REMOVE[@]}" "${PUBLISH_LOCKFILE}"
	rm -f "${FILES_TO_REMOVE[@]}" "${PUBLISH_LOCKFILE}"
}
trap cleanup EXIT

main() {
	local all_done=false

	parse_cmdline_and_conf "$@"

	# Acquire lock before proceeding to avoid running multiple
	# instances of this script.
	set -C
	if ! { exec 200>"${PUBLISH_LOCKFILE}"; } 2>/dev/null; then
		echo "another instance of ${PROG_NAME} must be running because ${PUBLISH_LOCKFILE} exists"
		return 1
	fi
	set +C
	if ! flock -n 200; then
		echo "another instance of ${PROG_NAME} must be running because ${PUBLISH_LOCKFILE} is locked"
		return 1
	fi
	echo "$$" >> "${PUBLISH_LOCKFILE}"
	log_info 1 "${PROG_NAME} ($$) acquired lock on ${PUBLISH_LOCKFILE}"

	if ${RESTORE_PUBLISH_CONF}; then
		restore_publish_conf
		all_done=true
	fi
	if ${ZERO_PUBLISH_CONF}; then
		zero_publish_conf
		all_done=true
	fi
	if ${LIST_PUBLISH_VARS}; then
		source_publish_conf
		list_publish_conf "all"
		all_done=true
	fi
	if ${all_done}; then
		return
	fi

	source_publish_conf
	# Remove the existing JSON Web Token (jwt) file before running the
	# first `irisctl` command to force reauthentication and avoid using
	# an expired token.
	rm -f "${HOME}/.iris/jwt"
	publish_measurements
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
		log_info 2 "sourcing ${file}"
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
		log_info 2 "restoring file ${file}"
		mv "${file}.bak" "${file}"
	done
}

#
# Zero out publish configuration files.
#
zero_publish_conf() {
	echo "zeroing out ${PUBLISH_METADATA_CONF} and ${PUBLISH_DATA_CONF}"
	mv "${PUBLISH_METADATA_CONF}" "${PUBLISH_METADATA_CONF}.bak"
	mv "${PUBLISH_DATA_CONF}" "${PUBLISH_DATA_CONF}.bak"

	echo "METADATA_PUBLISHED_LAST_UUID=" >> "${PUBLISH_METADATA_CONF}"
	echo "METADATA_PUBLISHED_LAST_DATETIME=" >> "${PUBLISH_METADATA_CONF}"

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
			log_info 2 "${var} has been modified ${!orig} => ${!var}"
		fi
	done
	if ! ${modified}; then
		return
	fi

	log_info 2 "updating ${PUBLISH_METADATA_CONF}"
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
				log_info 2 "${var} has been modified"
				log_info 2 "was ${DATA_PUBLISHED_CUR_SET_ORIG[*]}"
				log_info 2 "is  ${DATA_PUBLISHED_CUR_SET[*]}"
			fi
			continue
		fi
		orig="${var}_ORIG"
		if [[ "${!var}" != "${!orig}" ]]; then
			modified=true
			log_info 2 "${var} has been modified ${!orig} => ${!var}"
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

	log_info 2 "updating ${PUBLISH_DATA_CONF}"
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
	local tmp_file

	if ! ${USE_CACHE} && ! create_meas_md_all_json; then
		fatal "failed to create ${MEAS_MD_ALL_JSON}"
	fi

        tmp_file="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	FILES_TO_REMOVE+=("${tmp_file}")

	if ${PUBLISH_METADATA_DISABLED}; then
		log_info 1 "publishing metadata is disabled"
	elif publish_metadata "${tmp_file}"; then
		list_publish_conf "metadata"
	fi

	if ${PUBLISH_DATA_DISABLED}; then
		log_info 1 "publishing data is disabled"
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
		log_info 2 "successfully updated ${PUBLISH_DATA_CONF}"
		list_publish_conf "data"
	fi
}

#
# Create the JSON file that contains the metadata of all measurements.
#
create_meas_md_all_json() {
	local output
	local output_path

	log_info 1 "creating ${MEAS_MD_ALL_JSON}"
	log_info 1 irisctl meas --all-users
	if ! output="$(irisctl meas --all-users 2>&1 > /dev/null)"; then
		return 1
	fi
	output_path=$(echo "${output}" | awk '/saving in/ {print $3}')
	log_info 1 mv "${output_path}" "${MEAS_MD_ALL_JSON}"
	mv "${output_path}" "${MEAS_MD_ALL_JSON}"
}

#
# Publish the metadata of every $MEAS_TAG measurement whether it
# successfully finished or not.
#
publish_metadata() {
	local tmp_file="$1"

	# Select a measurement to publish its metadata.
	log_info 1 "checking if a new measurement is ready to publish its metadata"
	select_metadata_uuid "${tmp_file}"
	if [[ "${METADATA_UUID}" == "" ]] ; then
		if [[ "${METADATA_PUBLISHED_LAST_UUID}" == "" ]]; then
			echo "no measurement to publish its metadata"
		else
			echo "no measurement after ${METADATA_PUBLISHED_LAST_UUID} to publish its metadata"
		fi
		return 1
	fi

	# Publish the measurement's metadata.
	log_info 1 "publishing metadata of ${METADATA_UUID}"
	log_info 1 "${PROCESS_MEASUREMENTS}" ${POSITIONAL_ARGS[@]:+"${POSITIONAL_ARGS[@]}"} publish_metadata "${METADATA_UUID}"
	if ! ${DRY_RUN} && ! "${PROCESS_MEASUREMENTS}" ${POSITIONAL_ARGS[@]:+"${POSITIONAL_ARGS[@]}"} publish_metadata "${METADATA_UUID}"; then
		fatal "failed to publish metadata of ${METADATA_UUID}"
	fi

	# Update the configuration file.
	METADATA_PUBLISHED_LAST_UUID="${METADATA_UUID}"
	METADATA_PUBLISHED_LAST_DATETIME="$(irisctl meas --uuid "${METADATA_PUBLISHED_LAST_UUID}" -o |  awk -F'"' '/^  "creation_time":/ {print $4}')"
	if ! update_publish_conf_metadata; then
		fatal "failed to update ${PUBLISH_METADATA_CONF} but successfully published metadata of ${METADATA_UUID}"
	fi
	log_info 2 "successfully published metadata of ${METADATA_UUID} and updated ${PUBLISH_METADATA_CONF}"
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

	log_info 1 "${irisctl_cmd[@]}"
	if ! "${irisctl_cmd[@]}" > "${tmp_file}"; then
		fatal "failed to execute ${irisctl_cmd[*]}"
	fi

	if [[ "${METADATA_PUBLISHED_LAST_UUID:-}" == "" ]]; then
		log_info 1 "METADATA_PUBLISHED_LAST_UUID is not set; selecting the most recent ${MEAS_TAG} measurement"
		METADATA_UUID="$(tail -n 1 "${tmp_file}" | awk '{ print $1 }')"
	else
		log_info 1 "selecting the measurement after ${METADATA_PUBLISHED_LAST_UUID}"
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
		log_info 1 "no need to wait because ${hours} hours ($(( hours / 24 )) days) have elapsed since the creation time of the last published measurement"
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
# If we have gathered a few consecutive successful measurements
# that are candidates for publishing but are not published yet,
# we unfortunately have to ignore them because this measurement's
# failure breaks the consecutivity of the set.
#
try_to_publish_data() {
	local tmp_file="$1"
	local iteration

	log_info 1 "try_to_publish_data(): checking if a new measurement can be added to the current set"
	iteration=0
	while :; do
		_=$(( iteration++ )) # debugging support (to be removed) XXX
		echo "try_to_publish_data(): iteration ${iteration}"
		if [[ ${iteration} -gt 10 ]]; then
			fatal "try_to_publish_data(): iteration=${iteration}"
		fi
		# Create the list of measurements that we need to consider.
		create_meas_to_consider "${tmp_file}"
		select_data_uuid "${tmp_file}" "${iteration}"
		log_info 1 "try_to_publish_data(): CONSIDER_UUID=${CONSIDER_UUID} CONSIDER_STAT=${CONSIDER_STAT}"
		if [[ "${CONSIDER_STAT}" == "no_more" ]]; then
			return
		fi
		if [[ "${CONSIDER_STAT}" == "good" ]]; then
			break
		fi
		if [[ "${CONSIDER_STAT}" == "agent_failure" || "${CONSIDER_STAT}" == "too_late" || "${CONSIDER_STAT}" == "worker_failure" ]]; then
			if [[ ${#DATA_PUBLISHED_CUR_SET[@]} -gt 0 && ${#DATA_PUBLISHED_CUR_SET[@]} -ne ${DATA_SET_SIZE} ]]; then
				log_info 1 "try_to_publish_data(): resetting the current set of ${#DATA_PUBLISHED_CUR_SET[@]} measurements"
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
	log_info 1 "try_to_publish_data(): selected ${CONSIDER_UUID} to add to the current set"
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
	log_info 3 "select_data_uuid(): setting DATA_CONSIDERED_LAST_UUID to ${CONSIDER_UUID}"
	DATA_CONSIDERED_LAST_UUID="${CONSIDER_UUID}"
	CONSIDER_STAT="good"

	# Get the metadata of this measurement to qualify it.
	irisctl_cmd=("irisctl" "meas" "--uuid" "${CONSIDER_UUID}" "-o")
	log_info 1 "${irisctl_cmd[*]} > ${tmp_file}"
	if ! "${irisctl_cmd[@]}" > "${tmp_file}"; then
		fatal "select_data_uuid(): failed to execute ${irisctl_cmd[*]}"
	fi

	# Ensure that all agents succeeded.
	log_info 3 "select_data_uuid(): making sure all agents succeeded"
	num_agents="$(grep -c '"tool_parameters"' "${tmp_file}")"
	num_state="$(grep -c '"state": "finished"' "${tmp_file}")"
	if [[ $((num_state - 1)) -ne ${num_agents} ]]; then
		echo "ignoring ${CONSIDER_UUID} because only $((num_state - 1)) of ${num_agents} agents successfully finished"
		CONSIDER_STAT="agent_failure"
		return
	fi
	log_info 3 "select_data_uuid(): all agents succeeded"

	# If a previous measurement exists in the current set, ensure
	# that the current measurement is no more than 7 hours after
	# the previous one.
	if [[ ${#DATA_PUBLISHED_CUR_SET[@]} -gt 0 && ${#DATA_PUBLISHED_CUR_SET[@]} -lt ${DATA_SET_SIZE} ]]; then
		log_info 3 "select_data_uuid(): making sure the current measurement is no more than 7 hours after the previous one"
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
		if [[ $(( t_cur_secs - t_prev_secs )) -gt $(( 7 * 3600 )) ]]; then
			echo "ignoring ${CONSIDER_UUID} because it was not created within 7 hours after ${DATA_PUBLISHED_CUR_SET[-1]}"
			CONSIDER_STAT="too_late"
			return
		fi
	else
		log_info 3 "select_data_uuid(): no need to make sure the current measurement is no more than 7 hours after the previous one"
	fi

	# Ensure there was no worker failure.
	log_info 3 "select_data_uuid(): making sure there was no worker failure"
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
	local creation_time

	list_publish_conf "data" # debugging support XXX

	# If this is the very first time, assume that the last considered
	# measurement is the most recent measurement.
	if [[ "${DATA_CONSIDERED_LAST_UUID}" == "" ]]; then
		log_info 3 "create_meas_to_consider(): this is the very first time because DATA_CONSIDERED_LAST_UUID is empty"
		irisctl_cmd=("irisctl" "list" "-t" "${MEAS_TAG}" "-s" "finished" "--before" "${NOW}.000000" "${MEAS_MD_ALL_JSON}")
		log_info 1 "${irisctl_cmd[*]} > ${tmp_file}"
		if ! "${irisctl_cmd[@]}" > "${tmp_file}"; then
			fatal "create_meas_to_consider(): failed to execute ${irisctl_cmd[*]}"
		fi
		DATA_CONSIDERED_LAST_UUID="$(tail -n 1 "${tmp_file}" | awk '{ print $1 }')"
		log_info 3 "create_meas_to_consider(): DATA_CONSIDERED_LAST_UUID initialized to ${DATA_CONSIDERED_LAST_UUID}"
	fi

	# Get the creation time of $DATA_CONSIDERED_LAST_UUID.
	irisctl_cmd=("irisctl" "meas" "--uuid" "${DATA_CONSIDERED_LAST_UUID}" "-o")
	log_info 1 "${irisctl_cmd[*]} > ${tmp_file}"
	if ! "${irisctl_cmd[@]}" > "${tmp_file}"; then
		fatal "create_meas_to_consider(): failed to execute ${irisctl_cmd[*]}"
	fi
	creation_time="$(awk -F'"' '/^  "creation_time":/ {print $4}' "${tmp_file}")"
	if [[ "${creation_time}" == "" ]]; then
		cp -a "${FILES_TO_REMOVE[@]}" "${TOPLEVEL}/cache" # debugging support XXX
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
	log_info 1 "${irisctl_cmd[*]} > ${tmp_file}"
	if ! "${irisctl_cmd[@]}" > "${tmp_file}"; then
		fatal "create_meas_to_consider(): failed to execute ${irisctl_cmd[*]}"
	fi

	log_info 3 "create_meas_to_consider(): $(wc -l "${tmp_file}") measurements to consider after ${DATA_CONSIDERED_LAST_UUID}"
	cat "${tmp_file}" # debugging support XXX
}

#
# Publish the current set of consecutive measurements and generate a
# random number of days to wait before starting to build the next set.
#
publish_cur_set() {
	local uuid

	log_info 1 "publishing the current set"
	for uuid in "${DATA_PUBLISHED_CUR_SET[@]}"; do
		log_info 1 "${PROCESS_MEASUREMENTS}" ${POSITIONAL_ARGS[@]:+"${POSITIONAL_ARGS[@]}"} publish_data "${uuid}"
		if ! ${DRY_RUN} && !  "${PROCESS_MEASUREMENTS}" ${POSITIONAL_ARGS[@]:+"${POSITIONAL_ARGS[@]}"} publish_data "${uuid}"; then
			fatal "failed to publish data of ${uuid}"
		fi
		_=$(( DATA_PUBLISHED_TOT_NUM++ ))
		log_info 2 "successfully published data of ${uuid}"
		# Debugging support.
		echo "${NOW}   ${uuid}" >> "${DATA_ALL}"
	done
	DATA_PUBLISHED_LAST_UUID="${DATA_PUBLISHED_CUR_SET[-1]}"
	DATA_PUBLISHED_LAST_DATETIME="$(irisctl meas --uuid "${DATA_PUBLISHED_LAST_UUID}" -o |  awk -F'"' '/^  "creation_time":/ {print $4}')"
	# Sanity check.
	if [[ "${DATA_PUBLISHED_LAST_DATETIME}" == "" ]]; then
		fatal "publish_cur_set(): failed to parse out DATA_PUBLISHED_LAST_DATETIME"
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
	FILES_TO_REMOVE+=("${tmp_file}")
	irisctl_cmd=("irisctl" "meas" "--uuid" "${uuid}" "-o")
	log_info 1 "${irisctl_cmd[*]} > ${tmp_file}"
	if ! "${irisctl_cmd[@]}" > "${tmp_file}"; then
		fatal "failed to execute ${irisctl_cmd[*]}"
	fi
	start_datetime="$(awk -F'"' '/^  "start_time":/ {print $4}' "${tmp_file}")"
	if [[ "${start_datetime}" == "" ]]; then
		fatal "worker_failed(): failed to parse out start_time"
	fi
	start_datetime=$(date -d "${start_datetime} UTC - 2 hours" +%Y-%m-%dT%H:%M:%S)
	if [[ "${start_datetime}" != *Z ]]; then
		start_datetime="${start_datetime}Z"
	fi
	end_datetime="$(awk -F'"' '/^  "end_time":/ {print $4}' "${tmp_file}")"
	if [[ "${end_datetime}" == "" ]]; then
		fatal "worker_failed(): failed to parse out end_time"
	fi
	end_datetime=$(date -d "${end_datetime} UTC + 2 hours" +%Y-%m-%dT%H:%M:%S)
	if [[ "${end_datetime}" != *Z ]]; then
		end_datetime="${end_datetime}Z"
	fi
	
	# Now execute logcli to get the container logs.
        logcli_addr=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' iris_loki_1)
	log_info 1 logcli --addr="http://${logcli_addr}:3100" query "{container_name=\"iris_worker_1\"}" --from="${start_datetime}" --to="${end_datetime}" --limit 1000000 "> ${tmp_file} 2> /dev/null"
	if ! logcli --addr="http://${logcli_addr}:3100" query "{container_name=\"iris_worker_1\"}" --from="${start_datetime}" --to="${end_datetime}" --limit 1000000 > "${tmp_file}" 2> /dev/null; then
		fatal "worked_failed(): failed to execute logcli"
	fi

	# Finally see if there was a worker failure for this measurement.
	log_info 3 grep -i "failed .* watch_measurement_agent('${uuid}" "${tmp_file}"
	if grep -q -i "failed .* watch_measurement_agent('${uuid}" "${tmp_file}"; then
		log_info 2 "worker_failed(): there was a worker failure for measurement ${uuid}"
		return 0
	fi

	# Sanity check. XXX
	if grep -q "${uuid}" "${MEAS_WORKER_FAILURES}"; then
		fatal "worked_failed(): panic: ${uuid} is in ${MEAS_WORKER_FAILURES}"
	fi
	log_info 2 "worker_failed(): no worker failures found for measurement ${uuid}"
	rm -f "${tmp_file}"
	return 1
}

#
# Parse the command line and the configuration file.
#
parse_cmdline_and_conf() {
	local args
	local arg

	if ! args="$(getopt \
			--options "c:hlnruv:z" \
			--longoptions "config: dry-run help list now: restore use-cache verbose: zero" \
			-- "$@")"; then
		return 1
	fi
	eval set -- "${args}"
	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-c|--config) CONFIG_FILE="$1"; shift 1;;
		-n|--dry-run) DRY_RUN=true;;
		-h|--help) usage;;
		-l|--list) LIST_PUBLISH_VARS=true;;
		   --now) NOW="$1"; shift 1;;
		-r|--restore) RESTORE_PUBLISH_CONF=true;;
		-u|--use-cache) USE_CACHE=true;;
		-v|--verbose) VERBOSE="$1"; shift 1;;
		-z|--zero) ZERO_PUBLISH_CONF=true;;
		--) break;;
		*) fatal "panic: error parsing arg=${arg}";;
		esac
	done
	POSITIONAL_ARGS=("$@")

	log_info 1 "sourcing ${CONFIG_FILE}"
	source "${CONFIG_FILE}"

	if [[ "${NOW}" != "" ]]; then
		echo "assuming now is ${NOW} UTC"
	else
		NOW="$(date -u +%Y-%m-%dT%H:%M:%S)"
		echo "now is ${NOW} UTC"
	fi

	if ${USE_CACHE}; then
		if [[ ! -f "${MEAS_MD_ALL_JSON}" ]]; then
			fatal "${MEAS_MD_ALL_JSON} does not exist but --use-cache is specified"
		fi
		log_info 2 "using cached ${MEAS_MD_ALL_JSON}"
	fi

	if [[ ${#POSITIONAL_ARGS[@]} -gt 0 ]]; then
		log_info 0 "passing ${POSITIONAL_ARGS[*]} to ${PROCESS_MEASUREMENTS}"
	fi
}

main "$@"
