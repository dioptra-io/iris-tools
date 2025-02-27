#!/bin/bash

set -eu
set -o pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

readonly PROG_NAME="${0##*/}"
CONFIG_FILE="$(git rev-parse --show-toplevel)/conf/tables.conf" # --config
LIST_FILES=false # --list
NOW="" # --now
RESTORE_FILES=false # --restore
USE_CACHE=false # --use-cache

#
# These variables will be initialized with values in $PUBLISH_CONF
# (which is defined in $CONFIG_FILE).  We set them to an invalid value
# for sanity checks in source_publish_conf().
#
PUBLISHING_DISABLED="X"        # disable publishing
TOT_NUM_PUBLISHED="X"          # total number of published measurements
NUM_PUBLISHED_CUR_SET="X"      # number of measurements published in the current set
DATETIME_LAST_PUBLISHED="X"    # date and time of the last published measurement
UUID_LAST_PUBLISHED="X"        # uuid of the last published measurement
NUM_DAYS_TO_WAIT="X"           # number of days to wait before publishing the next set
readonly PUBLISH_VARS=(
	"PUBLISHING_DISABLED"
	"TOT_NUM_PUBLISHED"
	"NUM_PUBLISHED_CUR_SET"
	"DATETIME_LAST_PUBLISHED"
	"UUID_LAST_PUBLISHED"
	"NUM_DAYS_TO_WAIT"
)


usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} --help
	${PROG_NAME} --list
	${PROG_NAME} [-c <config>] [-n <now>]
	-c, --config	configuration file (default ${CONFIG_FILE})
	-h, --help	print help message and exit
	-l, --list	list publishing variables and their values, then exit
	-n, --now	assume now is the given arg in the format yyyy-mm-ddThh:mm:ssZ
	-r, --restore	restore ${PUBLISH_CONF}, then exit
	-u, --use-cache	use the ${MEAS_MD_ALL_JSON} file in cache directory
EOF
	exit "${exit_code}"
}

main() {
	local datetime_last_published_secs
	local now_secs
	local diff_days
	local new_meas_uuid

	parse_args "$@"
	info "sourcing ${CONFIG_FILE}"
	# shellcheck disable=SC1090
	source "${CONFIG_FILE:?unbound CONFIG_FILE}"
	if ${USE_CACHE} && [[ ! -f "${MEAS_MD_ALL_JSON}" ]]; then
		error "${MEAS_MD_ALL_JSON} does not exist but --use-cache is specified"
	fi

	if ${RESTORE_FILES}; then
		restore_publish_conf
	fi
	source_publish_conf
	if ${LIST_FILES} || ${RESTORE_FILES}; then
		return 0
	fi
	# sanity check
	if [[ ${NUM_PUBLISHED_CUR_SET} -gt ${MAX_NUM_PER_SET} ]]; then
		error "number of published measurements in the current set (${NUM_PUBLISHED_CUR_SET}) exceeds $MAX_NUM_PER_SET"
	fi
	if ${PUBLISHING_DISABLED}; then
		echo "publishing is disabled"
		return 0
	fi

	# this is mostly for debugging
	if [[ "${NOW}" != "" ]]; then
		echo "assuming now is ${NOW}"
	fi

	#
	# If we are not in the middle of publishing a set and at least
	# $NUM_DAYS_TO_WAIT days have not passed since the last publish
	# date, there is nothing to do.
	#
	if [[ ${NUM_PUBLISHED_CUR_SET} -eq ${MAX_NUM_PER_SET} ]]; then
		datetime_last_published_secs=$(date -u -d "${DATETIME_LAST_PUBLISHED}" +%s)
		if [[ "${NOW}" != "" ]]; then
			now_secs=$(date -u -d "${NOW}" +%s)
		else
			now_secs=$(date -u +%s)
		fi
		diff_days=$(( (now_secs - datetime_last_published_secs) / (24 * 60 * 60) ))
		info "${diff_days} day(s) since the last measurement published on ${DATETIME_LAST_PUBLISHED}"
		if [[ ${diff_days} -lt ${NUM_DAYS_TO_WAIT} ]]; then
			return 0
		fi
		#
		# We have waited at least $NUM_DAYS_TO_WAIT days and
		# are now ready to start publishing a new set.
		#
		NUM_PUBLISHED_CUR_SET=0
	fi

	#
	# We are either at the beginning or in the middle of publishing
	# a set.  Is a new measurement ready?
	#
	info "checking if a new measurement is ready for publishing"
	# shellcheck disable=SC2016
	new_meas_uuid="$(new_measurement)"
	if [[ "${new_meas_uuid}" == "" ]] ; then
		echo "no new measurement to publish"
		return 0
	fi
	echo "${new_meas_uuid} can be published"

	#
	# Now publish the measurement and update the publish configuration
	# file if it succeeds.
	#
	# XXX if ./process_tables publish "${new_meas_uuid}"; then
	#	update_publish.conf
	# fi
	update_publish_conf
}

source_publish_conf() {
	local pub_conf_path="${CONFIG_FILE%/*}/${PUBLISH_CONF}"
	local var

	info "sourcing publish configuration file ${pub_conf_path}"
	# shellcheck disable=SC1090
	source "${pub_conf_path}"
	# sanity check
	for var in "${PUBLISH_VARS[@]}"; do
		if [[ "${!var}" == "X" ]]; then
			error "${var} is not initialized from ${pub_conf_path}"
		fi
		printf "%-24s %s\n" "${var}" "${!var}"
	done
	# sanity check
	if [[ ${NUM_PUBLISHED_CUR_SET} -gt ${MAX_NUM_PER_SET} ]]; then
		error "${NUM_PUBLISHED_CUR_SET} is greater than ${MAX_NUM_PER_SET}"
	fi
}

update_publish_conf() {
	local pub_conf_path="${CONFIG_FILE%/*}/${PUBLISH_CONF}"
	local var

	_=$(( NUM_PUBLISHED_CUR_SET++ ))
	if [[ "${NOW}" != "" ]]; then
		DATETIME_LAST_PUBLISHED="$(date -u -d "${NOW}" +"%Y-%m-%dT%H:%M:%SZ")"
	else
		DATETIME_LAST_PUBLISHED="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
	fi
	UUID_LAST_PUBLISHED="${new_meas_uuid}"
	if [[ ${NUM_PUBLISHED_CUR_SET} -eq ${MAX_NUM_PER_SET} ]]; then
		# force more entropy with dd
		NUM_DAYS_TO_WAIT=$(( $(dd if=/dev/urandom bs=2 count=1 2>/dev/null | od -An -N2 -i | tr -d ' ') % (MAX_DAYS_TO_WAIT - MIN_DAYS_TO_WAIT) + MIN_DAYS_TO_WAIT ))
		echo "XXX ${NUM_DAYS_TO_WAIT}"
	else
		NUM_DAYS_TO_WAIT=0
	fi
	_=$(( TOT_NUM_PUBLISHED++ ))

	info "updating publish configuration file ${pub_conf_path}"
	mv "${pub_conf_path}" "${pub_conf_path}.bak"
	for var in "${PUBLISH_VARS[@]}"; do
		printf "%-24s %s\n" "${var}" "${!var}"
		echo "${var}=${!var}" >> "${pub_conf_path}"
	done
}

restore_publish_conf() {
	local pub_conf_path="${CONFIG_FILE%/*}/${PUBLISH_CONF}"

	if [[ -f "${pub_conf_path}.bak" ]]; then
		info "restoring publish configuration file ${pub_conf_path}"
		mv "${pub_conf_path}.bak" "${pub_conf_path}"
	else
		echo "${pub_conf_path}.bak does not exist"
	fi
}

#
# XXX The Iris API incorrectly returns a measurement as "finished" as
#     soon as one agent completes its task, even though the measurement is
#     still ongoing.  We need to handle this.
#
new_measurement() {
	local cmd=("irisctl" "list" "--all-users" "--state" "finished" "--tag" "zeph-gcp-daily")
	local tmp_file="/tmp/${PROG_NAME}.$$"

	if [[ "${NOW}" != "" ]]; then
		cmd+=("--before" "${NOW}")
	fi
	if ${USE_CACHE}; then
		cmd+=("${MEAS_MD_ALL_JSON}")
	fi
	info "${cmd[@]}"
	if ! "${cmd[@]}" > "${tmp_file}"; then
		error "failed to execute: ${cmd[*]}"
		# XXX Why do we need an explicit exit when we have set -e?
		exit 1
	fi
	if [[ "${UUID_LAST_PUBLISHED}" == "" ]]; then
		tail -n 1 "${tmp_file}" | awk '{ print $1 }'
	else
		awk -v pat="${UUID_LAST_PUBLISHED}" '$0 ~ pat { if (getline) print $1; else exit }' "${tmp_file}"
	fi
}

parse_args() {
	local args
	local arg

	if ! args="$(getopt \
			--options "c:hln:ru" \
			--longoptions "config: help list now: restore use-cache" \
			-- "$@")"; then
		usage 1
	fi
	eval set -- "${args}"
	
	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-c|--config) CONFIG_FILE="$1"; shift 1;;
		-h|--help) usage 0;;
		-l|--list) LIST_FILES=true;;
		-n|--now) NOW="$1"; shift 1;;
		-r|--restore) RESTORE_FILES=true;;
		-u|--use-cache) USE_CACHE=true;;
		--) break;;
		*) error "internal error parsing arg=${arg}";;
		esac
	done

	if [[ $# -ne 0 ]]; then
		error "extra command line arguments: $*"
	fi
}

info() {
	(1>&2 echo -e "\033[1;34minfo: $*\033[0m")
}

error() {
	(1>&2 echo "error: $*")
	return 1
}

main "$@"
