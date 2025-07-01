#!/bin/bash

#
# This wrapper script is triggered by cron to handle measurement exports.
# It calls the $EXPORT_MEASUREMENTS script, which checks for new
# measurements that haven't been exported yet and exports them accordingly.
#

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC1090"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
readonly TOPLEVEL="$(git rev-parse --show-toplevel)"
source "${TOPLEVEL}/tools/common.sh"
source "${TOPLEVEL}/conf/export_settings.conf"

readonly VERBOSE=1


cleanup() {
	log_info 2 rm -f "/tmp/${PROG_NAME}.$$."*
	rm -f "/tmp/${PROG_NAME}.$$."*
	log_line
}
trap cleanup EXIT

main() {
	local tmp_meas_before
	local tmp_meas_after
	local tmp_meas_exported

	log_info 1 "started VERBOSE=${VERBOSE}"

	tmp_meas_before="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	cp "${EXPORTED_MEAS}" "${tmp_meas_before}"
	export_measurements
	tmp_meas_after="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	cp "${EXPORTED_MEAS}" "${tmp_meas_after}"

	tmp_meas_exported="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	grep -Fvx -f "${tmp_meas_before}" "${tmp_meas_after}" > "${tmp_meas_exported}" || :
	if [[ -s "${tmp_meas_exported}" ]]; then
		log_info 1 "$(wc -l < "${tmp_meas_exported}") measurements exported"
		move_measurements "${tmp_meas_exported}"
	else
		log_info 1 "no measurements exported"
	fi

	log_info 1 "exited"
}

#
# Export measurements to $EXPORTS_DIR.
#
export_measurements() {
	export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin
	export SHELL=/bin/bash

	IRIS_PASSWORD="$(sops -d "${SECRETS_YML}" | yq e ".services.production.api[] | select(.user == \"${IRIS_USERNAME}\") | .pass" -)"
	if [[ "${IRIS_PASSWORD}" == "" ]]; then
		fatal "failed to get IRIS_PASSWORD"
	fi
	export IRIS_PASSWORD
	CLICKHOUSE_PASSWORD="$(sops -d "${SECRETS_YML}" | yq e ".services.production.clickhouse[] | select(.user == \"${CLICKHOUSE_USERNAME}\") | .pass" -)"
	if [[ "${CLICKHOUSE_PASSWORD}" == "" ]]; then
		fatal "failed to get CLICKHOUSE_PASSWORD"
	fi
	export CLICKHOUSE_PASSWORD

	log_info 1 "${EXPORT_MEASUREMENTS} -v 2"
	"${EXPORT_MEASUREMENTS}" -v 2
	if [[ -f "${EXPORT_LOCKFILE}" ]]; then
		log_lock_details "${EXPORT_LOCKFILE}"
		fatal "${EXPORT_LOCKFILE} still exists"
	fi
}

#
# Move newly exported measurements from $EXPORTS_DIR to $FTP_DIR.
# XXX We are not deleting the files in $EXPORTS_DIR yet.
#
move_measurements() {
	local meas_exported_file="$1"
	local tmp_file
	local uuid

	tmp_file="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	while read -r uuid; do
		(cd "${EXPORTS_DIR}" && find . \( -name "*${uuid}*" -o -name "*${uuid//-/_}*" \) -print) >> "${tmp_file}"
	done < <(cat "${meas_exported_file}")

	log_info 1 "sudo rsync --progress --files-from=${tmp_file} ${EXPORTS_DIR} ${FTP_DIR}"
	sudo rsync --progress --files-from="${tmp_file}" "${EXPORTS_DIR}" "${FTP_DIR}"
}

main "$@"
