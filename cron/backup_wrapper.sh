#!/bin/bash

#
# This wrapper script is invoked by cron to (1) run $BACKUP_MEASUREMENTS,
# which backs up ClickHouse tables for newly completed measurements to
# $BACKUP_LOCAL_DIR, and (2) move the backup files to $BACKUP_NFS_DIR.
#

set -euo pipefail
export SHELLCHECK_OPTS="--exclude=SC1090"
shellcheck "$0"

readonly PROG_NAME="${0##*/}"
readonly TOPLEVEL="$(git rev-parse --show-toplevel)"
source "${TOPLEVEL}/pipeline/common.sh"
source "${TOPLEVEL}/conf/backup_settings.conf"

readonly VERBOSE=1


cleanup() {
	log_info 1 "removing /tmp/${PROG_NAME}.$$.*"
	rm -f "/tmp/${PROG_NAME}.$$."*
	log_info 1 "exited"
	log_line
}
trap cleanup EXIT

main() {
	local tmp_meas_before
	local tmp_meas_after
	local tmp_meas_backedup

	log_info 1 "started VERBOSE=${VERBOSE}"

	tmp_meas_before="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	cp "${BACKEDUP_MEAS_UUIDS}" "${tmp_meas_before}"
	backup_measurements
	tmp_meas_after="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	cp "${BACKEDUP_MEAS_UUIDS}" "${tmp_meas_after}"

	tmp_meas_backedup="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	grep -Fvx -f "${tmp_meas_before}" "${tmp_meas_after}" > "${tmp_meas_backedup}" || :
	if [[ -s "${tmp_meas_backedup}" ]]; then
		log_info 1 "$(wc -l < "${tmp_meas_backedup}") measurements backed up"
		move_files "${tmp_meas_backedup}"
	else
		log_info 1 "no measurements backed up"
	fi
}

#
# Backup measurements to $BACKUP_LOCAL_DIR.
#
backup_measurements() {
	#
	# Run $BACKUP_MEASUREMENTS.
	#
	setup_environment
	log_info 1 "${BACKUP_MEASUREMENTS} -v 2 -m 5"
	if "${BACKUP_MEASUREMENTS}" -v 2 -m 5; then
		log_info 0 "${BACKUP_MEASUREMENTS} exited successfully"
	else
		log_error "${BACKUP_MEASUREMENTS} did not exit successfully"
	fi
	if [[ -f "${BACKUP_LOCKFILE}" ]]; then
		log_lock_details "${BACKUP_LOCKFILE}"
		log_fatal "${BACKUP_LOCKFILE} still exists"
	fi
}

#
# Move newly backed up files from $BACKUP_LOCAL_DIR to $BACKUP_NFS_DIR.
#
move_files() {
	local meas_backedup_file="$1"
	local tmp_file
	local uuid

	#
	# Generate the list of files to move.
	#
	tmp_file="$(mktemp "/tmp/${PROG_NAME}.$$.XXXX")"
	while read -r uuid; do
		log_info 1 "(cd ${BACKUP_LOCAL_DIR} && find . \( -name \"*${uuid}*\" -o -name \"*${uuid//-/_}*\" \) -type f -print) >> ${tmp_file}"
		(cd "${BACKUP_LOCAL_DIR}" && find . \( -name "*${uuid}*" -o -name "*${uuid//-/_}*" \) -type f -print) >> "${tmp_file}"
	done < <(awk '{ print $1 }' "${meas_backedup_file}")

	#
	# User rsync to copy.  We cannot add --remove-source-files
	# because $BACKUP_NFS_USER does not have permission to remove.
	#
	chmod 644 "${tmp_file}"
	log_info 1 "sudo -u ${BACKUP_NFS_USER} rsync --progress --files-from=${tmp_file} ${BACKUP_LOCAL_DIR} ${BACKUP_NFS_DIR}"
	sudo -u "${BACKUP_NFS_USER}" rsync --progress --files-from="${tmp_file}" "${BACKUP_LOCAL_DIR}" "${BACKUP_NFS_DIR}"

	#
	# Remove from $BACKUP_LOCAL_DIR.
	#
	while read -r uuid; do
		log_info 1 "sudo rm -f ${BACKUP_LOCAL_DIR}/${uuid}"
		sudo rm -f "${BACKUP_LOCAL_DIR}/${uuid}"
	done < "${tmp_file}"
}

main "$@"
