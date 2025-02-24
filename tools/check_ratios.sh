#!/bin/bash

#
# This script scans the log lines of $CONTAINER_NAME to compute and
# print the ratio between two variables (by default, $DEF_VAR1 and
# $DEF_VAR2).
#
# Because variables corresponding to the same time are not printed
# on the same line, this script first concatenates all lines that
# have the exact same date and time into a single line and then parses
# out var1 and var2 to compute and print their ratio.
#

set -eu
set -o pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

readonly PROG_NAME="${0##*/}"
# The following variables can be set via the environment.
: "${CONTAINER_NAME:="iris-agent"}"
: "${START_DATE:="2025-01-30T00:00:00Z"}"
: "${END_DATE:="2025-01-31T00:00:00Z"}"

readonly VARIABLES=(
	"pcap_received"
	"pcap_dropped"
	"pcap_interface_dropped"
	"probes_read"
	"packets_sent"
	"packets_failed"
	"filtered_low_ttl"
	"filtered_high_ttl"
	"filtered_prefix_excl"
	"filtered_prefix_not_incl"
	"average_rate"
	"average_utilization"
	"packets_received"
	"packets_received_invalid"
	"icmp_distinct_incl_dest"
	"icmp_distinct_excl_dest"
)
readonly DEF_VAR1="packets_received"
readonly DEF_VAR2="pcap_received"

AGENT_HOSTNAME="" # -a
LOGS_FILE="" # -l
VAR1="${DEF_VAR1}" # 1st argument
VAR2="${DEF_VAR2}" # 2nd argument


usage() {
	local exit_code="$1"

	cat <<EOF
usage:
	${PROG_NAME} [-h] [-a <agent-hostname>] [-l <logs-file>]  [<var1> <var2>]
	${PROG_NAME} --vars
	-h, --help      print help message and exit
	-a, --agent	agent hostname (e.g., iris-us-east4)
	-l, --logs	path to logs file
	    --vars	print all variables names
	var1            variable 1 (defult: ${DEF_VAR1})
	var2            variable 1 (defult: ${DEF_VAR2})

environment variables:
	CONTAINER_NAME (default: ${CONTAINER_NAME})
	START_DATE (default: ${START_DATE})
	END_DATE (default: ${END_DATE})

examples:
	$ ./${PROG_NAME} -a iris-us-east4 -l iris-us-east4.txt
	$ ./${PROG_NAME} -l iris-us-east4.txt
	$ ./${PROG_NAME} --vars
	$ ./${PROG_NAME} -l iris-us-east4.txt packets_received packets_received_invalid
EOF
	exit "${exit_code}"
}

main() {
	local tmp_file1
	local tmp_file2

	parse_args "$@"

	check_logs_file "${LOGS_FILE}"
	if [[ ! -s "${LOGS_FILE}" ]]; then
		(1>&2 echo "warning: ${LOGS_FILE} is empty")
	fi

	tmp_file1="/tmp/check_ratios.$$.1"
	(1>&2 echo "reversing and cleaning ${LOGS_FILE} and saving in ${tmp_file1}")
	reverse_and_clean "${LOGS_FILE}" > "${tmp_file1}"

	tmp_file2="/tmp/check_ratios.$$.2"
	(1>&2 echo "processing ${tmp_file1} and saving in ${tmp_file2}")
	concat_by_vars "${VAR1}" "${VAR2}" < "${tmp_file1}" > "${tmp_file2}"

	#
	# Although it's better to use concat_by_vars, here's another
	# way to concatenate the lines based on their datetime and a
	# time delta which could be 0 milliseconds.  Let's leave these
	# commented out lines here for future reference.
	#
	#echo "processing ${tmp_file1} and saving in ${tmp_file2}"
	#concat_by_time 0 < "${tmp_file1}" > "${tmp_file2}"

	tmp_file3="/tmp/check_ratios.$$.3"
	(1>&2 echo "checking ratios in ${tmp_file2} and saving in ${tmp_file3}")
	check_ratios "${VAR1}" "${VAR2}" < "${tmp_file2}" > "${tmp_file3}"
	(1>&2 echo "remove temporary files if you don't need them: rm -f ${tmp_file1} ${tmp_file2} ${tmp_file3}")
}

parse_args() {
	local args
	local arg

	if ! args="$(getopt \
			--options "a:hl:" \
			--longoptions "agent: help logs: vars" \
			-- "$@")"; then
		usage 1
	fi
        eval set -- "${args}"
	
	if [[ $# -eq 1 ]]; then
		usage 1
	fi

	while :; do
		arg="$1"
		shift
		case "${arg}" in
		-a|--agent) AGENT_HOSTNAME="$1"; shift 1;;
		-h|--help) usage 0;;
		-l|--logs) LOGS_FILE="$1"; shift 1;;
		--vars) printf "%s\n" "${VARIABLES[@]}"; exit 0;;
		--) break;;
		*) echo "internal error parsing arg=${arg}"; usage 1;;
		esac
	done

	if [[ $# -ne 0 && $# -ne 2 ]]; then
		usage 1
	fi
	if [[ $# -eq 2 ]]; then
		VAR1="$1"
		VAR2="$2"
	fi
}

#
# Check if $logs_file exists and can be used; otherwise, generate it.
#
check_logs_file() {
	local logs_file="$1"
	local answer="n"

	if [[ -f "${logs_file}" ]]; then
		read -r -p "use the existing ${logs_file}? [Y/n] " answer
		if [[ "${answer}" == "" || "${answer,,}" == "y" ]]; then
			echo "ignoring --agent ${AGENT_HOSTNAME} because logs file ${logs_file} already exists"
			return 0
		fi
	fi
	if [[ "${AGENT_HOSTNAME}" == "" ]]; then
		echo "you have to specify an agent hostname (--agent) to create a new ${logs_file}"
		return 1
	fi

	export CONTAINER_NAME
	export START_DATE
	export END_DATE
	export PATTERN="${AGENT_HOSTNAME}"
	./scan_logs.sh > "${logs_file}"
}

#
# The first pass on the logs file reverses its chornological order
# and removes excessive stuff before the date (container_id, filename,
# ...) and variables that their value is 0.
#
reverse_and_clean() {
	local logs_file="$1"

	tac "${logs_file}" | 
	sed -n -E -e 's/.*\[(202[0-9].*)\] \[info\] /\1 /' -e 's/([a-zA-Z_]+=0)//g' -e 's/([a-zA-Z_]+=[1-9][0-9]*)/\1 /gp'
}

#
# Concatenate all lines between $from and $to and print as one line.
#
concat_by_vars() {
	local from="$1"
	local to="$2"

	awk -v from="${from}" -v to="${to}" '{
		if ($0 ~ from) {
			between = 1;
			line = $0;
		}
		if ($0 ~ to && between == 1) {
			line = line " " $0;
			gsub(/[ \t]+/, " ", line);
			print line;
			between = 0;
			line = "";
		} else if (between == 1) {
			# skip date and time
			for (i = 3; i <= NF; i++) {
				line = line " " $i;
			}
		}
	} 
	END {
		if (line != "") {
			gsub(/[ \t]+/, " ", line);
			print line;
		}
	}'
}

#
# Concatenate all lines that their time is less than $ms milliseconds
# apart and print as one line.
# XXX For non-zero $ms, the following awk script does not handle midnight.
#
concat_by_time() {
	local ms="$1"

	awk -v ms="${ms}" '
	{
		cur_date = $1;
		split($2, time_ms, ".");
		cur_time = time_ms[1];
		cur_ms = time_ms[2];
		if (prev_date == "" || cur_date != prev_date || cur_time != prev_time || (cur_ms - prev_ms) > ms) {
			if (prev_timestamp_line != "") {
				gsub(/[ \t]+/, " ", prev_timestamp_line);
				print prev_timestamp_line;
			}
			prev_date = cur_date;
			prev_time = cur_time;
			prev_ms = cur_ms;
			prev_timestamp_line = $0;
		} else {
			for (i = 3; i <= NF; i++) {
				prev_timestamp_line = prev_timestamp_line " " $i;
			}
		}
	} 
	END {
		if (prev_timestamp_line != "") {
			gsub(/[ \t]+/, " ", prev_timestamp_line);
			print prev_timestamp_line;
		}
	}'
}

check_ratios() {
	local var1="$1"
	local var2="$2"

	awk -v var1="${var1}" -v var2="${var2}" '{
		print $0;
		var1_val = var2_val = 0;
		for (i = 3; i <= NF; i++) {
			if ($i ~ var1 "=") {
				var1_val = substr($i, index($i, "=") + 1);
			}
			if ($i ~ var2 "=") {
				var2_val = substr($i, index($i, "=") + 1);
			}
		}
		printf("%s %s %s=%d %s=%d", $1, $2, var1, var1_val, var2, var2_val);
		if (var2_val > 0) {
			printf(" ratio=%.03f", var1_val / var2_val);
		}
		printf("\n");
	}'
}

main "$@"
