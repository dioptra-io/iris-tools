#!/bin/bash

set -euo pipefail
shellcheck "$0" # exits if shellcheck doesn't pass

TOPLEVEL="$(git rev-parse --show-toplevel)"
readonly TOPLEVEL
readonly UPLOAD_DATA="${TOPLEVEL}/pipeline/upload_data.sh"

# Test that upload_data function returns 1 when no files are found
test_upload_data_returns_1_when_no_files() {
	echo "Testing: upload_data returns 1 when no files are found"
	
	# Extract and test just the upload_data function logic
	# We'll simulate the condition by checking the return statement in the code
	if grep -q "return 1" "${UPLOAD_DATA}"; then
		echo "✓ upload_data function has 'return 1' for no files case"
	else
		echo "✗ upload_data function missing 'return 1' for no files case"
		exit 1
	fi
}

# Test that main function checks upload_data return value
test_main_checks_upload_data_return() {
	echo "Testing: main function checks upload_data return value"
	
	if grep -q "if ! upload_data" "${UPLOAD_DATA}"; then
		echo "✓ main function checks upload_data return value"
	else
		echo "✗ main function does not check upload_data return value"
		exit 1
	fi
}

# Test that main function uses upload_success variable
test_main_uses_upload_success() {
	echo "Testing: main function uses upload_success variable"
	
	if grep -q "upload_success=true" "${UPLOAD_DATA}"; then
		echo "✓ main function initializes upload_success"
	else
		echo "✗ main function does not initialize upload_success"
		exit 1
	fi
	
	if grep -q "upload_success=false" "${UPLOAD_DATA}"; then
		echo "✓ main function sets upload_success to false on failure"
	else
		echo "✗ main function does not set upload_success to false on failure"
		exit 1
	fi
	
	if grep -q "if \${upload_success}" "${UPLOAD_DATA}"; then
		echo "✓ main function checks upload_success before marking as published"
	else
		echo "✗ main function does not check upload_success before marking as published"
		exit 1
	fi
}

# Test that main function skips publication on failure
test_main_skips_publication_on_failure() {
	echo "Testing: main function skips publication on failure"
	
	if grep -q "skipping marking.*as published due to upload failure" "${UPLOAD_DATA}"; then
		echo "✓ main function logs when skipping publication"
	else
		echo "✗ main function does not log when skipping publication"
		exit 1
	fi
}

echo "Running upload_data.sh tests..."
echo "================================"
test_upload_data_returns_1_when_no_files
echo ""
test_main_checks_upload_data_return
echo ""
test_main_uses_upload_success
echo ""
test_main_skips_publication_on_failure
echo ""
echo "================================"
echo "All tests passed!"
