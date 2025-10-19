#!/bin/sh

set -e

# Build the binary
./build.sh

# Example 1: Basic usage with type specification
echo "--- Example 1: Basic usage with type specification ---"
jq -c -n '{timestamp:"2025-10-17T00:47:02.012345Z", status:200, body:"apt update done"}' |
	./arrow-json2csv2schema \
		-types status:float64 \
		-pretty |
		jq

# Example 2: Specifying types for user data
echo "\n--- Example 2: Specifying types for user data ---"
cat <<EOF | ./arrow-json2csv2schema -types user_id:string -pretty | jq
{"user_id": 12345, "username": "testuser", "is_active": true}
EOF

# Example 3: Including a subset of columns
echo "\n--- Example 3: Including a subset of columns ---"
cat <<EOF | ./arrow-json2csv2schema --include foo,qux -pretty | jq
{"foo": "bar", "baz": 123, "qux": true}
EOF
