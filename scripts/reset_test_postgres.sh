#!/usr/bin/env bash
set -euo pipefail

if ! command -v psql >/dev/null 2>&1; then
  echo "psql is required but was not found in PATH." >&2
  exit 1
fi

host="${POSTGRES_HOST:-localhost}"
port="${POSTGRES_PORT:-5432}"
user="${POSTGRES_USER:-postgres}"
password="${POSTGRES_PASSWORD:-postgres}"
admin_db="${POSTGRES_ADMIN_DB:-postgres}"
test_db="${TOWER_BPMN_TEST_POSTGRES_DB:-tower_bpmn_test}"

run_tests=0
if [[ "${1:-}" == "--run-tests" ]]; then
  run_tests=1
  shift
fi

export PGPASSWORD="${password}"

psql -h "${host}" -p "${port}" -U "${user}" -d "${admin_db}" -v ON_ERROR_STOP=1 <<SQL
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '${test_db}'
  AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS "${test_db}";
CREATE DATABASE "${test_db}";
SQL

test_url="postgresql://${user}:${password}@${host}:${port}/${test_db}"
export TOWER_BPMN_TEST_POSTGRES_URL="${test_url}"

echo "Reset complete."
echo "TOWER_BPMN_TEST_POSTGRES_URL=${TOWER_BPMN_TEST_POSTGRES_URL}"

if [[ "${run_tests}" -eq 1 ]]; then
  cargo test --features postgres "$@"
fi
