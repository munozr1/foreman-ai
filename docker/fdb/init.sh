#!/bin/sh
set -eu
# Configure only an unconfigured database; a running volume is left intact.
status=$(fdbcli -C /etc/foundationdb/fdb.cluster --exec 'status minimal' --timeout 10 2>&1 || true)
case "$status" in
  *'The database is available'*) exit 0 ;;
esac
fdbcli -C /etc/foundationdb/fdb.cluster --exec 'configure new single ssd' --timeout 30
fdbcli -C /etc/foundationdb/fdb.cluster --exec 'status minimal' --timeout 30
