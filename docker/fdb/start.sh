#!/bin/sh
set -eu
mkdir -p /etc/foundationdb /var/fdb/data /var/fdb/logs
printf '%s\n' 'foremen:foremen@172.29.81.10:4500' > /etc/foundationdb/fdb.cluster
exec fdbserver -p 172.29.81.10:4500 -C /etc/foundationdb/fdb.cluster \
  -d /var/fdb/data -L /var/fdb/logs --knob_enable_native_cdc=1 \
  --knob_disable_posix_kernel_aio=1
