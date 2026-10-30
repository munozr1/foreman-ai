"""Write and verify a known mutation set while the host restarts the bridge."""

import argparse
import json
import sys
import time

from foremen.fdb_access import open_database, physical_key
from foremen.recorder import clickhouse_client


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["write", "verify"])
    parser.add_argument("--id", required=True)
    parser.add_argument("--start", type=int, default=0)
    args = parser.parse_args()
    if args.phase == "write":
        database = open_database()
        expected = []
        for index in range(args.start, args.start + 25):
            key = f"restart/{args.id}/{index}"
            value = json.dumps({"writer": "restart-test", "index": index})
            transaction = database.create_transaction()
            transaction.options.set_timeout(5000)
            transaction[physical_key(key)] = value.encode()
            transaction.commit().wait()
            expected.append([key, transaction.get_committed_version(), value])
        print(json.dumps(expected))
        return
    expected = {tuple(row) for row in json.load(sys.stdin)}
    client = clickhouse_client()
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline:
            result = client.query(
                "SELECT DISTINCT key, commit_version, value FROM mutations "
                "WHERE startsWith(key, {prefix:String}) AND op_code = 0",
                parameters={"prefix": f"restart/{args.id}/"},
            )
            actual = {tuple(row) for row in result.result_rows}
            if actual == expected:
                print(f"Bridge restart PASS: all {len(expected)} exact mutations retained; no gaps")
                return
            time.sleep(0.1)
        raise AssertionError(f"Missing mutations: {expected - actual}; unexpected: {actual - expected}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
