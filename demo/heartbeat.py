"""Keep CDC's watermark advancing while a blocked decision is evaluated."""

import json
import threading
import time
from types import TracebackType
from typing import Any

from foremen.fdb_access import open_database, physical_key


class Heartbeat:
    def __init__(self, db: Any = None, interval: float = 1.0) -> None:
        self.db = db if db is not None else open_database()
        self.interval = interval
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.error: Exception | None = None

    def _run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.db[physical_key("heartbeat")] = json.dumps({"time": time.time()}).encode()
            except Exception as error:
                self.error = error
                return
            self.stop_event.wait(self.interval)

    def __enter__(self) -> "Heartbeat":
        self.thread = threading.Thread(target=self._run, daemon=True, name="foremen-heartbeat")
        self.thread.start()
        return self

    def __exit__(
        self, exception_type: type[BaseException] | None,
        exception: BaseException | None, traceback: TracebackType | None,
    ) -> None:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout=6)
        if exception is None and self.error is not None:
            raise RuntimeError("Heartbeat failed") from self.error


def main() -> None:
    """Run as an optional standalone heartbeat process."""
    with Heartbeat() as heartbeat:
        try:
            while heartbeat.error is None:
                time.sleep(1)
        except KeyboardInterrupt:
            return


if __name__ == "__main__":
    main()
