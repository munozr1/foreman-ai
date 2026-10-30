"""Host-side durability test; no privileged Docker socket inside the app."""

import json
import subprocess
import uuid


def compose(*args: str, capture: bool = False, stdin: str | None = None) -> str:
    result = subprocess.run(
        ["docker", "compose", *args], check=True, text=True,
        stdout=subprocess.PIPE if capture else None, input=stdin,
    )
    return result.stdout or ""


def main() -> None:
    run_id = uuid.uuid4().hex
    command = ["run", "--rm", "--no-deps", "-T", "app", "python",
               "scripts/restart_probe.py"]
    before = json.loads(compose(*command, "write", "--id", run_id, capture=True))
    compose(*command, "verify", "--id", run_id, stdin=json.dumps(before))
    compose("stop", "bridge")
    try:
        during = json.loads(compose(*command, "write", "--id", run_id,
                                    "--start", "25", capture=True))
    finally:
        # Restore service even if a write fails; never discard persisted state.
        compose("up", "--wait", "bridge")
    after = json.loads(compose(*command, "write", "--id", run_id,
                               "--start", "50", capture=True))
    compose(*command, "verify", "--id", run_id, stdin=json.dumps(before + during + after))


if __name__ == "__main__":
    main()
