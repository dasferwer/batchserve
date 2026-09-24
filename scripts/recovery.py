import json
import os
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import httpx

from batchserve.db import connect

base = os.environ.get("API_URL", "http://localhost:8093")
count = int(os.environ.get("ROWS", "200000"))


def docker(*args):
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def wait(client, identity, predicate):
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        response = client.get(f"/jobs/{identity}")
        response.raise_for_status()
        state = response.json()
        if predicate(state):
            return state
        if state["status"] in {"failed", "cancelled"}:
            raise RuntimeError(state)
        time.sleep(0.05)
    raise TimeoutError("Задание не завершилось")


with (
    tempfile.TemporaryDirectory() as folder,
    httpx.Client(base_url=base, headers={"X-API-Key": "demo-a-key"}, timeout=180) as a,
    httpx.Client(base_url=base, headers={"X-API-Key": "demo-b-key"}, timeout=180) as b,
):
    path = Path(folder) / "large.csv"
    with path.open("w") as stream:
        stream.write("id,x1,x2\n")
        for i in range(count):
            stream.write(f"{i},1.0,-0.5\n")
    docker("stop", "worker")
    try:
        with path.open("rb") as stream:
            response = a.post(
                "/jobs",
                headers={"Idempotency-Key": str(uuid.uuid4())},
                files={"file": ("large.csv", stream)},
                data={"model": "linear-v1"},
            )
        response.raise_for_status()
        large = response.json()["id"]
        response = b.post(
            "/jobs",
            headers={"Idempotency-Key": str(uuid.uuid4())},
            files={"file": ("small.csv", b"id,x1,x2\nsmall,1,2\n")},
            data={"model": "linear-v2"},
        )
        response.raise_for_status()
        small = response.json()["id"]
        docker("start", "worker")
        wait(b, small, lambda state: state["status"] == "completed")
        large_state = a.get(f"/jobs/{large}").json()
        assert large_state["status"] == "running"
        wait(a, large, lambda state: state["batches"].get("completed", 0) >= 1)
        docker("kill", "-s", "SIGKILL", "worker")
        with connect() as conn:
            saved = conn.execute(
                "SELECT number,output_key FROM batches WHERE job_id=%s AND status='completed' ORDER BY number",
                (large,),
            ).fetchall()
        assert saved
        docker("start", "worker")
        final = wait(a, large, lambda state: state["status"] == "completed")
        with connect() as conn:
            after = conn.execute(
                "SELECT number,output_key FROM batches WHERE job_id=%s ORDER BY number", (large,)
            ).fetchall()
        assert after[: len(saved)] == saved
        seen = 0
        with a.stream("GET", f"/jobs/{large}/result") as response:
            response.raise_for_status()
            for line in response.iter_lines():
                row = json.loads(line)
                assert row["id"] == str(seen)
                assert row["prediction"] == 1
                seen += 1
        assert seen == count
        print(
            json.dumps(
                {
                    "rows": seen,
                    "small_finished_before_large": True,
                    "preserved_batches": len(saved),
                    "completed_batches": final["batches"]["completed"],
                },
                indent=2,
            )
        )
    finally:
        docker("start", "worker")
