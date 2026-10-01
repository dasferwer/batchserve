"""Проверка реальной очистки S3 после отклонения, отказа хранилища и SIGKILL API."""

import json
import os
import subprocess
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx

from batchserve.db import connect

BASE = os.environ.get("API_URL", "http://localhost:8093")
HEADERS = {"X-API-Key": "demo-a-key"}


def docker(*args):
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def post(path, key):
    with (
        path.open("rb") as source,
        httpx.Client(base_url=BASE, headers=HEADERS, timeout=180) as client,
    ):
        return client.post(
            "/jobs",
            headers={"Idempotency-Key": str(key)},
            files={"file": ("input.csv", source)},
            data={"model": "linear-v1"},
        )


def wait_api():
    deadline = time.monotonic() + 60
    with httpx.Client(base_url=BASE, headers=HEADERS, timeout=5) as client:
        while time.monotonic() < deadline:
            try:
                if client.get("/health").status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
    raise TimeoutError("API не восстановилось")


def wait_stage(key):
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        with connect() as conn:
            row = conn.execute(
                "SELECT s.* FROM staging_uploads s WHERE request_key=%s AND EXISTS "
                "(SELECT 1 FROM staging_parts p WHERE p.staging_id=s.id)",
                (key,),
            ).fetchone()
        if row:
            assert row["status"] == "active" and row["job_id"] is None, row
            return row
        time.sleep(0.02)
    raise TimeoutError("Запрос не зафиксировал первую часть")


def assert_object_exists(key):
    docker(
        "exec",
        "-T",
        "api",
        "python",
        "-c",
        "import sys; from batchserve.storage import client,bucket; "
        "client().head_object(Bucket=bucket(),Key=sys.argv[1])",
        key,
    )


def wait_object_exists(key, seconds=60):
    probe = """
import sys
from botocore.exceptions import ClientError
from batchserve.storage import client, bucket
try:
    client().head_object(Bucket=bucket(), Key=sys.argv[1])
except ClientError as exc:
    if exc.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 404:
        sys.exit(75)
    raise
"""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            docker("exec", "-T", "api", "python", "-c", probe, key)
            return
        except subprocess.CalledProcessError as exc:
            if exc.returncode != 75:
                raise
        time.sleep(0.1)
    raise TimeoutError("Первая часть не появилась в S3")


def verify_cleanup(key, accepted_job, accepted_key):
    with connect() as conn:
        stage = conn.execute(
            "SELECT * FROM staging_uploads WHERE request_key=%s", (key,)
        ).fetchone()
        assert stage and stage["job_id"] is None
        assert conn.execute("SELECT id FROM jobs WHERE request_key=%s", (key,)).fetchone() is None
        # Проверка не ждёт полный TTL: истечение задаётся только нашим реестрам.
        conn.execute(
            "UPDATE staging_uploads SET expires_at=clock_timestamp()-interval '1 second' "
            "WHERE id=%s OR job_id=%s",
            (stage["id"], accepted_job),
        )
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        with connect() as conn:
            state = conn.execute(
                "SELECT status FROM staging_uploads WHERE id=%s", (stage["id"],)
            ).fetchone()
        if state["status"] == "cleaned":
            break
        time.sleep(0.1)
    else:
        raise TimeoutError("Фоновая очистка staging не завершилась")
    docker(
        "exec",
        "-T",
        "api",
        "python",
        "-c",
        "import sys; from batchserve.storage import client,bucket; "
        "assert not client().list_objects_v2(Bucket=bucket(),Prefix='inputs/'+sys.argv[1]+'/').get('Contents')",
        str(stage["id"]),
    )
    assert_object_exists(accepted_key)


def main():
    wait_api()
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        valid, invalid, large = root / "valid.csv", root / "invalid.csv", root / "large.csv"
        valid.write_bytes(b"id,x1,x2\naccepted,1,2\n")
        response = post(valid, uuid.uuid4())
        response.raise_for_status()
        accepted_job = uuid.UUID(response.json()["id"])
        with connect() as conn:
            accepted_key = conn.execute(
                "SELECT input_key FROM batches WHERE job_id=%s LIMIT 1", (accepted_job,)
            ).fetchone()["input_key"]
        assert_object_exists(accepted_key)
        invalid.write_text(
            "id,x1,x2\n" + "".join(f"{i},1,2\n" for i in range(1000)) + "bad,NaN,1\n"
        )
        key = uuid.uuid4()
        assert post(invalid, key).status_code == 422
        verify_cleanup(key, accepted_job, accepted_key)

        with large.open("w") as target:
            target.write("id,x1,x2\n")
            for number in range(1_000_000):
                target.write(f"{number},1,2\n")
        for failure in ("s3_down", "api_sigkill"):
            key = uuid.uuid4()
            try:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    task = pool.submit(post, large, key)
                    stage = wait_stage(key)
                    with connect() as conn:
                        first = conn.execute(
                            "SELECT input_key FROM staging_parts WHERE staging_id=%s ORDER BY number LIMIT 1",
                            (stage["id"],),
                        ).fetchone()["input_key"]
                    # Реестр фиксируется до PUT: отдельно ждём физический объект.
                    wait_object_exists(first)
                    if failure == "s3_down":
                        docker("stop", "storage")
                        assert task.result(timeout=120).status_code == 503
                    else:
                        docker("kill", "-s", "SIGKILL", "api")
                        try:
                            task.result(timeout=30)
                        except httpx.HTTPError:
                            pass
                        else:
                            raise AssertionError("Запрос успел завершиться до SIGKILL")
            finally:
                docker("up", "-d", "--no-deps", "api", "storage")
                wait_api()
            verify_cleanup(key, accepted_job, accepted_key)
        print(
            json.dumps(
                {
                    "ok": True,
                    "invalid_tail_cleaned": True,
                    "s3_failure_cleaned": True,
                    "api_sigkill_cleaned": True,
                    "accepted_input_preserved": True,
                    "orphan_prefixes_empty": True,
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()
