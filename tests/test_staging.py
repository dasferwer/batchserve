import os
import signal
import subprocess
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pika
import psycopg
import pytest
from botocore.exceptions import EndpointConnectionError

from batchserve import api, storage
from batchserve.db import connect


@pytest.fixture
def objects(monkeypatch):
    saved = {}
    monkeypatch.setenv("AMQP_URL", "amqp://demo:demo@127.0.0.1:1/%2F")
    monkeypatch.setenv("S3_BUCKET", "batchserve-staging-test")

    def unavailable_broker(*args, **kwargs):
        raise pika.exceptions.AMQPConnectionError()

    monkeypatch.setattr(api.pika, "BlockingConnection", unavailable_broker)

    class FakeS3:
        def delete_objects(self, *, Bucket, Delete):
            assert Bucket == "batchserve-staging-test"
            for item in Delete["Objects"]:
                saved.pop(item["Key"], None)
            return {}

    monkeypatch.setattr(storage, "client", lambda: FakeS3())
    return saved


def send(client, payload, key=None):
    return client.post(
        "/jobs",
        headers={"Idempotency-Key": str(key or uuid.uuid4())},
        files={"file": ("input.csv", payload)},
        data={"model": "linear-v1"},
    )


def csv_payload(count=1001):
    return b"id,x1,x2\n" + b"".join(f"row-{i},1,2\n".encode() for i in range(count))


@pytest.mark.parametrize("failure", ["invalid_tail", "upload", "sql"])
def test_failed_request_keeps_durable_staging_until_cleanup(failure, client, objects, monkeypatch):
    def upload(path, key):
        objects[key] = path.read_bytes()
        if failure == "upload" and len(objects) == 2:
            raise RuntimeError("Upload response lost after object was stored")

    monkeypatch.setattr(api, "upload", upload)
    if failure == "sql":
        original = psycopg.Connection.execute

        def fail_job_insert(self, query, *args, **kwargs):
            if str(query).lstrip().startswith("INSERT INTO jobs"):
                raise psycopg.OperationalError("Injected job transaction failure")
            return original(self, query, *args, **kwargs)

        monkeypatch.setattr(psycopg.Connection, "execute", fail_job_insert)
    content = csv_payload(1000) + b"bad,NaN,1\n" if failure == "invalid_tail" else csv_payload()
    response = send(client, content)
    assert response.status_code == (422 if failure == "invalid_tail" else 503)
    assert objects
    with connect() as conn:
        assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 0
        assert conn.execute("SELECT to_regclass('staging_uploads') AS table_name").fetchone()[
            "table_name"
        ]
        manifest = conn.execute("SELECT * FROM staging_uploads").fetchone()
        assert manifest["status"] == "abandoned"
        assert set(objects) <= {
            row["input_key"]
            for row in conn.execute("SELECT input_key FROM staging_parts").fetchall()
        }
        conn.execute("UPDATE staging_uploads SET expires_at=clock_timestamp()-interval '1 second'")
    from batchserve.staging import cleanup_due

    assert cleanup_due() == 1
    assert not objects
    with connect() as conn:
        assert conn.execute("SELECT status FROM staging_uploads").fetchone()["status"] == "cleaned"


def test_manifest_and_part_are_committed_before_every_upload(client, objects, monkeypatch):
    protected = []

    def upload(path, key):
        with connect() as conn:
            exists = conn.execute("SELECT to_regclass('staging_parts') AS name").fetchone()["name"]
            protected.append(
                bool(exists)
                and conn.execute(
                    "SELECT 1 FROM staging_parts p JOIN staging_uploads s ON s.id=p.staging_id "
                    "WHERE p.input_key=%s AND s.status='active'",
                    (key,),
                ).fetchone()
                is not None
            )
        objects[key] = path.read_bytes()

    monkeypatch.setattr(api, "upload", upload)
    assert send(client, csv_payload()).status_code == 200
    assert protected == [True, True]


@pytest.mark.parametrize("conflicting", [False, True])
def test_idempotency_loser_is_cleaned_but_accepted_parts_are_preserved(
    conflicting, client, objects, monkeypatch
):
    from batchserve.staging import cleanup_due

    ready = threading.Barrier(2)

    def upload(path, key):
        objects[key] = path.read_bytes()
        ready.wait(timeout=5)

    monkeypatch.setattr(api, "upload", upload)
    key = uuid.uuid4()
    left = b"id,x1,x2\na,1,2\n"
    right = b"id,x1,x2\na,3,2\n" if conflicting else left
    with ThreadPoolExecutor(max_workers=2) as pool:
        tasks = [pool.submit(send, client, payload, key) for payload in (left, right)]
        responses = [task.result(timeout=10) for task in tasks]
    assert sorted(response.status_code for response in responses) == (
        [200, 409] if conflicting else [200, 200]
    )
    with connect() as conn:
        assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 1
        manifests = conn.execute("SELECT status,job_id FROM staging_uploads").fetchall()
        assert sorted(row["status"] for row in manifests) == ["abandoned", "adopted"]
        accepted = conn.execute("SELECT input_key FROM batches").fetchone()["input_key"]
        conn.execute("UPDATE staging_uploads SET expires_at=clock_timestamp()-interval '1 second'")
    assert len(objects) == 2 and cleanup_due() == 1
    assert set(objects) == {accepted}


def test_expired_active_producer_cannot_block_cleanup_of_abandoned_request(client, objects):
    from batchserve.staging import cleanup_due, register_part, staged_request

    with staged_request("a", uuid.uuid4(), "active") as (conn, active):
        active_key = register_part(conn, active, 0)
        objects[active_key] = b"active"
        with (
            pytest.raises(ValueError),
            staged_request("a", uuid.uuid4(), "failed") as (other, abandoned),
        ):
            abandoned_key = register_part(other, abandoned, 0)
            objects[abandoned_key] = b"abandoned"
            raise ValueError("Failed producer")
        conn.execute(
            "UPDATE staging_uploads SET expires_at=clock_timestamp()-interval '2 minutes' WHERE id=%s",
            (active,),
        )
        conn.execute(
            "UPDATE staging_uploads SET expires_at=clock_timestamp()-interval '1 second' WHERE id=%s",
            (abandoned,),
        )
        assert cleanup_due(limit=1) == 1
        assert set(objects) == {active_key}
    assert cleanup_due(limit=1) == 1
    assert not objects


def abandoned_object(objects):
    from batchserve.staging import register_part, staged_request

    with pytest.raises(ValueError), staged_request("a", uuid.uuid4(), "failed") as (conn, identity):
        key = register_part(conn, identity, 0)
        objects[key] = b"durable"
        raise ValueError("Producer failed")
    with connect() as conn:
        conn.execute("UPDATE staging_uploads SET expires_at=clock_timestamp()-interval '1 second'")
    return identity, key


def test_sigkill_leaves_manifest_and_releases_producer_lock(client, objects, tmp_path, monkeypatch):
    from batchserve.staging import cleanup_due

    project = Path(__file__).resolve().parents[1]
    program = """
import os,signal,sys,uuid
from pathlib import Path
from batchserve.staging import staged_request,register_part
with staged_request('a',uuid.uuid4(),'crash') as (conn,identity):
    key=register_part(conn,identity,0)
    target=Path(sys.argv[1])/key
    target.parent.mkdir(parents=True)
    target.write_bytes(b'uploaded-before-crash')
    print(identity,flush=True)
    os.kill(os.getpid(),signal.SIGKILL)
"""
    result = subprocess.run(
        [sys.executable, "-c", program, str(tmp_path)],
        cwd=project,
        env={**os.environ, "PYTHONPATH": str(project)},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    identity = uuid.UUID(result.stdout.strip())
    with connect() as conn:
        row = conn.execute("SELECT status FROM staging_uploads WHERE id=%s", (identity,)).fetchone()
        assert row["status"] == "active"
        key = conn.execute(
            "SELECT input_key FROM staging_parts WHERE staging_id=%s", (identity,)
        ).fetchone()["input_key"]
    assert (tmp_path / key).read_bytes() == b"uploaded-before-crash"
    assert cleanup_due() == 0

    class FilesystemS3:
        def delete_objects(self, *, Bucket, Delete):
            for item in Delete["Objects"]:
                (tmp_path / item["Key"]).unlink(missing_ok=True)
            return {}

    monkeypatch.setattr(storage, "client", lambda: FilesystemS3())
    with connect() as conn:
        conn.execute("UPDATE staging_uploads SET expires_at=clock_timestamp()-interval '1 second'")
    assert cleanup_due() == 1
    assert not (tmp_path / key).exists()


def test_two_cleaners_do_not_delete_same_active_cleanup(client, objects, monkeypatch):
    from batchserve.staging import cleanup_due

    abandoned_object(objects)
    started, release = threading.Event(), threading.Event()
    original = storage.client()

    class SlowS3:
        def delete_objects(self, **kwargs):
            started.set()
            assert release.wait(5)
            return original.delete_objects(**kwargs)

    monkeypatch.setattr(storage, "client", lambda: SlowS3())
    with ThreadPoolExecutor(max_workers=1) as pool:
        task = pool.submit(cleanup_due)
        try:
            assert started.wait(5)
            assert cleanup_due() == 0
        finally:
            release.set()
        assert task.result(timeout=5) == 1
    assert not objects


@pytest.mark.parametrize("failure", ["unavailable", "partial"])
def test_s3_cleanup_failure_keeps_intent_for_retry(failure, client, objects, monkeypatch):
    from batchserve.staging import cleanup_due

    identity, key = abandoned_object(objects)

    class FailedS3:
        def delete_objects(self, **kwargs):
            if failure == "unavailable":
                raise EndpointConnectionError(endpoint_url="http://storage-unavailable")
            return {"Errors": [{"Key": key, "Code": "AccessDenied"}]}

    with monkeypatch.context() as patch:
        patch.setattr(storage, "client", lambda: FailedS3())
        assert cleanup_due() == 0
    with connect() as conn:
        row = conn.execute("SELECT * FROM staging_uploads WHERE id=%s", (identity,)).fetchone()
        assert row["status"] == "abandoned" and row["last_error"]
        conn.execute("UPDATE staging_uploads SET expires_at=clock_timestamp()-interval '1 second'")
    assert objects and cleanup_due() == 1
    assert not objects


def test_sql_ack_failure_after_s3_delete_is_recoverable(client, objects, monkeypatch):
    from batchserve.staging import cleanup_due

    identity, _ = abandoned_object(objects)
    original = psycopg.Connection.execute

    def fail_ack(self, query, *args, **kwargs):
        if str(query).startswith("UPDATE staging_uploads SET status='cleaned'"):
            raise psycopg.OperationalError("Injected lost cleanup ack")
        return original(self, query, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(psycopg.Connection, "execute", fail_ack)
        with pytest.raises(psycopg.OperationalError, match="lost cleanup ack"):
            cleanup_due()
    assert not objects
    with connect() as conn:
        assert (
            conn.execute("SELECT status FROM staging_uploads WHERE id=%s", (identity,)).fetchone()[
                "status"
            ]
            == "cleaning"
        )
        conn.execute(
            "UPDATE staging_uploads SET cleanup_until=clock_timestamp()-interval '1 second'"
        )
    assert cleanup_due() == 1
    with connect() as conn:
        row = conn.execute(
            "SELECT status,cleanup_attempts FROM staging_uploads WHERE id=%s", (identity,)
        ).fetchone()
        assert row == {"status": "cleaned", "cleanup_attempts": 2}


def test_late_object_write_is_removed_on_next_manifest_sweep(client, objects):
    from batchserve.staging import cleanup_due

    identity, key = abandoned_object(objects)
    assert cleanup_due() == 1 and not objects
    # S3 мог закончить прежний PUT после обрыва соединения производителя.
    objects[key] = b"late-upload-result"
    with connect() as conn:
        conn.execute(
            "UPDATE staging_uploads SET expires_at=clock_timestamp()-interval '1 second' WHERE id=%s",
            (identity,),
        )
    assert cleanup_due() == 1
    assert not objects


def test_adoption_failure_rolls_back_job_and_batches(client, objects, monkeypatch):
    from batchserve.staging import cleanup_due

    monkeypatch.setattr(api, "upload", lambda path, key: objects.update({key: path.read_bytes()}))

    def fail_adopt(*args):
        raise RuntimeError("Injected manifest adoption failure")

    monkeypatch.setattr(api, "adopt", fail_adopt)
    assert send(client, csv_payload()).status_code == 503
    with connect() as conn:
        assert conn.execute("SELECT count(*) AS n FROM jobs").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM batches").fetchone()["n"] == 0
        conn.execute("UPDATE staging_uploads SET expires_at=clock_timestamp()-interval '1 second'")
    assert cleanup_due() == 1 and not objects


@pytest.mark.parametrize("status", ["running", "completed", "failed", "cancelled"])
def test_accepted_job_keeps_input_objects_after_ttl(status, client, objects, monkeypatch):
    from batchserve.staging import cleanup_due

    monkeypatch.setattr(api, "upload", lambda path, key: objects.update({key: path.read_bytes()}))
    response = send(client, b"id,x1,x2\na,1,2\n")
    assert response.status_code == 200
    with connect() as conn:
        conn.execute("UPDATE jobs SET status=%s", (status,))
        conn.execute("UPDATE staging_uploads SET expires_at=clock_timestamp()-interval '1 second'")
    assert cleanup_due() == 0
    assert len(objects) == 1


def test_batch_reference_protects_object_even_without_manifest_adoption(client, objects):
    from batchserve.staging import cleanup_due

    _, key = abandoned_object(objects)
    job_id = uuid.uuid4()
    with connect() as conn:
        conn.execute(
            "INSERT INTO jobs(id,tenant,model,request_key,fingerprint,total) VALUES (%s,'a','linear-v1',%s,'legacy-reference',1)",
            (job_id, uuid.uuid4()),
        )
        conn.execute("INSERT INTO batches(job_id,number,input_key) VALUES (%s,0,%s)", (job_id, key))
    assert cleanup_due() == 0
    assert key in objects
