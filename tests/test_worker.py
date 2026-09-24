import uuid

from batchserve.db import connect
from batchserve.worker import claim, complete, fail


def job(owner="a", batches=3):
    identity = uuid.uuid4()
    with connect() as conn:
        conn.execute("INSERT INTO tenants(id) VALUES (%s) ON CONFLICT DO NOTHING", (owner,))
        conn.execute(
            "INSERT INTO jobs(id,tenant,model,request_key,fingerprint,total) VALUES (%s,%s,'linear-v1',%s,'test',%s)",
            (identity, owner, uuid.uuid4(), batches * 1000),
        )
        with conn.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO batches(job_id,number,input_key) VALUES (%s,%s,%s)",
                [(identity, i, f"in/{i}") for i in range(batches)],
            )
    return identity


def expire(batch):
    with connect() as conn:
        conn.execute(
            "UPDATE batches SET lease_until=now()-interval '1 second' WHERE job_id=%s AND number=%s",
            (batch["job_id"], batch["number"]),
        )


def test_round_robin_and_tenant_concurrency(client):
    large = job("a", 100)
    small = job("b", 1)
    first, second = claim(), claim()
    assert {first["job_id"], second["job_id"]} == {large, small}
    assert claim() is None
    assert complete(first, "first")
    assert complete(second, "second")
    assert (
        client.get(f"/jobs/{small}", headers={"X-API-Key": "other-key"}).json()["status"]
        == "completed"
    )
    assert claim()["job_id"] == large


def test_stale_result_is_fenced_and_completed_batch_survives(client):
    identity = job(batches=2)
    first = claim()
    assert complete(first, "durable")
    old = claim()
    expire(old)
    assert not complete(old, "expired")
    fresh = claim()
    assert fresh["token"] != old["token"]
    assert not complete(old, "stale")
    assert complete(fresh, "fresh")
    with connect() as conn:
        keys = conn.execute(
            "SELECT output_key FROM batches WHERE job_id=%s ORDER BY number", (identity,)
        ).fetchall()
    assert [r["output_key"] for r in keys] == ["durable", "fresh"]


def test_cancel_blocks_late_results_and_frees_client(client):
    identity = job(batches=1)
    running = claim()
    assert client.post(f"/jobs/{identity}/cancel").json()["status"] == "cancelled"
    assert not complete(running, "too-late")
    next_job = job(batches=1)
    assert claim()["job_id"] == next_job


def test_failed_batch_retried_then_fails_job(client):
    identity = job(batches=1)
    for _ in range(3):
        batch = claim()
        assert batch
        fail(batch, "Провайдер недоступен")
    assert client.get(f"/jobs/{identity}").json()["status"] == "failed"
    assert claim() is None


def test_other_client_cannot_read_cancel_or_download(client):
    identity = job()
    headers = {"X-API-Key": "other-key"}
    assert client.get(f"/jobs/{identity}", headers=headers).status_code == 404
    assert client.post(f"/jobs/{identity}/cancel", headers=headers).status_code == 404
    assert client.get(f"/jobs/{identity}/result", headers=headers).status_code == 404
    assert client.get(f"/jobs/{identity}/result").status_code == 409


def test_heartbeat_cannot_revive_expired_or_stale_owner(client):
    from batchserve.worker import renew

    job(batches=1)
    task = claim()
    assert renew(task)
    expire(task)
    assert not renew(task)
    replacement = claim()
    assert not renew(task)
    assert renew(replacement)


def test_resume_reuses_completed_batches_and_fences_cancelled_owner(client):
    identity = job(batches=2)
    first = claim()
    complete(first, "preserved")
    old = claim()
    client.post(f"/jobs/{identity}/cancel")
    assert client.post(f"/jobs/{identity}/resume").json()["batches"]["completed"] == 1
    assert not complete(old, "cancelled-owner")
    new = claim()
    assert new["number"] == 1
    assert complete(new, "replacement")
    assert [r["action"] for r in client.get(f"/jobs/{identity}/events").json()] == [
        "cancel",
        "resume",
    ]
    assert client.post(f"/jobs/{identity}/resume").status_code == 409


def test_crashed_attempts_are_bounded_and_can_be_retried_explicitly(client):
    identity = job(batches=1)
    for _ in range(3):
        task = claim()
        expire(task)
    assert claim() is None
    assert client.get(f"/jobs/{identity}").json()["status"] == "failed"
    assert client.post(f"/jobs/{identity}/resume").status_code == 200
    assert claim()


def test_execution_deadline_limits_heartbeat(client):
    from batchserve.worker import renew

    job(batches=1)
    task = claim()
    with connect() as conn:
        conn.execute(
            "UPDATE batches SET started_at=clock_timestamp()-interval '6 minutes' WHERE job_id=%s",
            (task["job_id"],),
        )
    assert not renew(task)
    assert not complete(task, "too-long")


def test_execution_renews_lease_while_inference_is_busy(client, monkeypatch):
    import time

    from batchserve import worker

    identity = job(batches=1)
    task = claim()
    with connect() as conn:
        conn.execute(
            "UPDATE batches SET lease_until=clock_timestamp()+interval '0.3 seconds' WHERE job_id=%s",
            (identity,),
        )

    def inference(batch):
        time.sleep(0.5)
        return complete(batch, "long-inference")

    monkeypatch.setattr(worker, "_execute", inference)
    assert worker.execute(task, heartbeat_interval=0.05)
    assert client.get(f"/jobs/{identity}").json()["status"] == "completed"
