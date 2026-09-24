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
