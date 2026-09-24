import uuid

from batchserve import api
from batchserve.model import predict


def test_model_versions_are_deterministic():
    rows = [{"id": "a", "x1": -2, "x2": -1}, {"id": "b", "x1": 2, "x2": 1}]
    for version in ["linear-v1", "linear-v2"]:
        assert predict(version, rows) == predict(version, rows)
        assert [r["prediction"] for r in predict(version, rows)] == [0, 1]


def test_invalid_inputs_and_auth(client):
    assert client.get("/health", headers={"X-API-Key": "bad"}).status_code == 401
    assert len(client.get("/models").json()["versions"]) == 2
    headers = {"Idempotency-Key": str(uuid.uuid4())}
    assert (
        client.post(
            "/jobs",
            headers=headers,
            files={"file": ("x.csv", b"id,x1,x2\na,NaN,1\n")},
            data={"model": "linear-v1"},
        ).status_code
        == 422
    )
    assert (
        client.post(
            "/jobs",
            headers=headers,
            files={"file": ("x.csv", b"id,x1,x2\n")},
            data={"model": "linear-v1"},
        ).status_code
        == 422
    )
    assert (
        client.post(
            "/jobs",
            headers=headers,
            files={"file": ("x.csv", b"id,x1,x2\na,1,2\n")},
            data={"model": "unknown"},
        ).status_code
        == 422
    )


def test_idempotency_and_payload_conflict(client, monkeypatch):
    monkeypatch.setattr(api, "upload", lambda path, key: None)
    monkeypatch.setenv(
        "AMQP_URL", "amqp://demo:demo@127.0.0.1:1/%2F?connection_attempts=1&socket_timeout=1"
    )
    headers = {"Idempotency-Key": str(uuid.uuid4())}

    def send(content):
        return client.post(
            "/jobs",
            headers=headers,
            files={"file": ("x.csv", content)},
            data={"model": "linear-v1"},
        )

    first = send(b"id,x1,x2\na,1,2\n")
    assert first.status_code == 200
    assert send(b"id,x1,x2\na,1,2\n").json()["id"] == first.json()["id"]
    assert send(b"id,x1,x2\na,3,2\n").status_code == 409
