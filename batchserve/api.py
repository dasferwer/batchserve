import csv
import hashlib
import io
import json
import math
import os
import secrets
import tempfile
import uuid
from contextlib import asynccontextmanager
from itertools import islice
from pathlib import Path
from typing import Annotated

import pika
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import StreamingResponse

from batchserve.db import connect, init
from batchserve.model import VERSIONS
from batchserve.storage import bucket, client, upload


@asynccontextmanager
async def lifespan(app):
    init()
    yield


def tenant(x_api_key: str = Header(default="")):
    keys = json.loads(os.environ.get("CLIENT_KEYS", "{}"))
    for key, identity in keys.items():
        if secrets.compare_digest(x_api_key, key):
            return identity
    raise HTTPException(401, "Неверный ключ клиента")


app = FastAPI(title="BatchServe", lifespan=lifespan)


def get_job(identity, owner):
    with connect() as conn:
        job = conn.execute(
            "SELECT * FROM jobs WHERE id=%s AND tenant=%s", (identity, owner)
        ).fetchone()
        if not job:
            raise HTTPException(404, "Задание не найдено")
        progress = conn.execute(
            "SELECT status,count(*) AS count FROM batches WHERE job_id=%s GROUP BY status",
            (identity,),
        ).fetchall()
    return {**job, "batches": {row["status"]: row["count"] for row in progress}}


@app.get("/health")
def health(owner: Annotated[str, Depends(tenant)]):
    with connect() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/models")
def models(owner: Annotated[str, Depends(tenant)]):
    return {"versions": list(VERSIONS), "features": ["x1", "x2"]}


@app.post("/jobs")
def create(
    file: Annotated[UploadFile, File()],
    model: Annotated[str, Form()],
    idempotency_key: Annotated[uuid.UUID, Header()],
    owner: Annotated[str, Depends(tenant)],
):
    if model not in VERSIONS:
        raise HTTPException(422, "Неизвестная версия модели")
    digest = hashlib.sha256(model.encode())
    size = 0
    while chunk := file.file.read(1024 * 1024):
        size += len(chunk)
        if size > 128 * 1024 * 1024:
            raise HTTPException(413, "Лимит файла: 128 МиБ")
        digest.update(chunk)
    fingerprint = digest.hexdigest()
    file.file.seek(0)
    with connect() as conn:
        existing = conn.execute(
            "SELECT * FROM jobs WHERE tenant=%s AND request_key=%s", (owner, idempotency_key)
        ).fetchone()
    if existing:
        if existing["fingerprint"] != fingerprint:
            raise HTTPException(409, "Ключ уже использован для других данных или модели")
        return get_job(existing["id"], owner)
    identity, staging = uuid.uuid4(), uuid.uuid4()
    reader = csv.DictReader(io.TextIOWrapper(file.file, encoding="utf-8-sig", newline=""))
    parts = []
    total = 0
    try:
        if reader.fieldnames != ["id", "x1", "x2"]:
            raise ValueError("Нужен заголовок id,x1,x2 в указанном порядке")
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "part.json"
            while rows := list(islice(reader, 1000)):
                batch = []
                for row in rows:
                    if None in row or any(value is None for value in row.values()):
                        raise ValueError("Неверное число полей")
                    x1, x2 = float(row["x1"]), float(row["x2"])
                    if (
                        not row["id"]
                        or len(row["id"]) > 120
                        or not all(math.isfinite(x) and abs(x) <= 1e6 for x in (x1, x2))
                    ):
                        raise ValueError("Некорректный идентификатор или числовой признак")
                    batch.append({"id": row["id"], "x1": x1, "x2": x2})
                total += len(batch)
                if total > 1000000:
                    raise ValueError("Лимит задания: миллион строк")
                key = f"inputs/{staging}/{len(parts)}"
                path.write_text(json.dumps(batch))
                upload(path, key)
                parts.append(key)
        if not total:
            raise ValueError("Файл пуст")
    except (ValueError, csv.Error) as exc:
        raise HTTPException(422, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(503, "Не удалось сохранить входные пакеты") from exc
    with connect() as conn:
        conn.execute("INSERT INTO tenants (id) VALUES (%s) ON CONFLICT DO NOTHING", (owner,))
        inserted = conn.execute(
            """INSERT INTO jobs (id,tenant,model,request_key,fingerprint,total)
            VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (tenant,request_key) DO NOTHING RETURNING id""",
            (identity, owner, model, idempotency_key, fingerprint, total),
        ).fetchone()
        if inserted:
            with conn.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO batches (job_id,number,input_key) VALUES (%s,%s,%s)",
                    [(identity, i, key) for i, key in enumerate(parts)],
                )
        else:
            existing = conn.execute(
                "SELECT id,fingerprint FROM jobs WHERE tenant=%s AND request_key=%s",
                (owner, idempotency_key),
            ).fetchone()
            if existing["fingerprint"] != fingerprint:
                raise HTTPException(409, "Ключ уже использован для других данных")
            identity = existing["id"]
    try:
        with pika.BlockingConnection(pika.URLParameters(os.environ["AMQP_URL"])) as broker:
            channel = broker.channel()
            channel.queue_declare(queue="batches", durable=True)
            channel.basic_publish(
                exchange="",
                routing_key="batches",
                body=str(identity),
                properties=pika.BasicProperties(delivery_mode=2),
            )
    except (pika.exceptions.AMQPError, OSError):
        pass
    return get_job(identity, owner)


@app.get("/jobs/{identity}")
def status(identity: uuid.UUID, owner: Annotated[str, Depends(tenant)]):
    return get_job(identity, owner)


@app.post("/jobs/{identity}/cancel")
def cancel(identity: uuid.UUID, owner: Annotated[str, Depends(tenant)]):
    get_job(identity, owner)
    with connect() as conn:
        changed = conn.execute(
            "UPDATE jobs SET status='cancelled' WHERE id=%s AND status='running' RETURNING id",
            (identity,),
        ).fetchone()
        if changed:
            conn.execute(
                "UPDATE batches SET status='pending',token=NULL,lease_until=NULL WHERE job_id=%s AND status='running'",
                (identity,),
            )
            conn.execute("INSERT INTO job_events(job_id,action) VALUES (%s,'cancel')", (identity,))
    return get_job(identity, owner)


@app.post("/jobs/{identity}/resume")
def resume(identity: uuid.UUID, owner: Annotated[str, Depends(tenant)]):
    get_job(identity, owner)
    with connect() as conn:
        job = conn.execute("SELECT status FROM jobs WHERE id=%s FOR UPDATE", (identity,)).fetchone()
        if job["status"] not in ("cancelled", "failed"):
            raise HTTPException(409, "Продолжить можно отменённое или неудачное задание")
        conn.execute(
            "UPDATE batches SET status='pending',token=NULL,lease_until=NULL,attempts=0,error=NULL WHERE job_id=%s AND status<>'completed'",
            (identity,),
        )
        conn.execute("UPDATE jobs SET status='running' WHERE id=%s", (identity,))
        conn.execute("INSERT INTO job_events(job_id,action) VALUES (%s,'resume')", (identity,))
    return get_job(identity, owner)


@app.get("/jobs/{identity}/events")
def events(identity: uuid.UUID, owner: Annotated[str, Depends(tenant)]):
    get_job(identity, owner)
    with connect() as conn:
        return conn.execute(
            "SELECT action,created_at FROM job_events WHERE job_id=%s ORDER BY id", (identity,)
        ).fetchall()


@app.get("/jobs/{identity}/result")
def result(identity: uuid.UUID, owner: Annotated[str, Depends(tenant)]):
    if get_job(identity, owner)["status"] != "completed":
        raise HTTPException(409, "Результат ещё не готов")
    with connect() as conn:
        parts = conn.execute(
            "SELECT output_key FROM batches WHERE job_id=%s ORDER BY number", (identity,)
        ).fetchall()

    def stream():
        storage = client()
        for part in parts:
            response = storage.get_object(Bucket=bucket(), Key=part["output_key"])
            try:
                rows = json.loads(response["Body"].read())
            finally:
                response["Body"].close()
            yield "".join(json.dumps(row) + "\n" for row in rows)

    return StreamingResponse(
        stream(),
        media_type="application/x-ndjson",
        headers={"Content-Disposition": 'attachment; filename="predictions.ndjson"'},
    )
