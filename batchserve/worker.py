import json
import logging
import os
import tempfile
import threading
import time
import uuid
from pathlib import Path

import pika
from psycopg import Error

from batchserve.db import connect, init
from batchserve.model import predict
from batchserve.storage import download, upload

logger = logging.getLogger(__name__)


def claim():
    with connect() as conn:
        exhausted = conn.execute("""SELECT j.id FROM jobs j WHERE j.status='running' AND EXISTS(
            SELECT 1 FROM batches b WHERE b.job_id=j.id AND b.status='running'
            AND b.lease_until<clock_timestamp() AND b.attempts>=3)
            FOR UPDATE OF j SKIP LOCKED LIMIT 100""").fetchall()
        for job in exhausted:
            conn.execute(
                "UPDATE batches SET status='failed',error='Исчерпаны попытки после потери воркера',token=NULL WHERE job_id=%s AND status='running' AND lease_until<clock_timestamp() AND attempts>=3",
                (job["id"],),
            )
            conn.execute("UPDATE jobs SET status='failed' WHERE id=%s", (job["id"],))
        tenant = conn.execute("""SELECT t.id FROM tenants t WHERE EXISTS (
            SELECT 1 FROM jobs j JOIN batches b ON b.job_id=j.id WHERE j.tenant=t.id
            AND j.status='running' AND (b.status='pending' OR (b.status='running' AND b.lease_until<now() AND b.attempts<3)))
            AND NOT EXISTS (SELECT 1 FROM jobs j JOIN batches b ON b.job_id=j.id
                WHERE j.tenant=t.id AND j.status='running' AND b.status='running' AND b.lease_until>=now())
            ORDER BY t.served_at,t.id LIMIT 1 FOR UPDATE OF t SKIP LOCKED""").fetchone()
        if not tenant:
            return None
        batch = conn.execute(
            """SELECT b.*,j.model FROM batches b JOIN jobs j ON j.id=b.job_id
            WHERE j.tenant=%s AND j.status='running'
            AND (b.status='pending' OR (b.status='running' AND b.lease_until<now() AND b.attempts<3))
            ORDER BY j.created_at,b.number LIMIT 1 FOR UPDATE OF b SKIP LOCKED""",
            (tenant["id"],),
        ).fetchone()
        if not batch:
            return None
        token = uuid.uuid4()
        conn.execute(
            """UPDATE batches SET status='running',token=%s,
            lease_until=clock_timestamp()+interval '30 seconds',started_at=clock_timestamp(),attempts=attempts+1 WHERE job_id=%s AND number=%s""",
            (token, batch["job_id"], batch["number"]),
        )
        conn.execute("UPDATE tenants SET served_at=clock_timestamp() WHERE id=%s", (tenant["id"],))
        return {**batch, "token": token}


def complete(batch, output_key):
    with connect() as conn:
        job = conn.execute(
            "SELECT status FROM jobs WHERE id=%s FOR UPDATE", (batch["job_id"],)
        ).fetchone()
        if job["status"] != "running":
            return False
        updated = conn.execute(
            """UPDATE batches SET status='completed',output_key=%s,error=NULL
            WHERE job_id=%s AND number=%s AND token=%s AND status='running' AND lease_until>clock_timestamp() AND started_at>clock_timestamp()-interval '5 minutes'""",
            (output_key, batch["job_id"], batch["number"], batch["token"]),
        )
        if not updated.rowcount:
            return False
        conn.execute(
            """UPDATE jobs SET status='completed' WHERE id=%s AND NOT EXISTS
            (SELECT 1 FROM batches WHERE job_id=%s AND status!='completed')""",
            (batch["job_id"], batch["job_id"]),
        )
        return True


def fail(batch, message):
    with connect() as conn:
        conn.execute("SELECT id FROM jobs WHERE id=%s FOR UPDATE", (batch["job_id"],))
        conn.execute(
            """UPDATE batches SET status=CASE WHEN attempts>=3 THEN 'failed' ELSE 'pending' END,error=%s
            WHERE job_id=%s AND number=%s AND token=%s AND status='running' AND lease_until>clock_timestamp() AND started_at>clock_timestamp()-interval '5 minutes'""",
            (message[:300], batch["job_id"], batch["number"], batch["token"]),
        )
        conn.execute(
            """UPDATE jobs SET status='failed' WHERE id=%s AND status='running'
            AND EXISTS (SELECT 1 FROM batches WHERE job_id=%s AND status='failed')""",
            (batch["job_id"], batch["job_id"]),
        )


def _execute(batch):
    with tempfile.TemporaryDirectory() as folder:
        source, target = Path(folder) / "input", Path(folder) / "output"
        download(batch["input_key"], source)
        result = predict(batch["model"], json.loads(source.read_text()))
        target.write_text(json.dumps(result))
        # Устаревший воркер пишет отдельный объект; видимым становится только принятый token.
        output_key = f"results/{batch['job_id']}/{batch['number']}/{batch['token']}"
        upload(target, output_key)
        return complete(batch, output_key)


def renew(batch):
    with connect() as conn:
        return (
            conn.execute(
                """UPDATE batches b SET lease_until=clock_timestamp()+interval '30 seconds'
            FROM jobs j WHERE j.id=b.job_id AND j.status='running' AND b.job_id=%s AND b.number=%s
            AND b.token=%s AND b.status='running' AND b.lease_until>clock_timestamp()
            AND b.started_at>clock_timestamp()-interval '5 minutes' RETURNING b.job_id""",
                (batch["job_id"], batch["number"], batch["token"]),
            ).fetchone()
            is not None
        )


def execute(batch, heartbeat_interval=5):
    stop = threading.Event()

    def heartbeat():
        while not stop.wait(heartbeat_interval):
            try:
                if not renew(batch):
                    return
            except Error:
                return

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    try:
        return _execute(batch)
    finally:
        stop.set()
        thread.join(timeout=2)


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("pika").setLevel(logging.WARNING)
    init()
    while True:
        try:
            batch = claim()
            if batch:
                try:
                    execute(batch)
                except Exception as exc:
                    logger.exception("Пакет не обработан")
                    fail(batch, str(exc))
                continue
            # Уведомления ускоряют опрос; отсутствие брокера не теряет задания из БД.
            try:
                with pika.BlockingConnection(pika.URLParameters(os.environ["AMQP_URL"])) as broker:
                    channel = broker.channel()
                    channel.queue_declare(queue="batches", durable=True)
                    method, _, _ = channel.basic_get(queue="batches", auto_ack=False)
                    if method:
                        channel.basic_ack(method.delivery_tag)
            except (pika.exceptions.AMQPError, OSError):
                pass
        except Exception:
            logger.exception("Ошибка цикла воркера")
        time.sleep(0.5)


if __name__ == "__main__":
    main()
