import argparse
import logging
import os
import time
import uuid
from contextlib import contextmanager, suppress

from botocore.exceptions import BotoCoreError, ClientError
from psycopg import Error

from batchserve import storage
from batchserve.db import connect, init

logger = logging.getLogger(__name__)
LOCK_NAMESPACE = 330033
UNREFERENCED = """
    job_id IS NULL AND status IN ('active','abandoned','cleaning','cleaned')
    AND expires_at<=clock_timestamp()
    AND (cleanup_until IS NULL OR cleanup_until<=clock_timestamp())
    AND NOT EXISTS (
        SELECT 1 FROM staging_parts p JOIN batches b ON b.input_key=p.input_key
        WHERE p.staging_id=staging_uploads.id)
"""


def ttl_seconds():
    value = int(os.environ.get("STAGING_TTL_SECONDS", "300"))
    if not 30 <= value <= 86400:
        raise ValueError("STAGING_TTL_SECONDS должен быть от 30 до 86400")
    return value


def lock_key(identity):
    return identity.int % (2**31 - 1)


def abandon(conn, identity, reason):
    conn.execute(
        "UPDATE staging_uploads SET status='abandoned',last_error=%s,"
        "expires_at=clock_timestamp()+make_interval(secs=>%s) "
        "WHERE id=%s AND status='active' AND job_id IS NULL",
        (reason[:128], ttl_seconds(), identity),
    )


@contextmanager
def staged_request(owner, request_key, fingerprint):
    identity = uuid.uuid4()
    with connect() as conn:
        conn.autocommit = True
        # Session lock защищает и запрос, который ждёт S3 дольше TTL.
        # Между короткими транзакциями SQL открыт только сеанс, не транзакция.
        conn.execute("SELECT pg_advisory_lock(%s,%s)", (LOCK_NAMESPACE, lock_key(identity)))
        conn.execute("INSERT INTO tenants(id) VALUES (%s) ON CONFLICT DO NOTHING", (owner,))
        conn.execute(
            "INSERT INTO staging_uploads(id,tenant,request_key,fingerprint,expires_at) "
            "VALUES (%s,%s,%s,%s,clock_timestamp()+make_interval(secs=>%s))",
            (identity, owner, request_key, fingerprint, ttl_seconds()),
        )
        try:
            yield conn, identity
        except BaseException as exc:
            with suppress(Error):
                abandon(conn, identity, type(exc).__name__)
            raise
        # При SIGKILL PostgreSQL освобождает lock. Manifest уже зафиксирован.


def register_part(conn, identity, number):
    key = f"inputs/{identity}/{number}"
    with conn.transaction():
        active = conn.execute(
            "UPDATE staging_uploads SET expires_at=clock_timestamp()+make_interval(secs=>%s) "
            "WHERE id=%s AND status='active' RETURNING id",
            (ttl_seconds(), identity),
        ).fetchone()
        if not active:
            raise RuntimeError("Загрузка staging уже завершена")
        conn.execute(
            "INSERT INTO staging_parts(staging_id,number,input_key) VALUES (%s,%s,%s)",
            (identity, number, key),
        )
    return key


def adopt(conn, staging_id, job_id):
    updated = conn.execute(
        "UPDATE staging_uploads SET status='adopted',job_id=%s,last_error=NULL "
        "WHERE id=%s AND status='active'",
        (job_id, staging_id),
    )
    if updated.rowcount != 1:
        raise RuntimeError("Manifest не удалось принять вместе с заданием")


class IncompleteDeletion(RuntimeError):
    pass


def due_candidates(conn):
    cutoff = conn.execute("SELECT clock_timestamp() AS moment").fetchone()["moment"]
    cursor = None
    while True:
        boundary = " AND (expires_at,id)>(%s,%s)" if cursor else ""
        parameters = (cutoff, *cursor) if cursor else (cutoff,)
        rows = conn.execute(
            "SELECT id,expires_at FROM staging_uploads WHERE "
            + UNREFERENCED
            + " AND expires_at<=%s"
            + boundary
            + " ORDER BY expires_at,id LIMIT 100",
            parameters,
        ).fetchall()
        if not rows:
            return
        yield from rows
        cursor = (rows[-1]["expires_at"], rows[-1]["id"])


def cleanup_due(limit=20):
    if not 1 <= limit <= 100:
        raise ValueError("Предел очистки должен быть от 1 до 100")
    completed = 0
    attempted = 0
    with connect() as conn:
        conn.autocommit = True
        for candidate in due_candidates(conn):
            if attempted >= limit:
                break
            identity = candidate["id"]
            locked = conn.execute(
                "SELECT pg_try_advisory_lock(%s,%s) AS acquired",
                (LOCK_NAMESPACE, lock_key(identity)),
            ).fetchone()["acquired"]
            if not locked:
                continue
            try:
                with conn.transaction():
                    row = conn.execute(
                        "SELECT * FROM staging_uploads WHERE id=%s AND "
                        + UNREFERENCED
                        + " FOR UPDATE SKIP LOCKED",
                        (identity,),
                    ).fetchone()
                    if not row:
                        continue
                    attempted += 1
                    token = uuid.uuid4()
                    conn.execute(
                        "UPDATE staging_uploads SET status='cleaning',cleanup_token=%s,"
                        "cleanup_until=clock_timestamp()+interval '120 seconds',"
                        "cleanup_attempts=cleanup_attempts+1 WHERE id=%s",
                        (token, identity),
                    )
                    parts = conn.execute(
                        "SELECT input_key FROM staging_parts WHERE staging_id=%s ORDER BY number",
                        (identity,),
                    ).fetchall()
                try:
                    if parts:
                        s3 = storage.client()
                        for offset in range(0, len(parts), 1000):
                            response = s3.delete_objects(
                                Bucket=storage.bucket(),
                                Delete={
                                    "Objects": [
                                        {"Key": part["input_key"]}
                                        for part in parts[offset : offset + 1000]
                                    ],
                                    "Quiet": True,
                                },
                            )
                            if response.get("Errors"):
                                raise IncompleteDeletion("S3 отклонил часть удалений")
                except (BotoCoreError, ClientError, IncompleteDeletion) as exc:
                    conn.execute(
                        "UPDATE staging_uploads SET status='abandoned',cleanup_token=NULL,"
                        "cleanup_until=NULL,last_error=%s,"
                        "expires_at=clock_timestamp()+make_interval(secs=>%s) "
                        "WHERE id=%s AND cleanup_token=%s AND job_id IS NULL",
                        (
                            type(exc).__name__[:128],
                            min(2 ** min(row["cleanup_attempts"] + 1, 9), 300),
                            identity,
                            token,
                        ),
                    )
                    logger.warning(
                        "staging_cleanup_retry staging_id=%s kind=%s", identity, type(exc).__name__
                    )
                else:
                    updated = conn.execute(
                        "UPDATE staging_uploads SET status='cleaned',cleaned_at=clock_timestamp(),"
                        "cleanup_token=NULL,cleanup_until=NULL,last_error=NULL,"
                        "expires_at=clock_timestamp()+make_interval(secs=>%s) "
                        "WHERE id=%s AND cleanup_token=%s AND job_id IS NULL",
                        (ttl_seconds(), identity, token),
                    )
                    completed += updated.rowcount
            finally:
                with suppress(Error):
                    conn.execute(
                        "SELECT pg_advisory_unlock(%s,%s)", (LOCK_NAMESPACE, lock_key(identity))
                    )
    return completed


def main():
    parser = argparse.ArgumentParser(description="Очистка непринятых staging-загрузок")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    init()
    if args.once:
        print({"cleaned": cleanup_due()})
        return
    while True:
        try:
            cleaned = cleanup_due()
            if cleaned:
                logger.info("staging_cleaned count=%s", cleaned)
        except (Error, BotoCoreError, ClientError) as exc:
            logger.warning("staging_cleanup_unavailable kind=%s", type(exc).__name__)
        time.sleep(1)


if __name__ == "__main__":
    main()
