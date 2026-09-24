import os
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import make_conninfo

from batchserve.api import app


@pytest.fixture
def client(monkeypatch):
    base = os.environ["DATABASE_URL"]
    schema = "test_" + uuid.uuid4().hex
    with psycopg.connect(base, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    monkeypatch.setenv("DATABASE_URL", make_conninfo(base, options=f"-c search_path={schema}"))
    monkeypatch.setenv("CLIENT_KEYS", '{"test-key":"a","other-key":"b"}')
    try:
        with TestClient(app, headers={"X-API-Key": "test-key"}) as client:
            yield client
    finally:
        with psycopg.connect(base, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
