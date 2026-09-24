import os

import psycopg
from psycopg.rows import dict_row


def connect():
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def init():
    with connect() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(330029)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tenants (
                id text PRIMARY KEY, served_at timestamptz NOT NULL DEFAULT '-infinity');
            CREATE TABLE IF NOT EXISTS jobs (
                id uuid PRIMARY KEY, tenant text NOT NULL REFERENCES tenants(id),
                model text NOT NULL, request_key uuid NOT NULL, fingerprint text NOT NULL, status text NOT NULL DEFAULT 'running', total integer NOT NULL,
                created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(tenant,request_key));
            CREATE TABLE IF NOT EXISTS batches (
                job_id uuid REFERENCES jobs(id), number integer NOT NULL, input_key text NOT NULL,
                output_key text, status text NOT NULL DEFAULT 'pending', token uuid,
                lease_until timestamptz, attempts integer NOT NULL DEFAULT 0, error text,
                PRIMARY KEY(job_id,number));
            ALTER TABLE batches ADD COLUMN IF NOT EXISTS started_at timestamptz;
            CREATE TABLE IF NOT EXISTS job_events (
                id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,job_id uuid REFERENCES jobs(id),
                action text NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
        """)
