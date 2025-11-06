#!/usr/bin/env python3
"""
queuectl: A single-file CLI background job queue with workers, retries (exponential backoff),
Dead Letter Queue (DLQ), persistence (SQLite), and configuration via CLI.

Tech: Python 3.10+, Typer (CLI), SQLite (builtin), multiprocessing.

Usage examples (after installing deps):
    python queuectl.py init
    python queuectl.py enqueue '{"id":"job1","command":"echo hello"}'
    python queuectl.py worker start --count 2
    python queuectl.py status
    python queuectl.py list --state pending
    python queuectl.py dlq list
    python queuectl.py dlq retry job1
    python queuectl.py config set max_retries 5

See README section at bottom of this file (search for README_SNIPPET) for more details.
"""
from __future__ import annotations
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from multiprocessing import Process, current_process
from typing import Any, Dict, Iterable, List, Optional, Tuple

import typer

app = typer.Typer(help="queuectl - minimal job queue with workers, retries and DLQ")

DB_PATH_DEFAULT = os.environ.get("QUEUECTL_DB", os.path.join(os.getcwd(), "queuectl.db"))
LOG_DIR_DEFAULT = os.environ.get("QUEUECTL_LOG_DIR", os.path.join(os.getcwd(), "job_logs"))
HEARTBEAT_SECONDS = 3

# ---------------------------- Utilities ----------------------------------

def utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

@contextmanager
def db_conn(db_path: str):
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)  # autocommit mode
    try:
        conn.row_factory = sqlite3.Row
        # WAL improves concurrency for multiple workers
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        yield conn
    finally:
        conn.close()

# ---------------------------- Schema -------------------------------------

SCHEMA_SQL = r"""
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    command TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','processing','completed','failed','dead')),
    attempts INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    run_at TEXT NOT NULL,
    last_error TEXT,
    last_exit_code INTEGER
);

CREATE INDEX IF NOT EXISTS idx_jobs_state_runat ON jobs(state, run_at);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
    id TEXT PRIMARY KEY,           -- process name or pid
    pid INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    status TEXT NOT NULL           -- 'running' | 'stopping'
);

CREATE TABLE IF NOT EXISTS job_logs (
    job_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    ts TEXT NOT NULL,
    stream TEXT NOT NULL CHECK(stream IN ('stdout','stderr')),
    content TEXT NOT NULL,
    PRIMARY KEY(job_id, seq),
    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
);
"""

DEFAULT_CONFIG: Dict[str, Any] = {
    "max_retries": 3,
    "backoff_base": 2,           # delay_seconds = base ** attempts
    "job_timeout": 300           # seconds
}

# ---------------------------- DB helpers ---------------------------------

def ensure_schema(conn: sqlite3.Connection):
    conn.executescript(SCHEMA_SQL)
    # Insert defaults if not present
    for k, v in DEFAULT_CONFIG.items():
        conn.execute("INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (k, json.dumps(v)))


def get_config(conn: sqlite3.Connection) -> Dict[str, Any]:
    cur = conn.execute("SELECT key, value FROM settings")
    cfg = {row[0]: json.loads(row[1]) for row in cur.fetchall()}
    # fill missing defaults (if any)
    for k, v in DEFAULT_CONFIG.items():
        cfg.setdefault(k, v)
    return cfg


def set_config(conn: sqlite3.Connection, key: str, value: Any):
    ensure_schema(conn)
    conn.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                 (key, json.dumps(value)))


# Try to atomically claim a pending job. We use an IMMEDIATE transaction to lock the db for write,
# then update the chosen job to 'processing' only if it is still 'pending'.

def claim_pending_job(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT * FROM jobs WHERE state='pending' AND run_at<=? ORDER BY created_at LIMIT 1",
            (now,)
        ).fetchone()
        if not row:
            conn.execute("COMMIT")
            return None
        updated = conn.execute(
            "UPDATE jobs SET state='processing', updated_at=? WHERE id=? AND state='pending'",
            (utcnow(), row["id"])  # optimistic check
        )
        if updated.rowcount == 1:
            conn.execute("COMMIT")
            return row
        else:
            conn.execute("ROLLBACK")
            return None
    except Exception:
        conn.execute("ROLLBACK")
        raise


def update_job_state(conn: sqlite3.Connection, job_id: str, state: str, **extra):
    fields = ["state = ?", "updated_at = ?"]
    params: List[Any] = [state, utcnow()]
    for k, v in extra.items():
        fields.append(f"{k} = ?")
        params.append(v)
    params.append(job_id)
    conn.execute(f"UPDATE jobs SET {', '.join(fields)} WHERE id = ?", params)


def insert_job(conn: sqlite3.Connection, job: Dict[str, Any]):
    ensure_schema(conn)
    conn.execute(
        "INSERT INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at, run_at)\n"
        "VALUES(?,?,?,?,?,?,?,?)",
        (
            job["id"],
            job["command"],
            job.get("state", "pending"),
            int(job.get("attempts", 0)),
            int(job.get("max_retries", DEFAULT_CONFIG["max_retries"])),
            job.get("created_at", utcnow()),
            job.get("updated_at", utcnow()),
            job.get("run_at", utcnow()),
        ),
    )


def append_job_log(conn: sqlite3.Connection, job_id: str, stream: str, content: str):
    seq = conn.execute("SELECT COALESCE(MAX(seq),0)+1 FROM job_logs WHERE job_id=?", (job_id,)).fetchone()[0]
    conn.execute(
        "INSERT INTO job_logs(job_id, seq, ts, stream, content) VALUES(?,?,?,?,?)",
        (job_id, seq, utcnow(), stream, content)
    )

# ---------------------------- Worker logic --------------------------------

stop_event = threading.Event()


def _run_command_capture(job_id: str, command: str, timeout_s: int) -> Tuple[int, str, str]:
    # Capture stdout/stderr. We rely on shell for simplicity.
    try:
        proc = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
        return proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired as te:
        return 124, te.stdout or "", te.stderr or "Command timed out"


def heartbeat_loop(db_path: str, worker_id: str):
    with db_conn(db_path) as conn:
        ensure_schema(conn)
        while not stop_event.is_set():
            conn.execute(
                "INSERT INTO workers(id,pid,started_at,last_seen,status)\n"
                "VALUES(?,?,?,?,?)\n"
                "ON CONFLICT(id) DO UPDATE SET last_seen=excluded.last_seen, status=excluded.status",
                (worker_id, os.getpid(), utcnow(), utcnow(), "running"),
            )
            for _ in range(HEARTBEAT_SECONDS * 10):
                if stop_event.wait(0.1):
                    break
        # mark stopping
        conn.execute("UPDATE workers SET status='stopping', last_seen=? WHERE id=?", (utcnow(), worker_id))


def worker_loop(db_path: str):
    worker_id = f"worker-{os.getpid()}"
    hb_thread = threading.Thread(target=heartbeat_loop, args=(db_path, worker_id), daemon=True)
    hb_thread.start()

    with db_conn(db_path) as conn:
        ensure_schema(conn)
        cfg = get_config(conn)
        base = int(cfg.get("backoff_base", 2))
        timeout_s = int(cfg.get("job_timeout", 300))

        def handle_signal(signum, frame):
            # graceful: finish current job (loop iteration ends) then exit
            stop_event.set()
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        while not stop_event.is_set():
            job = claim_pending_job(conn)
            if not job:
                # idle sleep, but wake up early if stopping
                stop_event.wait(0.5)
                continue

            job_id = job["id"]
            command = job["command"]
            attempts = int(job["attempts"])
            max_retries = int(job["max_retries"])

            # Execute job
            rc, out, err = _run_command_capture(job_id, command, timeout_s)
            if out:
                append_job_log(conn, job_id, "stdout", out)
            if err:
                append_job_log(conn, job_id, "stderr", err)

            if rc == 0:
                update_job_state(conn, job_id, "completed", last_exit_code=rc, last_error=None)
            else:
                attempts += 1
                if attempts > max_retries:
                    update_job_state(conn, job_id, "dead", attempts=attempts, last_exit_code=rc, last_error=f"exit {rc}")
                else:
                    # exponential backoff
                    delay = base ** attempts
                    next_run = (datetime.now(timezone.utc) + timedelta(seconds=delay)).replace(microsecond=0).isoformat()
                    update_job_state(conn, job_id, "failed", attempts=attempts, last_exit_code=rc, last_error=f"exit {rc}")
                    conn.execute("UPDATE jobs SET state='pending', run_at=?, updated_at=? WHERE id=?",
                                 (next_run, utcnow(), job_id))
        # stop hb thread
        stop_event.set()
        hb_thread.join(timeout=2)

# ---------------------------- CLI Commands --------------------------------

@app.command()
def init(db: str = typer.Option(DB_PATH_DEFAULT, help="Path to SQLite DB")):
    """Initialize database and default settings."""
    with db_conn(db) as conn:
        ensure_schema(conn)
    typer.echo(f"Initialized DB at {db}")


@app.command()
def enqueue(job: str = typer.Argument(..., help="Job JSON string"),
            db: str = typer.Option(DB_PATH_DEFAULT, help="Path to SQLite DB")):
    """Enqueue a new job. Pass a JSON object with at least id and command.
    Example: queuectl enqueue '{"id":"job1","command":"echo hello"}'
    """
    try:
        obj = json.loads(job)
    except json.JSONDecodeError as e:
        typer.secho(f"Invalid JSON: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    if "id" not in obj or "command" not in obj:
        typer.secho("Job must include 'id' and 'command'", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    now = utcnow()
    obj.setdefault("state", "pending")
    obj.setdefault("attempts", 0)
    obj.setdefault("max_retries", DEFAULT_CONFIG["max_retries"])
    obj.setdefault("created_at", now)
    obj.setdefault("updated_at", now)
    obj.setdefault("run_at", now)

    with db_conn(db) as conn:
        try:
            insert_job(conn, obj)
        except sqlite3.IntegrityError:
            typer.secho(f"Job with id '{obj['id']}' already exists", fg=typer.colors.RED)
            raise typer.Exit(code=1)
    typer.echo(f"Enqueued job {obj['id']}")


@app.command()
def status(db: str = typer.Option(DB_PATH_DEFAULT, help="Path to SQLite DB")):
    """Show summary of job states and active workers."""
    with db_conn(db) as conn:
        ensure_schema(conn)
        cur = conn.execute("SELECT state, COUNT(*) as c FROM jobs GROUP BY state")
        counts = {r[0]: r[1] for r in cur.fetchall()}
        cfg = get_config(conn)
        now = datetime.now(timezone.utc)
        # consider workers active if heartbeat in last 2*HEARTBEAT_SECONDS
        threshold = (now - timedelta(seconds=2*HEARTBEAT_SECONDS)).replace(microsecond=0).isoformat()
        active = conn.execute("SELECT COUNT(*) FROM workers WHERE last_seen >= ?", (threshold,)).fetchone()[0]
    typer.echo("Jobs:")
    for state in ['pending','processing','completed','failed','dead']:
        typer.echo(f"  {state:10} {counts.get(state, 0)}")
    typer.echo(f"Active workers: {active}")
    typer.echo("Config:")
    for k in sorted(cfg.keys()):
        typer.echo(f"  {k}: {cfg[k]}")


@app.command()
def list(state: Optional[str] = typer.Option(None, help="Filter by state"),
         limit: int = typer.Option(50, help="Max rows"),
         db: str = typer.Option(DB_PATH_DEFAULT, help="Path to SQLite DB")):
    """List jobs (optionally by state)."""
    with db_conn(db) as conn:
        ensure_schema(conn)
        if state:
            rows = conn.execute(
                "SELECT id, command, state, attempts, max_retries, run_at, updated_at FROM jobs WHERE state=? ORDER BY created_at LIMIT ?",
                (state, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, command, state, attempts, max_retries, run_at, updated_at FROM jobs ORDER BY created_at LIMIT ?",
                (limit,),
            ).fetchall()
    for r in rows:
        typer.echo(json.dumps({k: r[k] for k in r.keys()}))


worker_app = typer.Typer(help="Manage workers")
app.add_typer(worker_app, name="worker")


@worker_app.command("start")
def worker_start(count: int = typer.Option(1, help="Number of worker processes"),
                 db: str = typer.Option(DB_PATH_DEFAULT, help="Path to SQLite DB")):
    """Start one or more workers (runs in foreground; Ctrl+C to stop gracefully)."""
    with db_conn(db) as conn:
        ensure_schema(conn)
    procs: List[Process] = []

    def handle_signal(signum, frame):
        stop_event.set()
        for p in procs:
            if p.is_alive():
                p.terminate()  # ask child to stop; they handle SIGTERM gracefully
        for p in procs:
            p.join(timeout=5)
        raise typer.Abort()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    for i in range(count):
        p = Process(target=worker_loop, args=(db,), daemon=False)
        p.start()
        procs.append(p)
        typer.echo(f"Started worker pid={p.pid}")

    # Wait for children
    for p in procs:
        p.join()


@worker_app.command("stop")
def worker_stop(db: str = typer.Option(DB_PATH_DEFAULT, help="Path to SQLite DB")):
    """Signal workers to stop gracefully (best-effort)."""
    # Since this is a simple single-file tool, we mark workers table to 'stopping'.
    with db_conn(db) as conn:
        ensure_schema(conn)
        conn.execute("UPDATE workers SET status='stopping', last_seen=?", (utcnow(),))
    typer.echo("Marked workers as stopping. If running in foreground, press Ctrl+C.")


# ---------------------------- DLQ Commands --------------------------------

dlq_app = typer.Typer(help="Dead Letter Queue operations")
app.add_typer(dlq_app, name="dlq")


@dlq_app.command("list")
def dlq_list(db: str = typer.Option(DB_PATH_DEFAULT, help="Path to SQLite DB")):
    with db_conn(db) as conn:
        ensure_schema(conn)
        rows = conn.execute(
            "SELECT id, command, attempts, max_retries, updated_at, last_error, last_exit_code FROM jobs WHERE state='dead' ORDER BY updated_at DESC"
        ).fetchall()
    for r in rows:
        typer.echo(json.dumps({k: r[k] for k in r.keys()}))


@dlq_app.command("retry")
def dlq_retry(job_id: str = typer.Argument(..., help="Job ID to retry from DLQ"),
             db: str = typer.Option(DB_PATH_DEFAULT, help="Path to SQLite DB")):
    with db_conn(db) as conn:
        ensure_schema(conn)
        row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not row:
            typer.secho(f"No such job: {job_id}", fg=typer.colors.RED)
            raise typer.Exit(code=1)
        update_job_state(conn, job_id, "pending", attempts=0, last_error=None)
        conn.execute("UPDATE jobs SET run_at=? WHERE id=?", (utcnow(), job_id))
    typer.echo(f"Job {job_id} moved from DLQ to pending")


# ---------------------------- Config Commands -----------------------------

config_app = typer.Typer(help="Configuration management")
app.add_typer(config_app, name="config")


@config_app.command("get")
def config_get(db: str = typer.Option(DB_PATH_DEFAULT, help="Path to SQLite DB")):
    with db_conn(db) as conn:
        ensure_schema(conn)
        cfg = get_config(conn)
    typer.echo(json.dumps(cfg, indent=2))


@config_app.command("set")
def config_set(key: str = typer.Argument(..., help="Config key"),
               value: str = typer.Argument(..., help="JSON value (e.g. 5 or \"string\")"),
               db: str = typer.Option(DB_PATH_DEFAULT, help="Path to SQLite DB")):
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        parsed = value  # allow raw strings
    with db_conn(db) as conn:
        set_config(conn, key, parsed)
    typer.echo(f"Set {key} = {parsed}")


# ---------------------------- Logs ----------------------------------------

@app.command()
def logs(job_id: str = typer.Argument(..., help="Job ID"),
         db: str = typer.Option(DB_PATH_DEFAULT, help="Path to SQLite DB")):
    """Show captured stdout/stderr logs for a job."""
    with db_conn(db) as conn:
        ensure_schema(conn)
        rows = conn.execute("SELECT seq, ts, stream, content FROM job_logs WHERE job_id=? ORDER BY seq", (job_id,)).fetchall()
        if not rows:
            typer.echo("No logs for job or job not found")
            raise typer.Exit()
        for r in rows:
            prefix = f"[{r['ts']}] {r['stream']}: "
            sys.stdout.write(prefix + r['content'])
            if not r['content'].endswith('\n'):
                sys.stdout.write('\n')


# ---------------------------- README snippet ------------------------------
README_SNIPPET = r"""
# queuectl (single-file)

A compact, production-leaning job queue built for the QueueCTL internship assignment.

## Features
- Enqueue jobs with arbitrary shell commands
- Multiple worker processes with safe claiming (no duplicate work)
- Exponential backoff retries; DLQ after max retries
- Persistent storage in SQLite (WAL enabled)
- Graceful shutdown (finish current job; Ctrl+C)
- Worker heartbeats & status summary
- Config via CLI: `max_retries`, `backoff_base`, `job_timeout`
- Captured stdout/stderr logs per job (`queuectl logs <id>`)

## Quick start
```bash
python queuectl.py init
python queuectl.py enqueue '{"id":"success","command":"echo Hello"}'
python queuectl.py enqueue '{"id":"fail","command":"bash -c \"exit 2\""}'
python queuectl.py worker start --count 2 &
python queuectl.py status
python queuectl.py logs success
python queuectl.py dlq list
python queuectl.py dlq retry fail
```

## Architecture
- **DB schema:** `jobs`, `job_logs`, `workers`, `settings`.
- **Claiming:** IMMEDIATE txn selects a `pending` job ready to run (`run_at <= now`), then flips to `processing` atomically.
- **Execution:** subprocess with timeout; nonzero exit codes count as failure.
- **Retry:** on failure, increment `attempts`, compute `delay = backoff_base ** attempts`, set `run_at` to `now+delay`, return to `pending`.
- **DLQ:** if `attempts > max_retries` -> `dead`.
- **Workers:** separate processes; heartbeats every ~3s into `workers` table; `status` shows active count.

## Testing key scenarios
- Success case (echo) -> `completed`.
- Invalid command (exit 127) retries then DLQ.
- Parallelism: start with `--count 3` and enqueue 50 `sleep 1` jobs; see no duplicate execution.
- Persistence: stop workers, restart; jobs resume because SQLite is on disk.

## Trade-offs
- Single-file simplicity; no external message broker.
- Foreground `worker start` (press Ctrl+C to stop); `worker stop` is best-effort marker.
- Scheduling primitive: ready when `run_at <= now`.

"""


@app.command()
def readme():
    """Print an inline README with setup and examples."""
    typer.echo(README_SNIPPET)


if __name__ == "__main__":
    app()
