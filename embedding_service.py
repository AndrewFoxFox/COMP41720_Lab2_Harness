import json
import os
import random
import sqlite3
import threading
import time

import pika
from fastapi import FastAPI

import common

DB = "embedding_seen.db"
app = FastAPI()


def _conn():
    c = sqlite3.connect(DB, timeout=10)
    c.execute("CREATE TABLE IF NOT EXISTS seen (message_id TEXT PRIMARY KEY)")
    return c


@app.get("/health")
def health():
    # the harness just checks something is listening on :8083 - this also
    # lets you eyeball that the service is up without reading logs.
    return {"status": "ok", "failure_rate": os.environ.get("FAILURE_RATE", "0.0")}


def consume():
    conn = common.connect()
    ch = conn.channel()
    common.setup(ch)
    ch.basic_qos(prefetch_count=1)

    def on_task(ch, method, props, body):
        msg = json.loads(body)
        message_id = msg["message_id"]
        doc_id = msg["correlation_id"]

        # idempotency guard, persisted to disk so it survives a restart -
        # redelivering the same document.stored event must not generate
        # a second embedding result.
        c = _conn()
        seen = c.execute("SELECT 1 FROM seen WHERE message_id=?",
                          (message_id,)).fetchone()
        if seen:
            c.close()
            print(f"embed: duplicate {message_id} ignored (idempotent)",
                  flush=True)
            ch.basic_ack(method.delivery_tag)
            return

        failure_rate = float(os.environ.get("FAILURE_RATE", "0.0"))
        time.sleep(0.2)  # simulated embedding work - swap for a real
                         # Ollama call here if you want one
        failed = random.random() < failure_rate

        with c:
            c.execute("INSERT INTO seen VALUES (?)", (message_id,))
        c.close()

        # the result event's id is derived from the task's id, so even if
        # this whole on_task call were to run twice for the same task
        # (e.g. a crash after insert but before publish), the downstream
        # document service would still see it as one message, not two.
        result = json.dumps({
            "message_id": message_id + "-result",
            "correlation_id": doc_id,
            "status": "failed" if failed else "embedded",
        })
        ch.basic_publish(
            exchange="", routing_key="embedding_results", body=result,
            properties=pika.BasicProperties(delivery_mode=2),
        )
        print(f"embed: {doc_id} -> {'failed' if failed else 'embedded'}",
              flush=True)
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume("embedding_tasks", on_task)
    print(f"embedding service: consuming "
          f"(FAILURE_RATE={os.environ.get('FAILURE_RATE', '0.0')})",
          flush=True)
    ch.start_consuming()


@app.on_event("startup")
def startup():
    threading.Thread(target=consume, daemon=True).start()
