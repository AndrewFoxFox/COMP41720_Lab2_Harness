import json
import sqlite3
import threading
import time
import uuid

import pika
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import common

DB = "documents.db"
QUOTA_URL = "http://127.0.0.1:8081"

app = FastAPI()


def _conn():
    c = sqlite3.connect(DB, timeout=10)
    c.execute("CREATE TABLE IF NOT EXISTS documents "
              "(id TEXT PRIMARY KEY, tenant TEXT, text TEXT, status TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS outbox "
              "(id TEXT PRIMARY KEY, doc_id TEXT, event_type TEXT, "
              "payload TEXT, published INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS processed_messages "
              "(message_id TEXT PRIMARY KEY)")
    return c


class DocReq(BaseModel):
    tenant: str = "default"
    text: str


@app.post("/documents", status_code=202)
def create_document(req: DocReq):
    doc_id = str(uuid.uuid4())

    # saga step 1: reserve quota synchronously. this is the one step that
    # is NOT async - if the tenant has no quota we want to reject the
    # request immediately rather than accept it and fail later.
    try:
        r = requests.post(f"{QUOTA_URL}/quota/reserve",
                           json={"doc_id": doc_id, "tenant": req.tenant},
                           timeout=5)
        r.raise_for_status()
    except Exception as e:
        raise HTTPException(status_code=503,
                             detail=f"quota service unavailable: {e}")

    # saga step 2: store the document record AND the outbox row for the
    # document.stored event in one local sqlite transaction. this is the
    # transactional outbox - it is impossible to save the document without
    # also queuing the event, because they are the same commit.
    event_id = str(uuid.uuid4())
    payload = json.dumps({
        "message_id": event_id,
        "correlation_id": doc_id,
        "tenant": req.tenant,
        "text": req.text,
    })
    c = _conn()
    with c:
        c.execute("INSERT INTO documents VALUES (?,?,?,?)",
                  (doc_id, req.tenant, req.text, "PENDING"))
        c.execute("INSERT INTO outbox VALUES (?,?,?,?,0)",
                  (event_id, doc_id, "document.stored", payload))
    c.close()

    return {"id": doc_id}


@app.get("/documents/{doc_id}/status")
def get_status(doc_id: str):
    c = _conn()
    row = c.execute("SELECT status FROM documents WHERE id=?",
                     (doc_id,)).fetchone()
    c.close()
    if row is None:
        raise HTTPException(status_code=404, detail="unknown id")
    return {"id": doc_id, "status": row[0]}


def outbox_relay():
    """Background poller: the only thing that ever publishes document.stored
    to RabbitMQ. Runs forever, separate from the request/response cycle, so
    a crash between the sqlite commit above and this loop running just means
    the event is published a little later, not lost."""
    while True:
        try:
            c = _conn()
            rows = c.execute(
                "SELECT id, doc_id, event_type, payload FROM outbox "
                "WHERE published=0").fetchall()
            if rows:
                conn = common.connect()
                ch = conn.channel()
                common.setup(ch)
                for event_id, doc_id, event_type, payload in rows:
                    ch.basic_publish(
                        exchange="", routing_key="embedding_tasks",
                        body=payload,
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
                    with c:
                        c.execute("UPDATE outbox SET published=1 WHERE id=?",
                                  (event_id,))
                    print(f"relay: published {event_type} for {doc_id}",
                          flush=True)
                conn.close()
            c.close()
        except Exception as e:
            print(f"relay error: {e}", flush=True)
        time.sleep(1.0)


def results_consumer():
    """Listens for embedding_results and drives the saga to its end state:
    COMPLETED on success, or FAILED + quota release (compensation) on
    failure. Idempotent via processed_messages - redelivery of a result we
    already handled is a no-op, so duplicates never double-complete or
    double-release."""
    conn = common.connect()
    ch = conn.channel()
    common.setup(ch)
    ch.basic_qos(prefetch_count=1)

    def on_result(ch, method, props, body):
        msg = json.loads(body)
        message_id = msg["message_id"]
        doc_id = msg["correlation_id"]
        status = msg["status"]

        c = _conn()
        seen = c.execute(
            "SELECT 1 FROM processed_messages WHERE message_id=?",
            (message_id,)).fetchone()
        if seen:
            c.close()
            ch.basic_ack(method.delivery_tag)
            print(f"results: duplicate {message_id} ignored (idempotent)",
                  flush=True)
            return

        if status == "embedded":
            with c:
                c.execute("UPDATE documents SET status='COMPLETED' WHERE id=?",
                          (doc_id,))
                c.execute("INSERT INTO processed_messages VALUES (?)",
                          (message_id,))
            print(f"saga: {doc_id} COMPLETED", flush=True)
        else:
            # compensation: the forward action (quota reserve) already
            # committed, so undo it explicitly - there is no automatic
            # rollback across services.
            try:
                requests.post(f"{QUOTA_URL}/quota/release",
                              json={"doc_id": doc_id}, timeout=5)
            except Exception as e:
                print(f"compensation: failed to release quota for "
                      f"{doc_id}: {e}", flush=True)
            with c:
                c.execute("UPDATE documents SET status='FAILED' WHERE id=?",
                          (doc_id,))
                c.execute("INSERT INTO processed_messages VALUES (?)",
                          (message_id,))
            print(f"saga: {doc_id} FAILED - quota released (compensation)",
                  flush=True)
        c.close()
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume("embedding_results", on_result)
    print("document service: results consumer waiting", flush=True)
    ch.start_consuming()


@app.on_event("startup")
def startup():
    threading.Thread(target=outbox_relay, daemon=True).start()
    threading.Thread(target=results_consumer, daemon=True).start()
