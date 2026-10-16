import sqlite3

from fastapi import FastAPI
from pydantic import BaseModel

DB = "quota.db"
app = FastAPI()


def _conn():
    c = sqlite3.connect(DB, timeout=10)
    c.execute("CREATE TABLE IF NOT EXISTS reservations "
              "(doc_id TEXT PRIMARY KEY, tenant TEXT, status TEXT)")
    return c


class ReserveReq(BaseModel):
    doc_id: str
    tenant: str = "default"


class ReleaseReq(BaseModel):
    doc_id: str


@app.post("/quota/reserve")
def reserve(req: ReserveReq):
    # idempotent: reserving the same doc_id twice does not double-reserve,
    # it just reports the state that already exists.
    c = _conn()
    row = c.execute("SELECT status FROM reservations WHERE doc_id=?",
                     (req.doc_id,)).fetchone()
    if row is None:
        with c:
            c.execute("INSERT INTO reservations VALUES (?,?,?)",
                      (req.doc_id, req.tenant, "reserved"))
        c.close()
        return {"doc_id": req.doc_id, "status": "reserved"}
    c.close()
    return {"doc_id": req.doc_id, "status": row[0]}


@app.post("/quota/release")
def release(req: ReleaseReq):
    # idempotent: releasing a doc_id that was never reserved, or releasing
    # it twice, is harmless - it just sets status=released either way.
    c = _conn()
    tenant_row = c.execute("SELECT tenant FROM reservations WHERE doc_id=?",
                            (req.doc_id,)).fetchone()
    tenant = tenant_row[0] if tenant_row else "default"
    with c:
        c.execute("INSERT OR REPLACE INTO reservations VALUES (?,?,?)",
                  (req.doc_id, tenant, "released"))
    c.close()
    return {"doc_id": req.doc_id, "status": "released"}


@app.get("/quota/state")
def state():
    # lets the harness (and you) see every reservation and its status
    # without reading logs - useful for proving compensation worked.
    c = _conn()
    rows = c.execute("SELECT doc_id, tenant, status FROM reservations").fetchall()
    c.close()
    return {"reservations": [{"doc_id": r[0], "tenant": r[1], "status": r[2]}
                              for r in rows]}
