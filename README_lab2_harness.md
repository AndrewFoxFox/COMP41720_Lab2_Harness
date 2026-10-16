# Lab 2 test harness — COMP41720 (2026-27)

`lab2_harness.py` is the checker the demonstrator runs at your machine at the
end of the Lab 2 session. It is also yours: run it while you work, and arrive
already green.

## Requirements

Python 3.8+ only — no packages to install. It talks to your three services on
their fixed ports (`Quota :8081`, `Document :8082`, `Embedding :8083`) and to
the RabbitMQ management API on `http://localhost:15672`.

## How to run it

```
python3 lab2_harness.py --quick      # while developing: contract + happy path
python3 lab2_harness.py              # the full session check (interactive)
python3 lab2_harness.py --window     # re-measure the inconsistency window any time
```

The full run walks the three criteria on the demonstrator's sheet:

1. **Happy path green across all three services** — the quota routes exist,
   `POST /documents` returns 202 + an id immediately, the saga drives the
   document to COMPLETED with events flowing through RabbitMQ, and three
   concurrent documents get three distinct ids.
2. **Compensation, shown live** — you set `FAILURE_RATE=1.0` on your Embedding
   Service and restart it. The harness sends three documents and proves each
   settles as FAILED/COMPENSATED, that no messages are left stranded on any
   queue, and that the reserved quota comes back. (If your Quota Service has a
   readable state endpoint such as `GET /quota/state`, the quota check is fully
   automatic; otherwise you show the demonstrator the releases in your log.)
   Then you set `FAILURE_RATE=0.0` back and one more document must COMPLETE.
3. **Your inconsistency-window numbers** — 20 fresh requests, timed from
   `POST /documents` returning to the status endpoint reporting COMPLETED;
   the harness prints your min / median / max. These are the numbers you state
   to the demonstrator — from a run you did in the room, as the brief says. If
   you built a separate search projection, measure its lag too and be ready to
   explain the difference.

Useful flags: `--payload '{"tenant": "t1", "text": "..."}'` if your
`POST /documents` insists on an exact body shape, `--requests N` for the
window sample size, `--timeout SECONDS` if your embedder is slow (e.g. a real
Ollama model), `--quota/--docs/--embed/--rabbit` for non-default ports.

A tip for criterion 2: the harness only *observes* — the failure injection is
yours. `FAILURE_RATE` is read when your Embedding Service starts, so restart
it after changing the variable, both times.

The harness never modifies your code, your services or your broker; it only
sends requests and reads the management API. Exit code 0 means every check it
ran passed.
