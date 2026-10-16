#!/usr/bin/env python3
"""
COMP41720 Distributed Systems (2026-27) - Lab 2 Test Harness
Sagas, the Outbox Pattern, and Eventual Consistency

This is the harness the demonstrator runs at your machine at the end of the
session. Run it yourself, early and often - if it is green here, it will be
green in the room.

WHAT IT CHECKS (the three pass criteria on the demonstrator's sheet):

  [1] Happy path green across the three services on their fixed ports:
      Quota :8081 (POST /quota/reserve, /quota/release), Document :8082
      (POST /documents, GET /documents/{id}/status), Embedding :8083.
      POST /documents returns an id immediately; the saga drives the
      document to COMPLETED, with events flowing through RabbitMQ.
  [2] Compensation, shown live: you raise FAILURE_RATE to 1.0 on your
      Embedding Service; the harness proves failed sagas settle as
      FAILED/COMPENSATED, reserved quota is released, and no messages
      are left stranded on any queue.
  [3] The inconsistency window: the harness runs 20 fresh requests in the
      room and reports YOUR min / median / max - the numbers you state
      to the demonstrator.

USAGE
  python3 lab2_harness.py                 full check (criteria 1-3, interactive)
  python3 lab2_harness.py --quick         criterion 1 only (no interactive parts)
  python3 lab2_harness.py --window        criterion 3 only (measure again any time)
  python3 lab2_harness.py --help          all options

Requires only Python 3.8+ (standard library). Broker checks talk to the
RabbitMQ management API on http://localhost:15672 (guest/guest by default).

No part of this file needs modifying. If the harness misreads your setup,
use the flags (--payload, --requests, --timeout, ...) rather than editing
checks.
"""

import argparse
import base64
import json
import socket
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request

VERSION = "1.0 (2026-08-26)"

# --------------------------------------------------------------------------
# output helpers
# --------------------------------------------------------------------------

USE_COLOUR = sys.stdout.isatty()

def _c(code, s):
    return f"\033[{code}m{s}\033[0m" if USE_COLOUR else s

def green(s):  return _c("32", s)
def red(s):    return _c("31", s)
def yellow(s): return _c("33", s)
def bold(s):   return _c("1", s)

PASSES, FAILS, WARNS = [], [], []

def ok(msg):
    PASSES.append(msg)
    print(f"  {green('PASS')}  {msg}")

def fail(msg, hint=None):
    FAILS.append(msg)
    print(f"  {red('FAIL')}  {msg}")
    if hint:
        print(f"        {yellow('hint:')} {hint}")

def warn(msg):
    WARNS.append(msg)
    print(f"  {yellow('WARN')}  {msg}")

def section(title):
    print()
    print(bold(f"--- {title} " + "-" * max(1, 66 - len(title))))

def pause(prompt):
    print()
    input(bold(f"  >> {prompt}  [press Enter] "))

def ask_yn(prompt):
    print()
    answer = input(bold(f"  >> {prompt}  [y/N] ")).strip().lower()
    return answer in ("y", "yes")

# --------------------------------------------------------------------------
# HTTP helpers (stdlib only)
# --------------------------------------------------------------------------

def http_json(method, url, body=None, auth=None, timeout=10):
    """Return (status, parsed-json-or-text, headers). Raises URLError on transport error."""
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if auth:
        token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode(errors="replace")
            status, hdrs = resp.status, dict(resp.headers)
    except urllib.error.HTTPError as e:
        raw = e.read().decode(errors="replace")
        status, hdrs = e.code, dict(e.headers or {})
    try:
        return status, (json.loads(raw) if raw.strip() else {}), hdrs
    except json.JSONDecodeError:
        return status, raw, hdrs

ID_KEYS = ("id", "document_id", "documentId", "documentID", "doc_id", "docId",
           "request_id", "requestId", "uuid", "correlation_id", "correlationId")

def extract_doc_id(payload, headers=None):
    """Pull a document id out of the many shapes teams use."""
    if isinstance(payload, dict):
        for key in ID_KEYS:
            if key in payload and payload[key] is not None:
                return str(payload[key])
        for v in payload.values():          # nested {"document": {"id": ...}}
            if isinstance(v, dict):
                nested = extract_doc_id(v)
                if nested:
                    return nested
        if len(payload) == 1:
            v = next(iter(payload.values()))
            if isinstance(v, (str, int)):
                return str(v)
    if isinstance(payload, str):
        s = payload.strip().strip('"')
        if s and len(s) < 200 and "\n" not in s:
            return s
    if headers:
        loc = headers.get("Location") or headers.get("location")
        if loc:
            return loc.rstrip("/").split("/")[-1]
    return None

RUNNING = ("pending", "queued", "accepted", "processing", "in_progress",
           "in-progress", "running", "started", "stored", "reserving",
           "embedding", "compensating", "new", "created", "ingesting")
DONE    = ("completed", "complete", "done", "success", "succeeded",
           "finished", "embedded", "indexed", "ready")
BAD     = ("failed", "error", "cancelled", "canceled", "rejected", "aborted")
COMP    = ("compensated", "rolled_back", "rolledback", "rolled-back", "reverted")

def norm_status(payload):
    """Normalise GET /documents/{id}/status to pending|completed|failed|compensated|unknown."""
    s = ""
    if isinstance(payload, dict):
        for key in ("status", "state", "saga_status", "sagaStatus", "document_status"):
            if key in payload and payload[key] is not None:
                s = str(payload[key]).lower()
                break
        else:
            for v in payload.values():
                if isinstance(v, dict):
                    inner = norm_status(v)
                    if inner != "unknown":
                        return inner
    elif isinstance(payload, str):
        s = payload.strip().strip('"').lower()
    if not s:
        return "unknown"
    if any(t == s or t in s for t in COMP):
        return "compensated"
    if any(t == s or t in s for t in BAD):
        return "failed"
    if any(t == s or t in s for t in DONE):
        return "completed"
    if any(t == s or t in s for t in RUNNING):
        return "pending"
    return "unknown"

# --------------------------------------------------------------------------
# document service driving
# --------------------------------------------------------------------------

def default_payload(tag):
    text = f"Harness document {tag}: a short passage about eventual consistency."
    return {
        "tenant_id": "harness", "tenant": "harness", "tenantId": "harness",
        "title": f"harness-{tag}", "name": f"harness-{tag}",
        "content": text, "text": text, "body": text, "document": text,
    }

def send_document(docs, payload, timeout=10):
    t0 = time.monotonic()
    code, body, hdrs = http_json("POST", f"{docs}/documents", payload, timeout=timeout)
    return code, body, hdrs, time.monotonic() - t0

def get_status(docs, did, timeout=10):
    code, body, _ = http_json("GET", f"{docs}/documents/{did}/status", timeout=timeout)
    return code, body

def wait_status(docs, did, timeout, targets, poll=1.0, quiet=False):
    """Poll until the normalised status is in targets (or failed/compensated).
    Returns the final normalised status."""
    deadline = time.monotonic() + timeout
    last = "unknown"
    while time.monotonic() < deadline:
        try:
            code, body = get_status(docs, did)
        except urllib.error.URLError:
            time.sleep(poll)
            continue
        if code == 404:
            last = "not-found"
        elif 200 <= code < 300:
            last = norm_status(body)
            if last in targets or last in ("failed", "compensated", "completed"):
                if last in targets:
                    return last
                return last          # settled, but not what we wanted
        if not quiet:
            print(f"        ... {did}: {last} ({int(deadline - time.monotonic())}s left)   ", end="\r")
        time.sleep(poll)
    return last

# --------------------------------------------------------------------------
# broker (RabbitMQ management API)
# --------------------------------------------------------------------------

class Broker:
    def __init__(self, api, auth):
        self.api = api.rstrip("/")
        self.auth = auth
        self.reachable = False

    def get(self, path):
        code, body, _ = http_json("GET", f"{self.api}{path}", auth=self.auth)
        return code, body

    def queues(self):
        code, body = self.get("/api/queues")
        return body if code == 200 and isinstance(body, list) else None

def queue_depths(broker):
    """{queue name: ready+unacked} or None if the API is unreachable."""
    qs = broker.queues()
    if qs is None:
        return None
    return {q.get("name", "?"): (q.get("messages_ready") or 0) + (q.get("messages_unacknowledged") or 0)
            for q in qs}

def publish_total(broker):
    """Sum of per-queue publish counters (messages routed into queues), or None."""
    qs = broker.queues()
    if qs is None:
        return None
    total = 0
    for q in qs:
        stats = q.get("message_stats") or {}
        total += stats.get("publish", 0)
    return total

# --------------------------------------------------------------------------
# criterion 1 - the contract and the happy path
# --------------------------------------------------------------------------

def check_quota_routes(quota):
    probe = {"tenant_id": "harness-probe", "tenant": "harness-probe",
             "tenantId": "harness-probe", "document_id": "harness-probe",
             "documentId": "harness-probe", "doc_id": "harness-probe",
             "units": 0, "amount": 0, "quantity": 0}
    okness = True
    for path in ("/quota/reserve", "/quota/release"):
        try:
            code, _, _ = http_json("POST", f"{quota}{path}", probe)
        except urllib.error.URLError as e:
            fail(f"cannot reach {quota}{path} ({e.reason})",
                 "is your Quota Service running on port 8081?")
            return False
        if code == 404:
            fail(f"POST {path} returned 404 on the Quota Service",
                 "the brief fixes these exact paths: POST /quota/reserve and POST /quota/release")
            okness = False
        elif code == 405:
            fail(f"{path} does not accept POST (HTTP 405)")
            okness = False
        else:
            ok(f"Quota Service route present: POST {path} (HTTP {code})")
    return okness

def check_embed_listening(host, port):
    try:
        with socket.create_connection((host, port), timeout=3):
            ok(f"Embedding Service is listening on {host}:{port}")
            return True
    except OSError:
        warn(f"nothing is listening on {host}:{port} - the brief assigns your Embedding "
             "Service port 8083. If it is a pure event consumer with no HTTP listener, "
             "tell the demonstrator; the saga completing below is the binding check.")
        return False

def check_criterion_1(quota, docs, embed_host, embed_port, broker, payload, timeout):
    section("Criterion 1a - three services on their fixed ports")
    check_quota_routes(quota)
    check_embed_listening(embed_host, embed_port)

    section("Criterion 1b - the happy path (POST /documents -> COMPLETED)")
    pub_before = publish_total(broker)

    try:
        code, body, hdrs, elapsed = send_document(docs, payload("c1"))
    except urllib.error.URLError as e:
        fail(f"cannot reach {docs}/documents ({e.reason})",
             "is your Document Service running on port 8082?")
        return None
    if not (200 <= code < 300):
        fail(f"POST /documents returned HTTP {code}",
             "the entry point must accept a JSON document and return 2xx (202 Accepted)")
        return None
    if code == 202:
        ok("POST /documents accepted the request (HTTP 202 Accepted)")
    else:
        ok(f"POST /documents accepted the request (HTTP {code})")
        warn("the brief specifies 202 Accepted for the asynchronous entry point")

    did = extract_doc_id(body, hdrs)
    if did is None:
        fail(f"could not find a document id in the response: {json.dumps(body)[:120]}",
             'return e.g. {"id": "..."} so the caller can poll /documents/{id}/status')
        return None
    ok(f"document id returned immediately: {did}")

    if elapsed < 3.0:
        ok(f"response was immediate ({elapsed*1000:.0f} ms) - the saga runs asynchronously")
    else:
        fail(f"POST /documents took {elapsed:.1f}s - it appears to run the saga synchronously",
             "return 202 + id once the request is recorded; let the saga proceed via events")

    try:
        code, sbody = get_status(docs, did)
    except urllib.error.URLError as e:
        fail(f"cannot reach {docs}/documents/{{id}}/status ({e.reason})")
        return did
    if code == 404:
        time.sleep(1.0)
        code, sbody = get_status(docs, did)
    if 200 <= code < 300:
        ok(f"GET /documents/{{id}}/status responds (HTTP {code}, status: {norm_status(sbody)})")
    else:
        fail(f"GET /documents/{did}/status returned HTTP {code}",
             'return 2xx with {"status": "PENDING"} while the saga is in flight')

    final = wait_status(docs, did, timeout, targets={"completed"})
    print()
    if final == "completed":
        ok(f"document {did} reached COMPLETED - quota reserved, stored, embedded")
    elif final in ("failed", "compensated"):
        fail(f"document {did} settled as {final.upper()} on the happy path",
             "is FAILURE_RATE set to 0.0 on the Embedding Service right now?")
    else:
        fail(f"document {did} did not complete within {timeout}s (last status: {final})",
             "check every service is running and consuming; use --timeout for a slow embedder")

    # events through the broker?
    if pub_before is not None:
        deadline = time.monotonic() + 15
        moved = False
        while time.monotonic() < deadline:
            pub_now = publish_total(broker)
            if pub_now is not None and pub_now > pub_before:
                ok(f"events flowed through RabbitMQ during the saga "
                   f"({pub_now - pub_before} message(s) routed into queues)")
                moved = True
                break
            time.sleep(2.0)
        if not moved:
            warn("no message traffic observed on the broker during the saga - the brief "
                 "requires events to flow through RabbitMQ (management stats can lag; "
                 "show the demonstrator the Rabbit UI if this looks wrong)")
    else:
        warn(f"management API not reachable at {broker.api} - broker checks skipped "
             "(default guest/guest, override with --rabbit-user/--rabbit-pass)")

    # several documents, distinct ids
    print("        sending 3 concurrent documents ...")
    results = [None, None, None]
    def _worker(i):
        try:
            c, b, h, _ = send_document(docs, payload(f"c1-par-{i}"))
            results[i] = extract_doc_id(b, h) if 200 <= c < 300 else None
        except urllib.error.URLError:
            results[i] = None
    threads = [threading.Thread(target=_worker, args=(i,)) for i in range(3)]
    for t in threads: t.start()
    for t in threads: t.join()
    ids = [r for r in results if r]
    if len(ids) == 3 and len(set(ids)) == 3:
        ok("3 concurrent documents accepted with 3 distinct ids (correlation ids intact)")
        done = sum(1 for r in ids
                   if wait_status(docs, r, timeout, targets={"completed"}, quiet=True) == "completed")
        print()
        if done == 3:
            ok("all 3 completed")
        else:
            fail(f"only {done}/3 completed within {timeout}s")
    else:
        fail(f"expected 3 distinct document ids, got {ids}")
    return did

# --------------------------------------------------------------------------
# criterion 2 - compensation under FAILURE_RATE=1.0, no orphaned state
# --------------------------------------------------------------------------

QUOTA_STATE_PATHS = ("/quota/state", "/quota", "/quota/status", "/quota/usage",
                     "/quotas", "/state")
QUOTA_KEY_HINTS = ("reserv", "used", "usage", "held", "charged", "balance",
                   "remaining", "available", "allocat")
# monotonic counters legitimately grow across a saga - never judge them
QUOTA_KEY_EXCLUDE = ("count", "total", "ever", "history", "events", "attempts",
                     "releases", "reserves", "_ops", "requests")

def find_quota_state(quota):
    """Return (path, body) of a readable quota-state endpoint, if the team built one."""
    for path in QUOTA_STATE_PATHS:
        try:
            code, body, _ = http_json("GET", f"{quota}{path}", timeout=3)
        except urllib.error.URLError:
            return None, None
        if 200 <= code < 300 and isinstance(body, (dict, list)):
            return path, body
    return None, None

def numeric_leaves(obj, prefix=""):
    """{dotted.path: number} for every quota-looking numeric leaf."""
    out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.update(numeric_leaves(v, f"{prefix}.{k}" if prefix else str(k)))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            out.update(numeric_leaves(v, f"{prefix}[{i}]"))
    elif isinstance(obj, (int, float)) and not isinstance(obj, bool):
        low = prefix.lower()
        if any(h in low for h in QUOTA_KEY_HINTS) and not any(x in low for x in QUOTA_KEY_EXCLUDE):
            out[prefix] = obj
    return out

def check_criterion_2(quota, docs, broker, payload, timeout):
    section("Criterion 2 - compensation under FAILURE_RATE=1.0, no orphaned state")
    print("  The harness sends documents while your Embedding Service fails every")
    print("  event. Each saga must settle as FAILED/COMPENSATED, the reserved quota")
    print("  must come back, and no messages may be left stranded on any queue.")

    state_path, state_before = find_quota_state(quota)
    if state_path:
        print(f"        (found quota state endpoint GET {state_path} - the quota check will be automatic)")

    depths_before = queue_depths(broker)

    pause("set FAILURE_RATE=1.0 on your Embedding Service, restart it, and press Enter when it is back up")

    if state_path:
        _, state_before = find_quota_state(quota)   # re-read after the restart pause

    settled = 0
    for i in (1, 2, 3):
        try:
            code, body, hdrs, _ = send_document(docs, payload(f"c2-{i}"))
        except urllib.error.URLError as e:
            fail(f"could not send document {i} ({e.reason})")
            return False
        did = extract_doc_id(body, hdrs) if 200 <= code < 300 else None
        if did is None:
            fail(f"document {i} was not accepted by POST /documents")
            return False
        final = wait_status(docs, did, timeout, targets={"failed", "compensated"})
        print()
        if final in ("failed", "compensated"):
            ok(f"document {did} settled as {final.upper()} under failure injection")
            settled += 1
        elif final == "completed":
            fail(f"document {did} COMPLETED although FAILURE_RATE=1.0",
                 "the Embedding Service must read FAILURE_RATE and actually fail - "
                 "did you restart it after setting the variable?")
            return False
        else:
            fail(f"document {did} never settled (last status: {final}) - the saga is stuck",
                 "on embedding failure something must mark the document FAILED and "
                 "release the quota; check your failure event / compensation handler")
            return False

    # stranded messages?
    if depths_before is not None:
        deadline = time.monotonic() + 20
        stranded = {}
        while time.monotonic() < deadline:
            now = queue_depths(broker)
            if now is None:
                break
            stranded = {q: d for q, d in now.items() if d > depths_before.get(q, 0)}
            if not stranded:
                break
            time.sleep(2.0)
        if not stranded:
            ok("no messages left stranded on any queue after the failed sagas")
        else:
            fail("messages left stranded after the failed sagas: "
                 + ", ".join(f'"{q}" (+{d - depths_before.get(q, 0)})' for q, d in stranded.items()),
                 "every event of a failed saga must be consumed and acknowledged - "
                 "unconsumed failure events mean nobody is running the compensation")

    # quota back where it started?
    if state_path:
        before = numeric_leaves(state_before)
        deadline = time.monotonic() + 20
        after = {}
        while time.monotonic() < deadline:
            _, state_after = find_quota_state(quota)
            after = numeric_leaves(state_after)
            if after == before:
                break
            time.sleep(1.5)
        if before and after == before:
            ok(f"reserved quota returned to its starting value (GET {state_path} unchanged "
               "across three failed sagas)")
        elif not before:
            if ask_yn("no quota counters found to compare - show the demonstrator the "
                      "release in your Quota Service log. Demonstrator: releases seen?"):
                ok("quota release confirmed by the demonstrator (from the Quota Service log)")
            else:
                fail("quota release not confirmed",
                     "the compensation must call POST /quota/release for every failed document")
                return False
        else:
            changed = [f"{k}: {before.get(k)} -> {after.get(k)}"
                       for k in sorted(set(before) | set(after))
                       if before.get(k) != after.get(k)]
            print(yellow("        quota values that changed across the failed sagas:"))
            for line in changed[:6]:
                print(f"          {line}")
            if ask_yn("some of these may be counters that legitimately grow. Demonstrator: "
                      "looking at the quota state, is the RESERVED quota back at its "
                      "starting level?"):
                ok("quota release confirmed by the demonstrator (state inspected together)")
            else:
                fail("quota state did not return to its starting value after compensation: "
                     + "; ".join(changed[:4]),
                     "reserved quota is stranded - the compensation must release exactly "
                     "what the failed saga reserved")
                return False
    else:
        if ask_yn("no quota state endpoint found (that is fine). Show the demonstrator "
                  "your Quota Service log: one /quota/release per failed document. "
                  "Demonstrator: releases seen?"):
            ok("quota release confirmed by the demonstrator (from the Quota Service log)")
        else:
            fail("quota release not confirmed",
                 "the compensation must call POST /quota/release for every failed document")
            return False

    pause("set FAILURE_RATE back to 0.0, restart the Embedding Service, and press Enter")
    try:
        code, body, hdrs, _ = send_document(docs, payload("c2-recovery"))
    except urllib.error.URLError as e:
        fail(f"could not send the recovery document ({e.reason})")
        return False
    did = extract_doc_id(body, hdrs) if 200 <= code < 300 else None
    if did is None:
        fail("recovery document was not accepted")
        return False
    final = wait_status(docs, did, timeout, targets={"completed"})
    print()
    if final == "completed":
        ok(f"recovery document {did} COMPLETED - the system converged and quota is usable again")
        return settled == 3
    fail(f"recovery document did not complete (status: {final})",
         "after compensation the system must serve new requests normally")
    return False

# --------------------------------------------------------------------------
# criterion 3 - the inconsistency window, measured in the room
# --------------------------------------------------------------------------

def check_criterion_3(docs, payload, n, timeout):
    section(f"Criterion 3 - the inconsistency window ({n} fresh requests)")
    print("  Measures the time from POST /documents returning to the status")
    print("  endpoint reporting COMPLETED - a fresh run, on this machine, now.")

    samples, failures = [], 0
    for i in range(1, n + 1):
        try:
            code, body, hdrs, _ = send_document(docs, payload(f"c3-{i:02d}"))
        except urllib.error.URLError as e:
            fail(f"request {i}: could not reach POST /documents ({e.reason})")
            return False
        did = extract_doc_id(body, hdrs) if 200 <= code < 300 else None
        if did is None:
            failures += 1
            print(f"        {i:>3}/{n}  not accepted (HTTP {code})")
            continue
        t0 = time.monotonic()
        deadline = t0 + timeout
        final = "unknown"
        while time.monotonic() < deadline:
            try:
                c, b = get_status(docs, did)
            except urllib.error.URLError:
                time.sleep(0.15)
                continue
            if 200 <= c < 300:
                final = norm_status(b)
                if final in ("completed", "failed", "compensated"):
                    break
            time.sleep(0.15)
        if final == "completed":
            ms = (time.monotonic() - t0) * 1000
            samples.append(ms)
            print(f"        {i:>3}/{n}  {ms:7.0f} ms")
        else:
            failures += 1
            print(f"        {i:>3}/{n}  did not complete ({final})")

    if not samples:
        fail("no request completed - cannot measure the window")
        return False
    if failures:
        warn(f"{failures}/{n} request(s) did not complete and were excluded")

    mn, md, mx = min(samples), statistics.median(samples), max(samples)
    print()
    print(bold("        YOUR NUMBERS - from this run, in this room:"))
    print(bold(f"          min {mn:.0f} ms   median {md:.0f} ms   max {mx:.0f} ms   ({len(samples)} requests)"))
    print()
    print("        This is POST -> status COMPLETED through your own status endpoint.")
    print("        If you built a separate search projection (Exercise 2.3), its lag is")
    print("        the window the brief defines - measure that too and quote whichever")
    print("        you built. Be ready to explain what the number means.")

    if len(samples) >= max(3, int(0.8 * n)):
        ok(f"inconsistency window measured over {len(samples)} requests: "
           f"min {mn:.0f} / median {md:.0f} / max {mx:.0f} ms")
        return True
    fail(f"only {len(samples)}/{n} requests completed - fix the pipeline, then measure again")
    return False

# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="COMP41720 Lab 2 test harness (sagas and eventual consistency).",
        epilog="Green here means green in the room. Run early, run often.")
    ap.add_argument("--quota", default="http://localhost:8081",
                    help="base URL of your Quota Service (default: %(default)s)")
    ap.add_argument("--docs", default="http://localhost:8082",
                    help="base URL of your Document Service (default: %(default)s)")
    ap.add_argument("--embed", default="localhost:8083",
                    help="host:port of your Embedding Service (default: %(default)s)")
    ap.add_argument("--rabbit", default="http://localhost:15672",
                    help="RabbitMQ management API URL (default: %(default)s)")
    ap.add_argument("--rabbit-user", default="guest")
    ap.add_argument("--rabbit-pass", default="guest")
    ap.add_argument("--payload", metavar="JSON",
                    help="JSON object to POST to /documents instead of the tolerant default")
    ap.add_argument("--requests", type=int, default=20,
                    help="requests for the inconsistency-window measurement (default: %(default)s)")
    ap.add_argument("--timeout", type=int, default=120,
                    help="seconds to wait for a saga to settle (default: %(default)s)")
    ap.add_argument("--quick", action="store_true",
                    help="run only criterion 1 (no interactive parts)")
    ap.add_argument("--window", action="store_true",
                    help="run only the criterion 3 measurement")
    args = ap.parse_args()

    if args.payload:
        try:
            fixed = json.loads(args.payload)
            if not isinstance(fixed, dict):
                raise ValueError
        except (json.JSONDecodeError, ValueError):
            print(red("--payload must be a JSON object, e.g. --payload '{\"tenant\": \"t1\", \"text\": \"...\"}'"))
            sys.exit(2)
        payload = lambda tag: dict(fixed)
    else:
        payload = default_payload

    host, _, port = args.embed.partition(":")
    embed_host, embed_port = host or "localhost", int(port or 8083)

    print(bold(f"COMP41720 Lab 2 harness {VERSION}"))
    print(f"quota: {args.quota}   documents: {args.docs}   "
          f"embedding: {embed_host}:{embed_port}   broker API: {args.rabbit}")

    broker = Broker(args.rabbit, (args.rabbit_user, args.rabbit_pass))

    c1 = c2 = c3 = None
    if args.window:
        c3 = check_criterion_3(args.docs, payload, args.requests, args.timeout)
    else:
        check_criterion_1(args.quota, args.docs, embed_host, embed_port,
                          broker, payload, args.timeout)
        c1 = not FAILS
        if not args.quick:
            c2 = check_criterion_2(args.quota, args.docs, broker, payload, args.timeout)
            c3 = check_criterion_3(args.docs, payload, args.requests, args.timeout)

    # ---------------------------------------------------------------- summary
    def box(flag):
        if flag is None:
            return "[- ]"
        return green("[ x ]") if flag else red("[   ]")

    section("Summary - demonstrator tick-sheet")
    print(f"   {box(c1)}  1. Happy path green across Quota (:8081), Document (:8082),")
    print(f"            Embedding (:8083); saga completes, events through RabbitMQ")
    print(f"   {box(c2)}  2. Compensation under raised FAILURE_RATE - sagas settle,")
    print(f"            quota released, nothing stranded")
    print(f"   {box(c3)}  3. Own inconsistency-window numbers, measured in the room")
    if args.quick:
        print(f"\n   (criteria 2-3 not run: --quick. Run the full harness in the session.)")
    if args.window:
        print(f"\n   (criteria 1-2 not run: --window measures the window only.)")
    print()
    total_bad = len(FAILS)
    if total_bad == 0:
        print(green(bold("   ALL CHECKS PASSED")) + f"   ({len(PASSES)} passes, {len(WARNS)} warnings)")
    else:
        print(red(bold(f"   {total_bad} CHECK(S) FAILED")) + f"   ({len(PASSES)} passes, {len(WARNS)} warnings)")
        print("   failed:")
        for f_ in FAILS:
            print(f"     - {f_}")
    sys.exit(0 if total_bad == 0 else 1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\ninterrupted.")
        sys.exit(130)
