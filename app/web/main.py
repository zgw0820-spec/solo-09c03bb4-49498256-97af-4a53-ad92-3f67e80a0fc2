"""Coordinator + web UI.

Flow:
- CSV uploads (request_id,timestamp,status) are stored on shared storage.
- A job runs map tasks (one per file -> R shards, kept on the worker that
  produced them) and then reduce tasks (one per partition -> latest record
  per request id).
- A worker can be stopped from the UI mid-merge. Its shards are lost and
  recomputed by surviving workers from the original files; intact shards are
  reused. Merges that read a lost shard but have not committed are
  invalidated and re-run; late commits from invalidated tasks are fenced by
  epochs and never reach the report.
- Once every merge has committed the job enters PUBLISH: the merge results are
  final and kept on shared storage. A background publisher builds report.json
  and retries on write failure; while it retries, worker deaths do not
  recompute shards or re-run merges (they are no longer needed), and a
  coordinator restart resumes publishing from the persisted state. The report
  is shown only after report.json has been written successfully.
"""
from __future__ import annotations

import json
import os
import shutil
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from .store import Store

DATA_DIR = os.environ.get("DATA_DIR", "./data")
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
MERGE_DIR = os.path.join(DATA_DIR, "merge")
STATE_PATH = os.path.join(DATA_DIR, "state", "state.json")
HEARTBEAT_TIMEOUT = float(os.environ.get("HEARTBEAT_TIMEOUT", "10"))
NPARTITIONS = int(os.environ.get("JOB_PARTITIONS", "8"))


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


# Report publishing: retried in the background until report.json is written.
PUBLISH_RETRY_MIN = max(0.05, _env_float("PUBLISH_RETRY_MS", 500) / 1000.0)
PUBLISH_RETRY_MAX = max(PUBLISH_RETRY_MIN, _env_float("PUBLISH_RETRY_MAX_MS", 3000) / 1000.0)

store = Store(STATE_PATH)
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))
_publish_wakeup = threading.Event()


# ------------------------------------------------------------------ helpers

def now() -> float:
    return time.time()


def iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def log_event(job: dict, msg: str) -> None:
    job["events"].append({"ts": iso(), "msg": msg})
    if len(job["events"]) > 1000:
        del job["events"][:-1000]


def shard_key(fid: str, part: int) -> str:
    return f"{fid}:{part}"


def job_phase(job: dict) -> str:
    if job["done"]:
        return "COMPLETE"
    if job.get("publish"):
        return "PUBLISH"
    if all(m["state"] == "DONE" for m in job["maps"].values()):
        return "REDUCE"
    return "MAP"


def reduces_final(job: dict) -> bool:
    """Every merge committed: results are frozen, shards/maps are irrelevant."""
    return all(rd["state"] == "COMMITTED" for rd in job["reduces"].values())


def results_final(job: dict) -> bool:
    """The accepted result set is fixed: publishing (done or pending)."""
    return bool(job["done"] or job.get("publish") or reduces_final(job))


def refresh_reduces(job: dict) -> None:
    """A pending reduce becomes READY once every input shard is OK."""
    for p in range(job["r"]):
        rd = job["reduces"][str(p)]
        if rd["state"] in ("PENDING", "READY"):
            ok = all(
                job["shards"].get(shard_key(fid, p), {}).get("status") == "OK"
                for fid in job["files"]
            )
            rd["state"] = "READY" if ok else "PENDING"


def _invalidate_reduces(job: dict) -> None:
    """Invalidate unfinished merges whose inputs include a lost shard.

    Committed merges stay valid: map output is deterministic, so recomputed
    shards are identical to the ones the committed merge already consumed.
    """
    for p in range(job["r"]):
        rd = job["reduces"][str(p)]
        deps_lost = any(
            job["shards"].get(shard_key(fid, p), {}).get("status") != "OK"
            for fid in job["files"]
        )
        if not deps_lost:
            continue
        if rd["state"] == "ASSIGNED":
            rd.update(state="PENDING", worker=None)
            rd["epoch"] += 1
            log_event(job, f"reduce {p} INVALIDATED (read a lost shard before "
                           f"finishing); will re-run, its late commit will be rejected")
        elif rd["state"] == "READY":
            rd["state"] = "PENDING"


def _mark_lost(job: dict, keys: list, reason: str) -> int:
    """Flip OK shards to LOST (bookkeeping only). Returns how many were lost."""
    lost = 0
    for k in keys:
        s = job["shards"].get(k)
        if s and s["status"] == "OK":
            s["status"] = "LOST"
            lost += 1
    if lost:
        log_event(job, f"{reason}: {lost} shard(s) LOST")
    return lost


def _lose_shards(job: dict, keys: list, reason: str) -> None:
    """Mark shards lost, schedule recompute of their files, invalidate merges."""
    lost = _mark_lost(job, keys, reason)
    if not lost:
        return
    if results_final(job):
        # Results already accepted (or being published): shards are no longer
        # inputs of anything, so a loss must not trigger recompute or re-run.
        log_event(job, "all merges already committed; accepted merge results "
                       "kept, no recompute scheduled")
        return
    for fid in {k.split(":")[0] for k in keys}:
        m = job["maps"][fid]
        if m["state"] == "DONE":
            m.update(state="PENDING", worker=None)
            m["epoch"] += 1
            log_event(job, f"recompute scheduled: file "
                           f"{store.data['files'][fid]['name']} from the original CSV "
                           f"(intact shards of surviving workers keep being reused)")
    _invalidate_reduces(job)


def on_worker_dead(wid: str, reason: str) -> None:
    """Caller must hold store.lock."""
    w = store.data["workers"].get(wid)
    if not w or w["state"] == "DEAD":
        return
    w["state"] = "DEAD"
    for jid in store.data["order"]:
        job = store.data["jobs"][jid]
        if job["done"]:
            continue
        if results_final(job):
            # Every merge is committed and its output lives on shared storage:
            # the shards it consumed are no longer inputs of anything. Record
            # the loss for the UI, but do NOT recompute or re-run anything.
            lost_keys = [k for k, s in job["shards"].items()
                         if s["worker"] == wid and s["status"] == "OK"]
            lost = _mark_lost(job, lost_keys, f"worker {wid} {reason}")
            log_event(job, f"all merges already committed - {lost} shard(s) "
                           f"no longer needed; job keeps its results and "
                           f"publishes without recompute")
            continue
        # tasks running on the dead worker are gone with it
        for fid, m in job["maps"].items():
            if m["state"] == "ASSIGNED" and m["worker"] == wid:
                m.update(state="PENDING", worker=None)
                m["epoch"] += 1
                log_event(job, f"map task for {store.data['files'][fid]['name']} "
                               f"died with {wid}; re-enqueued")
        for p in range(job["r"]):
            rd = job["reduces"][str(p)]
            if rd["state"] == "ASSIGNED" and rd["worker"] == wid:
                rd.update(state="PENDING", worker=None)
                rd["epoch"] += 1
                log_event(job, f"reduce {p} died with {wid}; re-enqueued")
        # its shards are lost: recompute them, invalidate surviving merges
        lost_keys = [k for k, s in job["shards"].items()
                     if s["worker"] == wid and s["status"] == "OK"]
        _lose_shards(job, lost_keys, f"worker {wid} {reason}")
        refresh_reduces(job)


def _collect_report(job: dict) -> dict:
    """Build the report from the committed merge outputs only.

    The reduce outputs are the accepted result set; nothing here reads shards,
    so publishing keeps working even if every worker has gone away.
    """
    records = []
    for p in range(job["r"]):
        out = job["reduces"][str(p)]["output"]
        with open(out, encoding="utf-8") as f:
            records.extend(json.load(f)["records"])
    still = [r for r in records if r["attempts"] >= 2 and 500 <= r["s"] < 600]
    still.sort(key=lambda r: r["rid"])
    # Each valid input record lands in exactly one (file, partition) shard and
    # is folded into exactly one request entry, so the attempts sum is the
    # accepted record count - independent of later shard loss.
    return {
        "total_records": sum(r["attempts"] for r in records),
        "total_requests": len(records),
        "retried": sum(1 for r in records if r["attempts"] >= 2),
        "still_5xx": [
            {"request_id": r["rid"], "attempts": r["attempts"],
             "latest_status": r["s"], "latest_ts": r["ts"]}
            for r in still
        ],
        "single_5xx": sum(1 for r in records
                          if r["attempts"] == 1 and 500 <= r["s"] < 600),
    }


def _report_path(job: dict) -> str:
    return os.path.join(MERGE_DIR, job["id"], "report.json")


def _write_report(report: dict, path: str) -> None:
    """Atomically write report.json; raises on any I/O failure."""
    tmp = path + f".tmp-{os.getpid()}-{threading.get_ident()}"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    try:
        dfd = os.open(os.path.dirname(path), os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except OSError:
        pass


def _publish(job: dict) -> bool:
    """One publish attempt, retried in the background until it succeeds.

    File I/O happens outside ``store.lock`` so a stuck write cannot block the
    web UI or task assignment. Only the short state transitions are locked.
    On success the job becomes done (report visible). On failure the accepted
    results stay untouched and the caller retries later; ``publish`` survives
    a coordinator restart, so publishing resumes from the persisted state.
    """
    jid = job["id"]
    try:
        path = _report_path(job)
        report = _collect_report(job)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        _write_report(report, path)
    except Exception as e:  # noqa: BLE001 - any write failure must be retried
        with store.lock:
            cur = store.data["jobs"].get(jid)
            pub = cur and cur.get("publish")
            if not pub or cur["done"]:
                return False
            pub["fails"] = pub.get("fails", 0) + 1
            pub["last_error"] = f"{type(e).__name__}: {e}"
            delay = min(PUBLISH_RETRY_MAX,
                        PUBLISH_RETRY_MIN * (2 ** min(pub["fails"] - 1, 8)))
            pub["next_try"] = now() + delay
            if pub["fails"] == 1 or pub["fails"] % 20 == 0:
                log_event(cur, f"report publish FAILED ({pub['last_error']}); "
                               f"merge results kept on shared storage, retrying "
                               f"every {delay:.1f}s - attempt {pub['fails']}")
            try:
                store.save()
            except OSError:
                pass  # in-memory commit state is kept; retried by the publisher
        return False
    with store.lock:
        cur = store.data["jobs"].get(jid)
        if not cur or cur["done"]:
            return True
        cur["report"] = report
        cur["publish"].update(state="DONE", done_at=iso(), last_error=None)
        cur["done"] = True
        log_event(cur, f"report published: {len(report['still_5xx'])} requests "
                       f"still 5xx after retry (of {report['total_requests']} "
                       f"requests)")
        store.save()
    return True


def maybe_complete(job: dict) -> None:
    """Enter the publish phase once every merge is committed.

    Only marks the job; the publisher thread does the (possibly slow) file I/O
    outside ``store.lock``. Caller holds the lock.
    """
    if job["done"] or job.get("publish"):
        return
    if not reduces_final(job):
        return
    job["publish"] = {"state": "PENDING", "since": iso(), "fails": 0,
                      "next_try": 0.0, "last_error": None}
    log_event(job, "all merges committed; results are final - publishing report "
                   "(a worker dying from now on does not trigger recompute)")
    store.save()
    _publish_wakeup.set()


def assign_task(wid: str):
    """Pick the next task for a worker. Caller holds store.lock."""
    for jid in store.data["order"]:
        job = store.data["jobs"][jid]
        if results_final(job):
            # done or publishing: no new task can change the report
            continue
        for fid in job["files"]:
            m = job["maps"][fid]
            if m["state"] == "PENDING":
                m.update(state="ASSIGNED", worker=wid, assigned=now())
                store.save()
                f = store.data["files"][fid]
                return {"type": "map", "job_id": jid, "file_id": fid,
                        "epoch": m["epoch"], "csv_path": f["path"], "r": job["r"]}
        for p in range(job["r"]):
            rd = job["reduces"][str(p)]
            if rd["state"] == "READY":
                inputs = []
                for fid in job["files"]:
                    s = job["shards"][shard_key(fid, p)]
                    w = store.data["workers"][s["worker"]]
                    inputs.append({
                        "file_id": fid, "worker_id": s["worker"],
                        "url": f"{w['base_url']}/shard-files/{jid}/{fid}/part-{p}.jsonl",
                    })
                rd.update(state="ASSIGNED", worker=wid, assigned=now())
                tmp = os.path.join(MERGE_DIR, jid,
                                   f"part-{p}.epoch-{rd['epoch']}.tmp.json")
                store.save()
                return {"type": "reduce", "job_id": jid, "partition": p,
                        "epoch": rd["epoch"], "inputs": inputs, "tmp_out": tmp}
    return None


def _peer_alive(base_url: str) -> bool:
    try:
        r = httpx.get(f"{base_url}/health", timeout=1.5)
        return r.status_code == 200
    except httpx.HTTPError:
        return False


# ------------------------------------------------------------------ app

def _recover_after_restart() -> None:
    """Caller must hold store.lock.

    Tasks the previous coordinator process had handed out can no longer
    commit: the worker may have died with it, or its commit POST may have been
    lost. Re-enqueue them with a new epoch; a late commit from the old attempt
    is fenced and discarded. Committed merges are never touched.
    """
    for jid in store.data["order"]:
        job = store.data["jobs"][jid]
        if results_final(job):
            continue
        recovered = 0
        for m in job["maps"].values():
            if m["state"] == "ASSIGNED":
                m.update(state="PENDING", worker=None)
                m["epoch"] += 1
                recovered += 1
        for rd in job["reduces"].values():
            # ASSIGNED only: READY/PENDING/COMMITTED carry no in-flight write
            if rd["state"] == "ASSIGNED":
                rd.update(state="PENDING", worker=None)
                rd["epoch"] += 1
                recovered += 1
        if recovered:
            log_event(job, f"coordinator restart: {recovered} in-flight task(s) "
                           f"re-enqueued with a new epoch (their old attempts can "
                           f"no longer commit)")
            refresh_reduces(job)
    store.save()


@asynccontextmanager
async def lifespan(_app):
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    os.makedirs(MERGE_DIR, exist_ok=True)
    with store.lock:
        _recover_after_restart()
        # A coordinator restart must not strand a job whose merges were all
        # accepted: resume publishing from the persisted state.
        for job in store.data["jobs"].values():
            if job["done"]:
                continue
            if job.get("publish"):
                _publish_wakeup.set()
            elif reduces_final(job):
                maybe_complete(job)
    threading.Thread(target=_reaper, daemon=True).start()
    threading.Thread(target=_publisher, daemon=True).start()
    yield


app = FastAPI(title="Retry-5xx log analyzer", lifespan=lifespan)


def _reaper() -> None:
    while True:
        time.sleep(2)
        try:
            with store.lock:
                for wid, w in list(store.data["workers"].items()):
                    if (w["state"] == "ALIVE"
                            and now() - w["last_heartbeat"] > HEARTBEAT_TIMEOUT):
                        on_worker_dead(wid, "heartbeat timeout")
        except Exception:  # noqa: BLE001 - the reaper must never die
            pass


def _publisher() -> None:
    """Retry failed report writes in the background; never lose the results."""
    while True:
        try:
            with store.lock:
                due = [job for job in store.data["jobs"].values()
                       if not job["done"] and job.get("publish")
                       and now() >= job["publish"].get("next_try", 0)]
            for job in due:
                _publish(job)
        except Exception:  # noqa: BLE001 - the publisher must never die
            pass
        _publish_wakeup.wait(0.25)
        _publish_wakeup.clear()


# ------------------------------------------------------------- UI pages

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    with store.lock:
        workers = {wid: dict(w, hb_age=int(now() - w["last_heartbeat"]))
                   for wid, w in store.data["workers"].items()}
        ctx = {"request": request, "files": store.data["files"],
               "workers": workers, "jobs": store.data["jobs"],
               "order": store.data["order"],
               "phase": {j: job_phase(jb) for j, jb in store.data["jobs"].items()}}
    return templates.TemplateResponse(request=request, name="index.html", context=ctx)


@app.post("/upload")
async def upload_ui(files: list[UploadFile] = File(...)):
    _save_uploads(files)
    return RedirectResponse("/", 303)


@app.post("/api/upload")
async def upload_api(files: list[UploadFile] = File(...)):
    return {"file_ids": _save_uploads(files)}


def _save_uploads(ufs) -> list:
    ids = []
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    for uf in ufs:
        if not uf.filename:
            continue
        fid = uuid.uuid4().hex[:8]
        safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in uf.filename)
        path = os.path.join(UPLOAD_DIR, f"{fid}__{safe}")
        with open(path, "wb") as out:
            shutil.copyfileobj(uf.file, out)
        with store.lock:
            store.data["files"][fid] = {"id": fid, "name": uf.filename, "path": path,
                                        "size": os.path.getsize(path),
                                        "uploaded": iso()}
            store.save()
        ids.append(fid)
    return ids


@app.post("/jobs/start")
def start_job_ui(file_ids: list[str] = Form(...)):
    jid = _create_job(file_ids)
    return RedirectResponse(f"/jobs/{jid}", 303)


class StartJob(BaseModel):
    file_ids: list[str]


@app.post("/api/jobs/start")
def start_job_api(body: StartJob):
    return {"job_id": _create_job(body.file_ids)}


def _create_job(file_ids) -> str:
    with store.lock:
        files = [f for f in file_ids if f in store.data["files"]]
        if not files:
            raise HTTPException(400, "no valid files selected")
        jid = uuid.uuid4().hex[:8]
        os.makedirs(os.path.join(MERGE_DIR, jid), exist_ok=True)
        job = {
            "id": jid, "created": iso(), "r": NPARTITIONS, "files": files,
            "maps": {fid: {"epoch": 1, "state": "PENDING", "worker": None,
                           "records": 0, "fails": 0} for fid in files},
            "shards": {},
            "reduces": {str(p): {"epoch": 1, "state": "PENDING", "worker": None,
                                 "records": 0, "output": None}
                        for p in range(NPARTITIONS)},
            "events": [], "report": None, "done": False,
        }
        store.data["jobs"][jid] = job
        store.data["order"].append(jid)
        log_event(job, f"job created: {len(files)} file(s), "
                       f"{NPARTITIONS} reduce partitions")
        store.save()
        return jid


@app.get("/jobs/{jid}", response_class=HTMLResponse)
def job_page(request: Request, jid: str):
    with store.lock:
        job = store.data["jobs"].get(jid)
        if not job:
            raise HTTPException(404, "unknown job")
        grid = {fid: [job["shards"].get(shard_key(fid, p)) for p in range(job["r"])]
                for fid in job["files"]}
        shard_count = {}
        for s in job["shards"].values():
            if s["status"] == "OK":
                shard_count[s["worker"]] = shard_count.get(s["worker"], 0) + 1
        ctx = {
            "request": request, "job": job, "phase": job_phase(job),
            "files": store.data["files"], "workers": store.data["workers"],
            "grid": grid, "parts": range(job["r"]), "shard_count": shard_count,
            "maps": [(fid, job["maps"][fid]) for fid in job["files"]],
            "maps_done": sum(1 for m in job["maps"].values() if m["state"] == "DONE"),
            "reduces_done": sum(1 for r in job["reduces"].values()
                                if r["state"] == "COMMITTED"),
            "reduces": sorted(job["reduces"].items(), key=lambda kv: int(kv[0])),
            "events": list(reversed(job["events"])),
            "publish": job.get("publish"),
        }
    return templates.TemplateResponse(request=request, name="job.html", context=ctx)


@app.post("/jobs/{jid}/kill/{wid}")
def kill_ui(jid: str, wid: str):
    _kill_worker(wid)
    return RedirectResponse(f"/jobs/{jid}", 303)


@app.post("/api/jobs/{jid}/kill/{wid}")
def kill_api(jid: str, wid: str):
    _kill_worker(wid)
    return {"ok": True}


def _kill_worker(wid: str) -> None:
    with store.lock:
        w = store.data["workers"].get(wid)
        base = w["base_url"] if w and w["state"] == "ALIVE" else None
    if base:
        try:
            httpx.post(f"{base}/control/kill", timeout=2)
        except httpx.HTTPError:
            pass
    with store.lock:
        on_worker_dead(wid, "stopped via web UI")
        store.save()


@app.post("/reset")
def reset():
    with store.lock:
        store.data["files"] = {}
        store.data["jobs"] = {}
        store.data["order"] = []
        store.save()
    shutil.rmtree(UPLOAD_DIR, ignore_errors=True)
    shutil.rmtree(MERGE_DIR, ignore_errors=True)
    return RedirectResponse("/", 303)


# ------------------------------------------------------------- worker API

class Register(BaseModel):
    worker_id: str
    boot_id: str
    base_url: str


@app.post("/api/workers/register")
def register(body: Register):
    with store.lock:
        old = store.data["workers"].get(body.worker_id)
        if old and old.get("boot_id") != body.boot_id and old["state"] == "ALIVE":
            on_worker_dead(body.worker_id, "re-registered with a new boot id")
        store.data["workers"][body.worker_id] = {
            "state": "ALIVE", "boot_id": body.boot_id, "base_url": body.base_url,
            "last_heartbeat": now(), "tasks_done": (old or {}).get("tasks_done", 0),
        }
        store.save()
    return {"ok": True}


class Heartbeat(BaseModel):
    boot_id: str
    base_url: str


@app.post("/api/workers/{wid}/heartbeat")
def heartbeat(wid: str, body: Heartbeat):
    with store.lock:
        w = store.data["workers"].get(wid)
        if not w or w.get("boot_id") != body.boot_id:
            if w and w["state"] == "ALIVE":
                on_worker_dead(wid, "heartbeat with a new boot id")
            store.data["workers"][wid] = {
                "state": "ALIVE", "boot_id": body.boot_id, "base_url": body.base_url,
                "last_heartbeat": now(), "tasks_done": (w or {}).get("tasks_done", 0)}
            store.save()
        else:
            w["last_heartbeat"] = now()
            w["base_url"] = body.base_url
    return {"ok": True}


@app.get("/api/tasks/next")
def tasks_next(worker_id: str, boot_id: str):
    with store.lock:
        w = store.data["workers"].get(worker_id)
        if not w or w["state"] != "ALIVE" or w.get("boot_id") != boot_id:
            raise HTTPException(403, "worker not registered")
        w["last_heartbeat"] = now()
        task = assign_task(worker_id)
    return {"task": task}


class ShardInfo(BaseModel):
    part: int
    records: int


class MapCommit(BaseModel):
    job_id: str
    file_id: str
    epoch: int
    boot_id: str
    shards: list[ShardInfo]


@app.post("/api/tasks/map-commit/{wid}")
def map_commit(wid: str, body: MapCommit):
    with store.lock:
        w = store.data["workers"].get(wid)
        job = store.data["jobs"].get(body.job_id)
        if not w or w["state"] != "ALIVE" or not job or w["boot_id"] != body.boot_id:
            raise HTTPException(403, "unknown worker or job")
        m = job["maps"][body.file_id]
        if (m["epoch"] != body.epoch or m["state"] != "ASSIGNED"
                or m["worker"] != wid):
            log_event(job, f"rejected stale map commit for "
                           f"{store.data['files'][body.file_id]['name']} from {wid} "
                           f"(epoch {body.epoch}, current {m['epoch']})")
            store.save()
            raise HTTPException(409, "stale map commit")
        total = 0
        for sh in body.shards:
            job["shards"][shard_key(body.file_id, sh.part)] = {
                "worker": wid, "status": "OK", "epoch": body.epoch,
                "records": sh.records}
            total += sh.records
        m.update(state="DONE", worker=wid, records=total)
        w["tasks_done"] += 1
        note = " (recomputed after shard loss)" if body.epoch > 1 else ""
        log_event(job, f"map done: {store.data['files'][body.file_id]['name']} by "
                       f"{wid} ({len(body.shards)} shards, {total} records){note}")
        refresh_reduces(job)
        store.save()
    return {"ok": True}


class ReduceCommit(BaseModel):
    job_id: str
    partition: int
    epoch: int
    boot_id: str
    tmp_path: str
    records: int


@app.post("/api/tasks/reduce-commit/{wid}")
def reduce_commit(wid: str, body: ReduceCommit):
    with store.lock:
        w = store.data["workers"].get(wid)
        job = store.data["jobs"].get(body.job_id)
        if not w or w["state"] != "ALIVE" or not job or w["boot_id"] != body.boot_id:
            raise HTTPException(403, "unknown worker or job")
        rd = job["reduces"][str(body.partition)]
        if (rd["state"] == "COMMITTED" or rd["epoch"] != body.epoch
                or rd["state"] != "ASSIGNED" or rd["worker"] != wid):
            try:
                os.unlink(body.tmp_path)
            except OSError:
                pass
            why = ("already committed" if rd["state"] == "COMMITTED"
                   else f"epoch {body.epoch} != current {rd['epoch']} "
                        f"(task was invalidated)")
            log_event(job, f"rejected late reduce commit: partition "
                           f"{body.partition} from {wid} - {why}; discarded, "
                           f"NOT in report")
            store.save()
            raise HTTPException(409, "stale reduce commit discarded")
        final = os.path.join(MERGE_DIR, body.job_id, f"part-{body.partition}.json")
        os.makedirs(os.path.dirname(final), exist_ok=True)
        os.replace(body.tmp_path, final)
        rd.update(state="COMMITTED", output=final, records=body.records)
        w["tasks_done"] += 1
        log_event(job, f"reduce {body.partition} committed by {wid} "
                       f"({body.records} requests)")
        maybe_complete(job)
        store.save()
    return {"ok": True}


class TaskFail(BaseModel):
    type: str
    job_id: str
    epoch: int
    boot_id: str
    partition: int | None = None
    file_id: str | None = None
    reason: str = ""
    missing: dict | None = None


@app.post("/api/tasks/fail/{wid}")
def task_fail(wid: str, body: TaskFail):
    with store.lock:
        job = store.data["jobs"].get(body.job_id)
        if not job:
            raise HTTPException(404, "unknown job")
        if body.type == "reduce" and body.partition is not None:
            rd = job["reduces"][str(body.partition)]
            if (rd["state"] == "ASSIGNED" and rd["worker"] == wid
                    and rd["epoch"] == body.epoch):
                rd.update(state="PENDING", worker=None)
                rd["epoch"] += 1
                log_event(job, f"reduce {body.partition} failed on {wid} "
                               f"({body.reason}); re-enqueued")
        elif body.type == "map" and body.file_id:
            m = job["maps"][body.file_id]
            if (m["state"] == "ASSIGNED" and m["worker"] == wid
                    and m["epoch"] == body.epoch):
                m["fails"] = m.get("fails", 0) + 1
                if m["fails"] >= 3:
                    m.update(state="ERROR", worker=None)
                    log_event(job, f"map of {store.data['files'][body.file_id]['name']} "
                                   f"failed 3 times ({body.reason}) - giving up; "
                                   f"check the CSV format")
                else:
                    m.update(state="PENDING", worker=None)
                    m["epoch"] += 1
                    log_event(job, f"map of {store.data['files'][body.file_id]['name']} "
                                   f"failed on {wid} ({body.reason}); re-enqueued")
        if body.missing:
            peer = body.missing.get("peer")
            pw = store.data["workers"].get(peer)
            if pw and pw["state"] == "ALIVE":
                if _peer_alive(pw["base_url"]):
                    # peer is up but the shard is unreadable -> shard itself is gone
                    _lose_shards(job, [shard_key(body.missing["file_id"],
                                                 body.missing["partition"])],
                                 f"shard unreadable on {peer} (reported by {wid})")
                else:
                    on_worker_dead(peer, f"unreachable (reported by {wid})")
        refresh_reduces(job)
        store.save()
    return {"ok": True}


# ------------------------------------------------------------- status API

@app.get("/health")
def health():
    return {"ok": True}


@app.get("/api/jobs/{jid}/status")
def job_status(jid: str):
    with store.lock:
        job = store.data["jobs"].get(jid)
        if not job:
            raise HTTPException(404, "unknown job")
        return {"id": jid, "phase": job_phase(job), "done": job["done"],
                "maps": job["maps"], "reduces": job["reduces"],
                "publish": job.get("publish"),
                "shards_ok": sum(1 for s in job["shards"].values()
                                 if s["status"] == "OK"),
                "shards_lost": sum(1 for s in job["shards"].values()
                                   if s["status"] == "LOST"),
                "events": job["events"], "report": job["report"],
                "workers": store.data["workers"]}


@app.get("/api/jobs/{jid}/report")
def job_report(jid: str):
    with store.lock:
        job = store.data["jobs"].get(jid)
        if not job:
            raise HTTPException(404, "unknown job")
        if not job["done"]:
            pub = job.get("publish")
            reason = ("all merges are committed; the report is being published "
                      "and is shown once the write succeeds"
                      if pub else
                      "job not complete - the report is shown only after all "
                      "merges finish")
            raise HTTPException(409, reason)
        return job["report"]
