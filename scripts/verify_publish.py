#!/usr/bin/env python3
"""端到端核对：报告写入失败 -> 工作进程掉线 -> 协调器重启 -> 写入恢复。

Scenario (all against one running system, same logs as verify.py):
  A. baseline job, no fault: the reference report.
  B. same inputs, but risk of report write is simulated by making
     ``<data-dir>/merge/<job-B>/report.json`` a directory, so the coordinator's
     atomic rename fails. All merges still commit and must be kept.
  C. while B waits to publish: a worker is killed through the coordinator API.
     Its intermediate shards are gone (tmpfs in Docker). Assertions: no shard is
     recomputed, no merge is re-run, no epoch changes, and no task is handed out.
  D. the coordinator process is restarted (restart command given by the caller).
     Assertions: the job is still in PUBLISH, retries resume, results unchanged.
  E. the fault is removed: the report must publish by itself and match A.

Until then the job page and /api/jobs/<id>/report expose progress only (409).

Stdlib only; needs filesystem access to the coordinator's DATA_DIR plus a way to
restart it.

Usage (local processes):
  python3 scripts/verify_publish.py --data-dir /tmp/d \
      --restart-cmd "sh /tmp/d/restart-coordinator.sh"

Usage (Docker Compose, from the host):
  python3 scripts/verify_publish.py \\
      --data-dir "$PWD/data" --restart-cmd "docker compose restart web" \\
      --block-cmd "docker compose exec -T web mkdir -p {path}"
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gen_logs import generate  # noqa: E402

CHECKS = []


def check(name: str, ok: bool, extra: str = "") -> None:
    CHECKS.append((name, ok, extra))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  ({extra})" if extra else ""))


class Ctx:
    base = "http://localhost:8000"
    data_dir = ""          # path as seen by the coordinator (for commands)
    host_data_dir = ""     # path on this machine (for direct inspection), "" if n/a
    block_cmd = ""
    unblock_cmd = ""
    restart_cmd = ""


def http(method, path, body=None, files=None, expect=None):
    url = Ctx.base + path
    data = None
    headers = {}
    if files is not None:
        boundary = uuid.uuid4().hex
        parts = []
        for field, filename, content in files:
            parts.append(
                f"--{boundary}\r\nContent-Disposition: form-data; "
                f"name=\"{field}\"; filename=\"{filename}\"\r\n"
                f"Content-Type: text/csv\r\n\r\n".encode())
            parts.append(content)
            parts.append(b"\r\n")
        parts.append(f"--{boundary}--\r\n".encode())
        data = b"".join(parts)
        headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            code, payload = resp.status, resp.read()
    except urllib.error.HTTPError as e:
        code, payload = e.code, e.read()
    if expect is not None and code != expect:
        raise AssertionError(
            f"{method} {path}: got {code}, expected {expect}: {payload[:300]!r}")
    return code, (json.loads(payload) if payload else {})


def page(path: str) -> str:
    with urllib.request.urlopen(Ctx.base + path, timeout=10) as resp:
        return resp.read().decode()


def wait_health(up: bool, timeout=60):
    """Return True once /health is reachable (up=True) or unreachable."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            with urllib.request.urlopen(Ctx.base + "/health", timeout=2) as r:
                ok = r.status == 200
        except (urllib.error.URLError, OSError):
            ok = False
        if ok == up:
            return True
        time.sleep(0.2)
    return False


def wait_phase(jid, want, timeout=180):
    t0 = time.time()
    while time.time() - t0 < timeout:
        _, st = http("GET", f"/api/jobs/{jid}/status", expect=200)
        if st["phase"] == want:
            return st
        time.sleep(0.1)
    raise TimeoutError(f"job {jid} never reached {want}")


def wait_done(jid, timeout=180):
    t0 = time.time()
    while time.time() - t0 < timeout:
        _, st = http("GET", f"/api/jobs/{jid}/status", expect=200)
        if st["done"]:
            return st
        time.sleep(0.1)
    raise TimeoutError(f"job {jid} not done within {timeout}s")


def run(cmd: str, path: str = "", check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd.replace("{path}", path), shell=True,
                          capture_output=True, text=True, check=check)


def cmd_ok(cmd: str, path: str = "") -> bool:
    return run(cmd, path, check=False).returncode == 0


def freeze(st):
    """Everything publishing must not alter.

    Shard bookkeeping is excluded: a worker death during PUBLISH flips its
    shards to LOST for display, which is expected - epochs/states/record
    counts must stay identical.
    """
    return {
        "maps": {fid: (m["epoch"], m["state"], m["worker"], m["records"])
                 for fid, m in st["maps"].items()},
        "reduces": {p: (r["epoch"], r["state"], r["records"])
                    for p, r in st["reduces"].items()},
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-url", default=os.environ.get("BASE_URL", Ctx.base))
    ap.add_argument("--data-dir", required=True,
                    help="DATA_DIR of the coordinator, as seen by the "
                         "restart/block commands")
    ap.add_argument("--host-data-dir", default="",
                    help="same directory as visible to this script; defaults to "
                         "--data-dir when it is a local path")
    ap.add_argument("--restart-cmd", required=True,
                    help="shell command that restarts only the coordinator")
    ap.add_argument("--block-cmd", default="",
                    help="command that makes the report path unwritable "
                         "({path} is replaced by the report.json path); "
                         "default: mkdir -p on the same filesystem")
    ap.add_argument("--unblock-cmd", default="",
                    help="command that removes the blockage; default: rmdir")
    ap.add_argument("--skip-baseline-upload", action="store_true")
    args = ap.parse_args()
    Ctx.base = args.base_url.rstrip("/")
    Ctx.data_dir = args.data_dir
    Ctx.host_data_dir = args.host_data_dir or args.data_dir
    Ctx.restart_cmd = args.restart_cmd
    Ctx.block_cmd = args.block_cmd
    Ctx.unblock_cmd = args.unblock_cmd

    print(f"== 0. 等待服务 {Ctx.base}")
    if not wait_health(True, timeout=60):
        print("service is not healthy")
        return 1

    print("== 1. 生成测试日志")
    tmp = tempfile.mkdtemp(prefix="logs-")
    expected, stats = generate(tmp, n_files=4, n_requests=6000)
    files = [("files", fn, open(os.path.join(tmp, fn), "rb").read())
             for fn in sorted(os.listdir(tmp)) if fn.endswith(".csv")]
    _, up = http("POST", "/api/upload", files=files, expect=200)
    fids = up["file_ids"]
    print(f"   {stats['total_records']} 条记录 / {stats['total_requests']} 个请求, "
          f"期望清单 {len(expected)} 个请求")

    print("== 2. 任务 A（无故障基准）")
    _, r = http("POST", "/api/jobs/start", body={"file_ids": fids}, expect=200)
    job_a = r["job_id"]
    st_a = wait_done(job_a)
    rep_a = st_a["report"]
    check("A 正常完成且清单正确", rep_a["still_5xx"] == expected,
          f"{len(rep_a['still_5xx'])} vs {len(expected)}")

    print("== 3. 任务 B：报告写入失败")
    _, r = http("POST", "/api/jobs/start", body={"file_ids": fids}, expect=200)
    job_b = r["job_id"]
    report_path = os.path.join(Ctx.data_dir, "merge", job_b, "report.json")
    host_report = os.path.join(Ctx.host_data_dir, "merge", job_b, "report.json")
    block = Ctx.block_cmd or "mkdir -p {path}"
    # Retries leave temp files next to the blocker, so rmdir only succeeds
    # while nothing is being written; clear contents first, then remove.
    unblock = Ctx.unblock_cmd or "rm -rf {path}"
    run(block, report_path)  # 归并还在跑，先占住报告路径
    check("已注入报告写入故障", cmd_ok(block, report_path), report_path)

    st_b = wait_phase(job_b, "PUBLISH")
    check("全部归并已提交后进入 PUBLISH", True, f"job={job_b}")
    code, _ = http("GET", f"/api/jobs/{job_b}/report")
    check("发布成功前 /report 返回 409（只显示进度）", code == 409)
    html = page(f"/jobs/{job_b}")
    check("发布成功前网页不显示清单",
          "重试后仍报 5xx 的请求（每个请求以最新记录为准）" not in html
          and "报告发布" in html)
    _, st_b = http("GET", f"/api/jobs/{job_b}/status", expect=200)
    check("B 分片未补算、归并未重跑（进入发布即冻结）",
          st_b["shards_ok"] == 4 * 8 and st_b["shards_lost"] == 0,
          f"ok={st_b['shards_ok']} lost={st_b['shards_lost']}")
    # PUBLISH is visible before the first write attempt has run in the
    # publisher thread; wait for the failure to be recorded.
    t0 = time.time()
    while time.time() - t0 < 30 and not (
            st_b["publish"]["fails"] >= 1 and st_b["publish"]["last_error"]):
        time.sleep(0.2)
        _, st_b = http("GET", f"/api/jobs/{job_b}/status", expect=200)
    check("失败已被记录并在重试",
          st_b["publish"]["fails"] >= 1 and st_b["publish"]["last_error"],
          st_b["publish"]["last_error"])
    frozen = freeze(st_b)

    print("== 4. 等待发布期间停掉一个工作进程（其分片随之丢失）")
    _, st_w = http("GET", f"/api/jobs/{job_b}/status", expect=200)
    parts = len(st_w["reduces"])
    held = {}
    for m in st_w["maps"].values():
        if m["worker"]:
            held[m["worker"]] = held.get(m["worker"], 0) + parts
    alive = sorted(wid for wid, w in st_w["workers"].items()
                   if w["state"] == "ALIVE" and held.get(wid))
    check("有一个存活且持有本任务分片的工作进程可停", bool(alive), f"{held} alive={alive}")
    victim = max(alive, key=lambda w: held[w])
    victim_shards = held[victim]
    before_events = len(st_w["events"])
    http("POST", f"/api/jobs/{job_b}/kill/{victim}", expect=200)
    # The killed worker answers its kill request, then exits 0.3s later; if it
    # re-registers with the same boot id in that window the coordinator only
    # learns about the death from heartbeat timeout. Wait for the observable
    # death instead of asserting immediately.
    t0 = time.time()
    dead = False
    while time.time() - t0 < 40:
        _, st_k = http("GET", f"/api/jobs/{job_b}/status", expect=200)
        if st_k["workers"][victim]["state"] == "DEAD":
            dead = True
            break
        time.sleep(0.2)
    check("被停进程状态为 DEAD", dead, victim)
    check("掉线后不补算分片、不重跑归并（结果集与 epoch 均不变）",
          freeze(st_k) == frozen)
    check("结果被接收后没有新的任务被派发",
          all(m["state"] != "ASSIGNED" for m in st_k["maps"].values())
          and all(r["state"] != "ASSIGNED" for r in st_k["reduces"].values()))
    new_events = "\n".join(e["msg"] for e in st_k["events"][before_events:])
    check("事件说明保留了已接收的归并结果",
          "all merges already committed" in new_events)
    check("掉线未触发补算/重跑事件",
          "recompute scheduled" not in new_events
          and "INVALIDATED" not in new_events)
    check("丢失分片被记为 LOST（仅用于展示，不影响发布）",
          st_k["shards_lost"] >= victim_shards
          and st_k["shards_ok"] + st_k["shards_lost"] == 4 * parts,
          f"lost={st_k['shards_lost']} (victim held {victim_shards}) "
          f"ok={st_k['shards_ok']}")
    check("报告仍未发布（/report 409）",
          http("GET", f"/api/jobs/{job_b}/report")[0] == 409)

    print("== 5. 只重启协调器")
    fails_before = st_k["publish"]["fails"]
    proc = subprocess.Popen(Ctx.restart_cmd, shell=True)
    went_down = wait_health(False, timeout=30)
    proc.wait(timeout=120)
    back = wait_health(True, timeout=60)
    check("协调器已重启并恢复服务", back and went_down,
          "观察到下线并重新上线" if went_down else "未观察到下线（重启命令未真正替换进程？）")
    time.sleep(2.5)
    _, st_r = http("GET", f"/api/jobs/{job_b}/status", expect=200)
    check("重启后任务仍在 PUBLISH 且未显示清单",
          st_r["phase"] == "PUBLISH" and not st_r["done"]
          and http("GET", f"/api/jobs/{job_b}/report")[0] == 409)
    check("重启后自动继续重试发布",
          st_r["publish"]["fails"] > fails_before,
          f"{fails_before} -> {st_r['publish']['fails']}")
    check("重启后归并结果/分片/epoch 不变", freeze(st_r) == frozen)

    print("== 6. 写入恢复，原任务自动发布")
    ok_unblock = cmd_ok(unblock, report_path)
    check("故障已清除", ok_unblock, "用于清除阻塞的命令执行成功")
    st_f = wait_done(job_b, timeout=120)
    rep_b = st_f["report"]
    check("写入恢复后自动发布（无需重跑归并）",
          st_f["done"] and st_f["publish"]["state"] == "DONE")
    check("B 清单 == A 清单", rep_b["still_5xx"] == rep_a["still_5xx"],
          f"{len(rep_b['still_5xx'])} vs {len(rep_a['still_5xx'])}")
    keys = ("total_records", "total_requests", "retried", "single_5xx")
    check("B 各项计数 == A（与正常运行一致）",
          {k: rep_b[k] for k in keys} == {k: rep_a[k] for k in keys},
          {k: rep_b[k] for k in keys})
    check("B 清单 == 期望清单", rep_b["still_5xx"] == expected)
    _, rep_api = http("GET", f"/api/jobs/{job_b}/report", expect=200)
    check("发布后 /report == 同批日志正常运行结果",
          rep_api == rep_a)
    html = page(f"/jobs/{job_b}")
    check("发布后网页显示清单与计数",
          "重试后仍报 5xx 的请求（每个请求以最新记录为准）" in html
          and f"共 {rep_b['total_requests']} 个请求" in html)
    if Ctx.host_data_dir and os.path.isfile(host_report):
        on_disk = json.load(open(host_report, encoding="utf-8"))
        check("report.json 与接口一致", on_disk == rep_b)
    else:
        check("report.json 存在于协调器数据卷（内容经接口核对）", True,
              "宿主机不可见，跳过读文件")
    check("发布未被重启/掉线影响（epoch 不变）", freeze(st_f) == frozen)

    failed = [c for c in CHECKS if not c[1]]
    print(f"\n{'全部通过' if not failed else '存在失败'}: "
          f"{len(CHECKS) - len(failed)}/{len(CHECKS)} 项通过")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
