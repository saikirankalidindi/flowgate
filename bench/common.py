"""Shared benchmark plumbing: start mock upstreams and gateway replicas as real processes,
wait for health, and tear everything down."""

from __future__ import annotations

import contextlib
import json
import os
import platform
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"
REDIS_URL = os.environ.get("FLOWGATE_BENCH_REDIS", "redis://localhost:6379/1")
PY = sys.executable


def wait_healthy(url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as r:
                if r.status == 200:
                    return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError(f"{url} not healthy after {timeout}s")


class Proc:
    def __init__(self, args: list[str], health: str, name: str):
        self.args, self.health, self.name = args, health, name
        self.p: subprocess.Popen | None = None

    def start(self) -> Proc:
        log = open(RESULTS / "raw" / f"{self.name}.log", "w")
        self.p = subprocess.Popen(self.args, stdout=log, stderr=subprocess.STDOUT,
                                  start_new_session=True, cwd=ROOT)
        wait_healthy(self.health)
        return self

    def kill(self, sig: int = signal.SIGKILL) -> None:
        if self.p and self.p.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(self.p.pid, sig)
            self.p.wait(timeout=10)

    def stop(self) -> None:
        self.kill(signal.SIGTERM)


def mock(port: int, *, workers: int = 1, **settings) -> Proc:
    args = [PY, "-m", "flowgate", "mock", "--port", str(port), "--workers", str(workers)]
    for k, v in settings.items():
        args += [f"--{k.replace('_', '-')}", str(v)]
    return Proc(args, f"http://127.0.0.1:{port}/healthz", f"mock-{port}")


def gateway(cfg: dict, port: int, *, workers: int = 1) -> Proc:
    f = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, dir=RESULTS / "raw")
    yaml.safe_dump(cfg, f)
    f.close()
    env_args = [PY, "-m", "flowgate", "serve", "-c", f.name, "--port", str(port),
                "--host", "127.0.0.1", "--workers", str(workers)]
    return Proc(env_args, f"http://127.0.0.1:{port}/healthz", f"gateway-{port}")


def http_json(url: str, data: dict | None = None) -> dict:
    req = urllib.request.Request(url, data=json.dumps(data).encode() if data is not None else None,
                                 headers={"content-type": "application/json"},
                                 method="POST" if data is not None else "GET")
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def machine() -> dict:
    cpu = platform.processor() or ""
    with contextlib.suppress(Exception):
        for line in open("/proc/cpuinfo"):
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    return {"cpu": cpu, "cores": os.cpu_count(), "python": platform.python_version(),
            "os": f"{platform.system()} {platform.release()}"}


def save(name: str, data: dict) -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    path = RESULTS / f"{name}.json"
    path.write_text(json.dumps({"machine": machine(), **data}, indent=2))
    return path


(RESULTS / "raw").mkdir(parents=True, exist_ok=True)
