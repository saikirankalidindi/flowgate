"""Command line: `flowgate serve` runs the gateway, `flowgate mock` runs the mock upstream."""

from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import tempfile


def _install_uvloop() -> None:
    try:
        import uvloop

        uvloop.install()
    except ImportError:
        pass


def _run_gateway(config_path: str, host: str | None, port: int | None, reuse_port: bool) -> None:
    from aiohttp import web

    from flowgate.app import create_app
    from flowgate.config import load_config

    _install_uvloop()
    cfg = load_config(config_path)
    web.run_app(create_app(cfg), host=host or cfg.host, port=port or cfg.port,
                reuse_port=reuse_port, access_log=None, print=None)


def _run_mock(args: argparse.Namespace, reuse_port: bool) -> None:
    from aiohttp import web

    from flowgate.mock.server import MockSettings, create_mock_app

    _install_uvloop()
    s = MockSettings(ttft_ms=args.ttft_ms, tpot_ms=args.tpot_ms, median_tokens=args.median_tokens,
                     sigma=args.sigma, chunk_tokens=args.chunk_tokens, error_rate=args.error_rate,
                     error_status=args.error_status, seed=args.seed)
    web.run_app(create_mock_app(s), host=args.host, port=args.port, reuse_port=reuse_port,
                access_log=None, print=None)


def _fan_out(n: int, target, *args) -> None:
    """Run n worker processes sharing one port via SO_REUSEPORT."""
    if n <= 1:
        target(*args, False)
        return
    procs = [mp.Process(target=target, args=(*args, True), daemon=True) for _ in range(n)]
    for p in procs:
        p.start()
    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        for p in procs:
            p.terminate()


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="flowgate")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="run the gateway")
    s.add_argument("-c", "--config", required=True)
    s.add_argument("--host")
    s.add_argument("--port", type=int)
    s.add_argument("--workers", type=int, default=1, help="processes sharing the port")

    m = sub.add_parser("mock", help="run the mock LLM upstream")
    m.add_argument("--host", default="127.0.0.1")
    m.add_argument("--port", type=int, default=9001)
    m.add_argument("--workers", type=int, default=1)
    m.add_argument("--ttft-ms", type=float, default=50.0)
    m.add_argument("--tpot-ms", type=float, default=5.0)
    m.add_argument("--median-tokens", type=float, default=150.0)
    m.add_argument("--sigma", type=float, default=0.8)
    m.add_argument("--chunk-tokens", type=int, default=1)
    m.add_argument("--error-rate", type=float, default=0.0)
    m.add_argument("--error-status", type=int, default=503)
    m.add_argument("--seed", type=int)

    args = ap.parse_args(argv)
    if args.cmd == "serve":
        if args.workers > 1 and not os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
            os.environ["PROMETHEUS_MULTIPROC_DIR"] = tempfile.mkdtemp(prefix="flowgate-prom-")
        _fan_out(args.workers, _run_gateway, args.config, args.host, args.port)
    else:
        _fan_out(args.workers, _run_mock, args)


if __name__ == "__main__":
    main()
