"""Render README charts from bench/results/*.json into docs/img/.

    uv run python bench/plot.py
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RES, IMG = ROOT / "bench" / "results", ROOT / "docs" / "img"

SURFACE, INK, INK2, MUTED, GRID, AXIS = "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
LABELS = {
    "posthoc": "count after the fact (typical gateway)",
    "reserve_max_tokens": "reserve max_tokens",
    "reserve_p95_allow": "reserve p95, allow overrun",
    "reserve_p95_truncate": "reserve p95, truncate (Hardcap default)",
}

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": AXIS, "axes.labelcolor": INK2, "xtick.color": MUTED, "ytick.color": MUTED,
    "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "axes.spines.top": False, "axes.spines.right": False, "font.size": 10,
    "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlelocation": "left",
})


def _load(name: str) -> dict | None:
    p = RES / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else None


def overshoot() -> None:
    d = _load("overshoot")
    if not d:
        return
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.2))
    modes = [m for m in LABELS if any(s["mode"] == m for s in d["summary"])]
    for ax, metric, title, ylabel in (
        (axes[0], "overshoot_max_pct", "Worst-case budget overshoot", "% over the token budget (max tenant)"),
        (axes[1], "utilization_mean_pct", "Budget actually usable", "% of budget served before 429"),
    ):
        for i, m in enumerate(modes):
            rows = sorted((s for s in d["summary"] if s["mode"] == m), key=lambda s: s["concurrency"])
            xs, ys = [r["concurrency"] for r in rows], [r[metric] for r in rows]
            ax.plot(xs, ys, color=SERIES[i], lw=2, marker="o", ms=5,
                    markeredgecolor=SURFACE, markeredgewidth=1.5, label=LABELS[m], zorder=3)
            if metric == "overshoot_max_pct" and m in ("posthoc", "reserve_p95_truncate"):
                label = f"{ys[-1]:+.2f}%" if abs(ys[-1]) < 1 else f"{ys[-1]:+.0f}%"
                ax.annotate(label, (xs[-1], ys[-1]), xytext=(6, 0),
                            textcoords="offset points", va="center", fontsize=9, color=INK2)
        ax.set_xscale("log", base=2)
        ax.set_xticks(rows and [r["concurrency"] for r in rows])
        ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
        ax.set_xlabel("concurrent streams per tenant")
        ax.set_ylabel(ylabel)
        ax.set_title(title)
    axes[0].axhline(0, color=AXIS, lw=1, zorder=2)
    axes[1].set_ylim(80, 101)
    p = d["params"]
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=2, frameon=False, fontsize=9,
               bbox_to_anchor=(0.5, 0.04))
    fig.text(0.01, 0.005, f"{p['replicas']} replicas · {p['tenants_per_run']} tenants × "
             f"{len(p['seeds'])} runs · {p['limit_tokens']:,}-token budget · max_tokens "
             f"{p['max_tokens']} · log-normal output lengths", fontsize=8, color=MUTED)
    fig.tight_layout(rect=(0, 0.14, 1, 1))
    fig.savefig(IMG / "overshoot.png", dpi=160)
    plt.close(fig)


def overhead() -> None:
    d = _load("overhead")
    if not d:
        return
    fig, ax = plt.subplots(figsize=(8, 3.8))
    rows = [r for r in d["results"] if not r["stream"]]
    rates = sorted({r["target_rps"] for r in rows})
    width = 0.2
    for j, (pct, shade) in enumerate((("p50", 0), ("p99", 1))):
        for k, target in enumerate(("direct", "gateway")):
            ys = [next(r for r in rows if r["target"] == target and r["target_rps"] == rate)
                  ["latency_ms"][pct] for rate in rates]
            xs = [i + (j * 2 + k - 1.5) * (width + 0.02) for i in range(len(rates))]
            color = SERIES[k]
            bars = ax.bar(xs, ys, width, color=color, alpha=1.0 if shade else 0.55,
                          label=f"{target} {pct}", zorder=3)
            ax.bar_label(bars, fmt="%.1f", fontsize=8, color=INK2, padding=2)
    ax.set_xticks(range(len(rates)), [f"{int(r):,} req/s" for r in rates])
    ax.set_yscale("log")
    ax.set_ylabel("latency (ms, log scale)")
    ax.set_title("Latency: direct to upstream vs. through Hardcap (one laptop)")
    ax.legend(frameon=False, fontsize=9, ncol=4, loc="upper left")
    ax.set_ylim(0.2, ax.get_ylim()[1] * 4)
    fig.tight_layout()
    fig.savefig(IMG / "overhead.png", dpi=160)
    plt.close(fig)


def failover() -> None:
    d = _load("failover")
    if not d:
        return
    fig, axes = plt.subplots(1, len(d["scenarios"]), figsize=(11, 3.8), sharey=True)
    for ax, sc in zip(axes, d["scenarios"]):
        tl = sc["timeline"]
        t = [b["t"] for b in tl]
        rate = 1 / 0.5
        ax.stackplot(t, [b["via_primary"] * rate for b in tl], [b["via_secondary"] * rate for b in tl],
                     [b["failed"] * rate for b in tl], colors=[SERIES[0], SERIES[2], "#e34948"],
                     labels=["served by primary", "served by secondary", "failed"],
                     step="post", edgecolor=SURFACE, linewidth=0.5, zorder=3)
        for x, lab in ((sc["fault_at_s"], "fault"), (sc["recover_at_s"], "recovered")):
            ax.axvline(x, color=INK2, lw=1, ls="--", zorder=4)
            ax.text(x + 0.15, ax.get_ylim()[1] * 0.02 + 5, lab, fontsize=8, color=INK2)
        name = "primary SIGKILLed" if sc["scenario"] == "kill" else "primary returns 503"
        ax.set_title(f"{name}: {sc['failed']} of {sc['requests']} failed")
        ax.set_xlabel("seconds")
    axes[0].set_ylabel("requests/s")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=3, frameon=False, fontsize=9)
    fig.tight_layout(rect=(0, 0.07, 1, 1))
    fig.savefig(IMG / "failover.png", dpi=160)
    plt.close(fig)


if __name__ == "__main__":
    IMG.mkdir(parents=True, exist_ok=True)
    overshoot()
    overhead()
    failover()
    print("charts in", IMG)
