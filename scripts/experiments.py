"""Extra experiment harness for the report (Lab 25B).

Nothing here is part of the simulator or the grader: it loads the *same*
``configs/gameday.yaml`` and overrides only the ``client:`` / ``cache:`` blocks
in memory, then prints the simulator's real metrics. Used to produce the
before/after tables in ``reports/REPORT.md``.

    python scripts/experiments.py                  # all experiments
    python scripts/experiments.py --only timeout   # one experiment
    python scripts/experiments.py --json out.json  # dump every row
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))  # run without pip install

import argparse
import copy
import json
from typing import Any

import yaml

from gameday_lab.factory import build_client
from gameday_lab.sim.config import load_config
from gameday_lab.sim.contracts import BaseClient
from gameday_lab.sim.engine import evaluate_checks, run_scenario

ROOT = Path(__file__).resolve().parents[1]
SHOW = ("availability", "latency_p95_s", "amplification", "retry_ratio", "cost_per_good_turn",
        "recovery_s", "primary_share_after_restore", "retry_after_violations",
        "client_error_retries", "incompatible_routes", "unneeded_failovers")
CACHE_SHOW = ("availability", "faq_availability", "cache_hit_ratio", "stale_served",
              "wrong_answers", "outdated_answers", "cost_per_good_turn")


def deep_merge(base: dict[str, Any], over: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_raw(name: str = "gameday.yaml") -> dict[str, Any]:
    return yaml.safe_load((ROOT / "configs" / name).read_text(encoding="utf-8"))


def make_config(client_over: dict[str, Any] | None = None, cache_over: dict[str, Any] | None = None,
                scenarios: str = "configs/scenarios.yaml"):
    """Rewrite configs/gameday.yaml in a temp file with overrides, then load it."""
    raw = load_raw()
    if client_over:
        raw["client"] = deep_merge(raw.get("client", {}), client_over)
    if cache_over:
        raw["cache"] = deep_merge(raw.get("cache", {}), cache_over)
    tmp = ROOT / "reports" / "_experiment_gameday.yaml"
    tmp.parent.mkdir(parents=True, exist_ok=True)
    tmp.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    return load_config(tmp, scenarios)


def run_tape(cfg, key: str, client: str = "resilient", seed: int | None = 25) -> dict[str, Any]:
    sim = run_scenario(cfg, key, build_client(client, cfg), seed)
    m = sim.metrics()
    m["checks"] = evaluate_checks(m, cfg.scenarios[key].checks)
    m["pass"] = all(m["checks"].values())
    return m


def row(label: str, key: str, m: dict[str, Any], cols=SHOW) -> dict[str, Any]:
    d = {"label": label, "tape": key, "pass": m["pass"]}
    d.update({c: m.get(c) for c in cols})
    failed = [k for k, v in m.get("checks", {}).items() if not v]
    if failed:
        d["failed_checks"] = failed
    if m.get("wrong_answers"):
        d["wrong_answers_by_reason"] = m.get("wrong_answers_by_reason")
    if m.get("give_up_reasons"):
        d["give_up_reasons"] = m.get("give_up_reasons")
    return d


def show(rows: list[dict[str, Any]]) -> None:
    cols = ["label", "tape", "pass", "availability", "latency_p95_s", "amplification", "retry_ratio",
            "cost_per_good_turn", "recovery_s", "primary_share_after_restore", "retry_after_violations",
            "client_error_retries", "incompatible_routes", "unneeded_failovers", "faq_availability",
            "cache_hit_ratio", "wrong_answers", "outdated_answers", "stale_served"]
    present = [c for c in cols if any(c in r for r in rows)]
    widths = {c: max(len(c), *(len(_fmt(r.get(c))) for r in rows)) for c in present}
    print("  ".join(c.ljust(widths[c]) for c in present))
    for r in rows:
        print("  ".join(_fmt(r.get(c)).ljust(widths[c]) for c in present))
        if r.get("failed_checks"):
            print(f"      ^ failed: {', '.join(r['failed_checks'])}")
        if r.get("wrong_answers_by_reason"):
            print(f"      ^ wrong_answers_by_reason: {r['wrong_answers_by_reason']}")
        if r.get("give_up_reasons"):
            print(f"      ^ give_up_reasons: {r['give_up_reasons']}")


def _fmt(v: Any) -> str:
    if v is None:
        return "-"
    if isinstance(v, bool):
        return "PASS" if v else "FAIL"
    if isinstance(v, float):
        return f"{v:g}"
    return str(v)


# --------------------------------------------------------------------------- experiments
def exp_reference(seeds: list[int]) -> list[dict[str, Any]]:
    cfg = make_config()
    rows = []
    for c in ("naive", "resilient"):
        for seed in seeds:
            for key in cfg.scenarios:
                rows.append(row(f"{c}@{seed}", key, run_tape(cfg, key, c, seed)))
    return rows


def exp_retry_budget(seeds: list[int]) -> list[dict[str, Any]]:
    """Experiment 1: a mistuned breaker, with and without the retry budget."""
    bad_breaker = {"breaker": {"min_calls": 1000000000}, "max_attempts_per_provider": 5}
    on = deep_merge(bad_breaker, {"retry_budget_ratio": 0.1, "retry_budget_min_per_s": 0.5,
                                  "retry_budget_max_tokens": 20})
    off = deep_merge(bad_breaker, {"retry_budget_ratio": 100.0, "retry_budget_min_per_s": 1.0e6,
                                   "retry_budget_max_tokens": 1.0e9})
    rows = []
    for label, over in (("budget ON", on), ("budget OFF", off)):
        cfg = make_config(over)
        for seed in seeds:
            for key in ("G2_overload_storm", "G3_brownout"):
                rows.append(row(f"{label}@{seed}", key, run_tape(cfg, key, "resilient", seed)))
    return rows


def exp_timeout(seeds: list[int]) -> list[dict[str, Any]]:
    """Experiment 2: attempt_timeout_s 8 (shipped) vs 30."""
    rows = []
    for label, over in (("timeout 8s", {"attempt_timeout_s": 8.0}), ("timeout 30s", {"attempt_timeout_s": 30.0})):
        cfg = make_config(over)
        for seed in seeds:
            for key in ("G6_provider_outage", "G4_slow_region"):
                rows.append(row(f"{label}@{seed}", key, run_tape(cfg, key, "resilient", seed)))
    return rows


def exp_capability(seeds: list[int]) -> list[dict[str, Any]]:
    """Experiment 3: ladder() with the tool-calling filter removed.

    Only the ladder is changed; everything else is the shipped ResilientClient.
    """
    import gameday_lab.client as client_mod

    class NoCapabilityFilter(client_mod.ResilientClient):
        name = "resilient-no-cap-filter"

        def ladder(self, task: str) -> list[str]:
            return list(self.order)

    cfg = make_config()
    rows = []
    for label, factory in (("capability filter ON", None), ("filter REMOVED", NoCapabilityFilter)):
        for seed in seeds:
            if factory is None:
                m = run_tape(cfg, "G6_provider_outage", "resilient", seed)
            else:
                m = run_with(cfg, "G6_provider_outage", factory, seed)
            rows.append(row(f"{label}@{seed}", "G6_provider_outage", m))
    return rows


def run_with(cfg: Any, key: str, cls: type, seed: int | None) -> dict[str, Any]:
    """Run one tape with a client class that takes the ResilientClient constructor."""
    from gameday_lab.sim.engine import run_scenario as rs

    def build(*_args: Any) -> BaseClient:
        return cls(cfg.provider_infos(), cfg.fallback_order, cfg.client, seed=seed or 1)

    sim = rs(cfg, key, build(), seed)
    m = sim.metrics()
    m["checks"] = evaluate_checks(m, cfg.scenarios[key].checks)
    m["pass"] = all(m["checks"].values())
    return m


CACHE_CFG = {"scenarios": "configs/scenarios_cache.yaml"}


def exp_cache(seeds: list[int]) -> list[dict[str, Any]]:
    """Part G sensitivity: no cache vs threshold / TTL / stale-if-error variants.

    Every variant starts from the shipped baseline and changes exactly one knob,
    so a difference is attributable to that knob.
    """
    base = load_raw()["cache"]
    variants: list[tuple[str, dict[str, Any], str]] = [("resilient (no cache)", {}, "resilient")]
    variants += [
        ("threshold 0.80", {"similarity_threshold": 0.80}, "cached"),
        (f"threshold {base['similarity_threshold']} (shipped)", {}, "cached"),
        ("threshold 0.97", {"similarity_threshold": 0.97}, "cached"),
        ("ttl 30s", {"ttl_s": 30}, "cached"),
        ("ttl 60s", {"ttl_s": 60}, "cached"),
        ("ttl 300s", {"ttl_s": 300}, "cached"),
        ("stale-if-error OFF (max_stale_s=0)", {"max_stale_s": 0}, "cached"),
    ]
    rows = []
    for label, over, client in variants:
        cfg = make_config(cache_over=over, scenarios="configs/scenarios_cache.yaml")
        for seed in seeds:
            m = run_tape(cfg, "C1_faq_outage", client, seed)
            rows.append(row(f"{label}@{seed}", "C1_faq_outage", m, CACHE_SHOW))
    return rows


def exp_cache_baseline(seeds: list[int]) -> list[dict[str, Any]]:
    """Same sweep, always keeping the shipped block as the base for one-knob changes."""
    return exp_cache(seeds)


def shipped_cache_summary(seeds: list[int]) -> list[dict[str, Any]]:
    """The cache block exactly as shipped in configs/gameday.yaml."""
    raw = load_raw()
    shipped = raw["cache"]
    print(f"  shipped cache block: {shipped}")
    base = load_raw()["cache"]
    cfg = make_config(cache_over={k: v for k, v in shipped.items() if base.get(k) != v},
                      scenarios="configs/scenarios_cache.yaml")
    rows = []
    for client in ("resilient", "cached"):
        for seed in seeds:
            m = run_tape(cfg, "C1_faq_outage", client, seed)
            rows.append(row(f"shipped {client}@{seed}", "C1_faq_outage", m, CACHE_SHOW))
    return rows


EXPERIMENTS = {
    "reference": exp_reference,
    "retry_budget": exp_retry_budget,
    "timeout": exp_timeout,
    "capability": exp_capability,
    "cache": exp_cache,
    "shipped_cache": shipped_cache_summary,
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="all")
    ap.add_argument("--seeds", default="25")
    ap.add_argument("--json", default=str(ROOT / "reports" / "experiments.json"))
    args = ap.parse_args()
    seeds = [int(s) for s in args.seeds.split(",")]
    names = list(EXPERIMENTS) if args.only == "all" else args.only.split(",")
    out: dict[str, Any] = {}
    for name in names:
        print(f"\n== {name} (seeds {args.seeds})")
        rows = EXPERIMENTS[name](seeds)
        out[name] = rows
        show(rows)
    Path(args.json).write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    # the override config is scratch: it is regenerated on every run
    (ROOT / "reports" / "_experiment_gameday.yaml").unlink(missing_ok=True)
    print(f"\nwrote {args.json}")


if __name__ == "__main__":
    main()
