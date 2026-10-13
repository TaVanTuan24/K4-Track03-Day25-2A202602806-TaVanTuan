"""Robustness / generalisation probes for Lab 25B.

Mirrors what the instructor does when grading: re-run the public tapes with
perturbed timing and intensity, use many seeds, and re-generate the SLI dataset
with different seeds and a shifted incident schedule. Nothing here modifies the
simulator, the scenarios on disk or the tests — perturbed tapes are written to
``reports/_probe/`` and deleted-safe.

    python scripts/robustness.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))  # run without pip install

import copy
import random
from typing import Any

import yaml

from gameday_lab.factory import build_client
from gameday_lab.sim.config import load_config
from gameday_lab.sim.engine import evaluate_checks, run_scenario
from gameday_lab.slo.alerting import BurnRateAlerter
from gameday_lab.slo.data import Incident, generate
from gameday_lab.slo.scoring import score_alerts

ROOT = Path(__file__).resolve().parents[1]
PROBE = ROOT / "reports" / "_probe"
SEEDS = [25, 26, 27, 1, 2, 3, 7, 11, 13, 42, 99, 123, 2024, 2025, 7, 8]


def perturbed_tapes() -> dict[str, dict[str, Any]]:
    """G1-G6 with shifted incident windows and re-scaled intensities.

    Every timing is moved (the grader's tapes are "lệch thời điểm, đổi cường độ"),
    so a client that hard-coded any incident time or bound would fail here.
    """
    raw = yaml.safe_load((ROOT / "configs" / "scenarios.yaml").read_text(encoding="utf-8"))
    out: dict[str, dict[str, Any]] = {}
    shifts = (0, 17, -23, 40, 61)
    scales = (1.0, 1.25, 0.8)
    for key, scen in raw["scenarios"].items():
        for si, shift in enumerate(shifts):
            for scale in scales:
                s = copy.deepcopy(scen)
                for eff in s.get("effects", []):
                    eff["start"] = round(eff["start"] + shift, 3)
                    eff["end"] = round(eff["end"] + shift, 3)
                    if "error_rate" in eff:
                        eff["error_rate"] = round(min(0.95, eff["error_rate"] * scale), 4)
                    if "capacity_factor" in eff and scale != 1.0:
                        eff["capacity_factor"] = round(min(1.0, eff["capacity_factor"] / scale), 4)
                    if "rate_limit_rps" in eff and scale != 1.0:
                        eff["rate_limit_rps"] = max(1, round(eff["rate_limit_rps"] / scale))
                for win in s.get("traffic", []):
                    win["start"] = round(win["start"] + shift, 3)
                    win["end"] = round(win["end"] + shift, 3)
                    win["multiplier"] = round(win["multiplier"] * scale, 3)
                for win in s.get("malformed", []):
                    win["start"] = round(win["start"] + shift, 3)
                    win["end"] = round(win["end"] + shift, 3)
                for win in s.get("primary_only", []):
                    win["start"] = round(win["start"] + shift, 3)
                    win["end"] = round(win["end"] + shift, 3)
                if s.get("restore_at") is not None:
                    s["restore_at"] = round(s["restore_at"] + shift, 3)
                s["duration_s"] = round(s["duration_s"] + shift + 20, 3)
                out[f"{key}__s{si}_{scale}"] = s
    return out


def perturbed_cache_tapes() -> dict[str, dict[str, Any]]:
    """C1 with the webhook / silent change / prompt deploy / outage moved.

    The silent change is the dangerous one: it is deliberately moved *off* the
    TTL lattice, because the public C1 places it exactly 60 s after the prompt
    deploy, which would flatter a 60 s TTL.
    """
    raw = yaml.safe_load((ROOT / "configs" / "scenarios_cache.yaml").read_text(encoding="utf-8"))
    base = raw["scenarios"]["C1_faq_outage"]
    out: dict[str, dict[str, Any]] = {}
    for i, (webhook, outage_lo, outage_hi, deploy, silent) in enumerate([
        (150, 240, 360, 420, 480),
        (132, 210, 330, 405, 447),
        (171, 268, 388, 439, 553),
        (118, 190, 310, 512, 601),
        (204, 300, 420, 445, 470),
    ]):
        s = copy.deepcopy(base)
        s["content_updates"] = [{"t": webhook, "source": "policy/refund"}]
        s["effects"] = [{"start": outage_lo, "end": outage_hi, "outage": "refuse"}]
        s["prompt_deploys"] = [{"t": deploy, "version": "v2"}]
        s["silent_updates"] = [{"t": silent, "source": "policy/shipping"}]
        s["restore_at"] = outage_hi
        s["duration_s"] = max(deploy, silent) + 240
        out[f"C1_perturbed_{i}"] = s
    return out


def run_resilient_probes() -> tuple[int, int]:
    scen = perturbed_tapes()
    PROBE.mkdir(parents=True, exist_ok=True)
    path = PROBE / "scenarios_probe.yaml"
    path.write_text(yaml.safe_dump({"scenarios": scen}, sort_keys=False), encoding="utf-8")
    cfg = load_config(str(ROOT / "configs" / "gameday.yaml"), str(path))
    ok = total = 0
    failures: dict[str, list[str]] = {}
    for key, sc in cfg.scenarios.items():
        for seed in SEEDS[:6]:
            total += 1
            m = run_scenario(cfg, key, build_client("resilient", cfg), seed).metrics()
            checks = evaluate_checks(m, sc.checks)
            if all(checks.values()):
                ok += 1
            else:
                failures.setdefault(key.split("__")[0], []).append(
                    f"{key}@{seed}: {[k for k, v in checks.items() if not v]}")
    print(f"G1-G6 perturbed tapes: {ok}/{total} runs PASS")
    for g, bad in sorted(failures.items()):
        print(f"  {g}: {len(bad)} failing runs, e.g. {bad[:2]}")
    return ok, total


def run_cache_probes() -> tuple[int, int]:
    scen = perturbed_cache_tapes()
    PROBE.mkdir(parents=True, exist_ok=True)
    path = PROBE / "scenarios_cache_probe.yaml"
    path.write_text(yaml.safe_dump({"scenarios": scen}, sort_keys=False), encoding="utf-8")
    cfg = load_config(str(ROOT / "configs" / "gameday.yaml"), str(path))
    ok = total = 0
    for key, sc in cfg.scenarios.items():
        for seed in SEEDS[:6]:
            total += 1
            m = run_scenario(cfg, key, build_client("cached", cfg), seed).metrics()
            checks = evaluate_checks(m, sc.checks)
            if all(checks.values()):
                ok += 1
            else:
                print(f"  FAIL {key}@{seed}: {[k for k, v in checks.items() if not v]} "
                      f"wrong={m['wrong_answers']} outdated={m['outdated_answers']} "
                      f"hit={m['cache_hit_ratio']} faq={m['faq_availability']}")
    print(f"C1 perturbed tapes: {ok}/{total} runs PASS")
    return ok, total


def run_traffic_probes() -> None:
    """Vary the offered load itself (the one axis the other perturbations miss).

    Reported per multiplier rather than as a single pass/fail, because a tape's
    checks are calibrated to the traffic it was written for: at 2x the offered
    rate the tape is no longer the same experiment, so a failure there is an
    "assumption exceeded" result, not automatically a code defect. What must hold
    at *every* level is the safety set — no retry-after violation, no retry of a
    client error, no tool call routed to a model without tool support.
    """
    from dataclasses import replace

    cfg = load_config(str(ROOT / "configs" / "gameday.yaml"), str(ROOT / "configs" / "scenarios.yaml"))
    safety = ("retry_after_violations", "client_error_retries", "incompatible_routes")
    for mult in (0.5, 1.0, 2.0):
        tuned = replace(cfg, workload=replace(cfg.workload, turns_per_s=cfg.workload.turns_per_s * mult))
        checked = passed = 0
        bad_safety: list[str] = []
        fails: dict[str, list[str]] = {}
        for key, sc in tuned.scenarios.items():
            m = run_scenario(tuned, key, build_client("resilient", tuned), 25).metrics()
            checks = evaluate_checks(m, sc.checks)
            checked += 1
            if all(checks.values()):
                passed += 1
            else:
                fails[key] = [k for k, v in checks.items() if not v]
            bad_safety += [f"{key}:{f}={m[f]}" for f in safety if m.get(f)]
        verdict = "OK" if not bad_safety else "SAFETY BROKEN " + ",".join(bad_safety)
        print(f"  traffic x{mult:<4} {passed}/{checked} tapes PASS   safety: {verdict}")
        for k, v in fails.items():
            print(f"      {k}: {v}")


def run_slo_probes() -> tuple[int, int]:
    """Hidden-SLO proxy: new seeds and a shifted/renamed incident schedule."""
    from dataclasses import replace

    from gameday_lab.slo.data import MIN_PER_DAY, default_incidents

    base = default_incidents()
    schedules: list[list[Incident]] = []
    for shift in (0, 137, -211, 401, MIN_PER_DAY // 3):
        for scale in (1.0, 1.3):
            incs = []
            for inc in base:
                incs.append(replace(
                    inc,
                    start_min=max(0, inc.start_min + shift),
                    duration_min=max(1, int(inc.duration_min * scale)),
                    error_ratio=(min(1.0, inc.error_ratio * scale) if inc.expect == "none" else inc.error_ratio),
                ))
            schedules.append(incs)
    ok = total = 0
    for si, incs in enumerate(schedules):
        for seed in (11, 2025, 777, 31337):
            total += 1
            tot, bad, real = generate(seed, incs)
            events = BurnRateAlerter().evaluate(tot, bad)
            score = score_alerts(events, real)
            if score["pass"]:
                ok += 1
            else:
                rows = [r for r in score["incidents"] if not r.get("detected", True) or r.get("paged")
                        or r.get("paged_instead")]
                print(f"  FAIL schedule{si} seed{seed}: false_pages={score['false_pages']} rows={rows}")
    print(f"hidden-like SLO datasets: {ok}/{total} PASS")
    return ok, total


def main() -> None:
    random.seed(0)
    print("== robustness probes ==")
    run_resilient_probes()
    run_cache_probes()
    run_slo_probes()
    print("offered-load sweep (G1-G6 at seed 25):")
    run_traffic_probes()
    # the perturbed tapes are scratch: they are regenerated identically each run
    if PROBE.exists():
        for f in PROBE.iterdir():
            f.unlink()
        PROBE.rmdir()


if __name__ == "__main__":
    main()
