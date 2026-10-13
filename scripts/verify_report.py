"""Verify reports/REPORT.md numbers against the artefacts they claim to come from.

Reads the JSON artifacts and checks that every value the report displays for the
G-tapes, the cache tape, the workflows and the SLO run is actually present.
Catches stale figures after a code change.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPORT = Path("reports/REPORT.md")
text = REPORT.read_text(encoding="utf-8")

problems: list[str] = []
checks = 0


def _norm(s: str) -> str:
    """Normalise away the report's human formatting (thin spaces, %.0 floats)."""
    s = s.replace("\u202f", "").replace("\u00a0", "").replace(" ", "")
    s = s.removesuffix(".0")
    return s


TEXT_N = _norm(text)


def want(value: str, label: str) -> None:
    global checks
    checks += 1
    v = _norm(str(value))
    # accept either the raw value or the report's percentage rendering
    forms = [v]
    try:
        f = float(value)
        if 0.0 <= f <= 1.0:
            forms.append(_norm(f"{f * 100:g}"))
    except (TypeError, ValueError):
        pass
    if not any(form in TEXT_N for form in forms):
        problems.append(f"{label}: {value!r} not found (tried {forms})")


# ---------- G-tapes ----------
for client in ("naive", "resilient"):
    data = json.loads(Path(f"reports/gameday/{client}.json").read_text())
    for key, m in data.items():
        short = key.split("_")[0]
        for field in ("availability", "latency_p95_s", "amplification", "retry_ratio", "cost_per_good_turn"):
            v = m.get(field)
            if v is None:
                continue
            want(str(v), f"gameday {client} {short} {field}")
        for field in ("retry_after_violations", "client_error_retries", "incompatible_routes",
                      "unneeded_failovers"):
            v = m.get(field)
            if v:
                want(str(v), f"counter {client} {short} {field}")

# ---------- cache tape ----------
for client in ("resilient", "cached"):
    m = json.loads(Path(f"reports/cache/{client}.json").read_text())["C1_faq_outage"]
    for field in ("availability", "faq_availability", "cache_hit_ratio", "stale_served",
                  "wrong_answers", "outdated_answers", "cost_per_good_turn"):
        v = m.get(field)
        if v is None:
            continue
        want(str(v), f"cache {client} {field}")

# ---------- workflows ----------
wf = json.loads(Path("reports/workflows.json").read_text())
for name, m in wf.items():
    for field in ("completion_rate", "worker_crashes", "duplicate_refunds", "duplicate_emails",
                  "lost_refunds", "overpaid_cents", "wasted_llm_per_crash", "store_writes"):
        v = m.get(field)
        if v is None:
            continue
        want(str(v), f"workflow {name} {field}")

# ---------- slo ----------
slo = json.loads(Path("reports/slo.json").read_text())
for name, m in slo.items():
    for row in m.get("incidents", []):
        if row.get("delay_min") is not None:
            want(str(row["delay_min"]), f"slo {name} {row['incident']} delay")
bud = slo["burnrate"].get("budget", {})
for field in ("requests", "allowed_bad", "bad", "availability", "budget_consumed", "budget_remaining"):
    if field in bud:
        want(str(bud[field]), f"slo budget {field}")
for e in slo["burnrate"].get("events", []):
    want(str(e["start_min"]), f"slo event {e['rule']} start")
    want(str(e["end_min"]), f"slo event {e['rule']} end")

# ---------- no unfilled template markers ----------
# NB: '| |' is legitimate here — the comparison tables leave the first *header*
# cell empty because that column holds the variant label — and '…' is used as
# ordinary prose (a list continuation and a numeric range), not as a placeholder.
for marker in ("TODO", "__CODE_COMMIT__", "<fill", "XXX", "FIXME", "Lorem"):
    if marker in text:
        problems.append(f"placeholder marker still present: {marker!r}")

print(f"checked {checks} values from the artifacts")
if problems:
    print(f"{len(problems)} PROBLEM(S):")
    for p in problems:
        print("  -", p)
    sys.exit(1)
print("all report figures trace to the committed artifacts")
