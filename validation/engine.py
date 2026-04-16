"""
Validation engine: load raw tables, run all rules, emit findings + scorecard.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from validation.rules import RULES

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
PROC = ROOT / "data" / "processed"
PROC.mkdir(parents=True, exist_ok=True)

RAW_TABLES = [
    "raw_nav_daily",
    "raw_subscriptions",
    "raw_token_transfers",
    "raw_cap_table",
    "raw_kyc_events",
]


def load_tables() -> dict[str, pd.DataFrame]:
    tables: dict[str, pd.DataFrame] = {}
    for name in RAW_TABLES:
        path = RAW / f"{name}.csv"
        df = pd.read_csv(path)
        for col in ("nav_date", "request_date", "settled_date", "kyc_expires", "last_reviewed", "as_of"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.date
        if "block_timestamp" in df.columns:
            df["block_timestamp"] = pd.to_datetime(df["block_timestamp"])
        tables[name] = df
    return tables


def run() -> dict:
    tables = load_tables()

    all_findings: list[dict] = []
    rule_results: list[dict] = []

    for label, fn in RULES:
        findings = fn(tables)
        rule_id = label.split(" ")[0]
        rule_results.append(
            {
                "rule_id": rule_id,
                "rule_name": label,
                "status": "pass" if not findings else "fail",
                "finding_count": len(findings),
                "severity": findings[0].severity if findings else None,
            }
        )
        all_findings.extend([f.asdict() for f in findings])

    findings_df = pd.DataFrame(all_findings)
    rules_df = pd.DataFrame(rule_results)

    findings_df.to_csv(PROC / "findings.csv", index=False)
    rules_df.to_csv(PROC / "rule_results.csv", index=False)

    by_severity = {}
    if not findings_df.empty:
        by_severity = findings_df["severity"].value_counts().to_dict()

    rules_passed = int((rules_df["status"] == "pass").sum())
    rules_total = int(len(rules_df))
    data_health_score = round(100 * rules_passed / rules_total, 1) if rules_total else 0.0

    summary = {
        "run_at": datetime.utcnow().isoformat() + "Z",
        "rules_total": rules_total,
        "rules_passed": rules_passed,
        "rules_failed": rules_total - rules_passed,
        "data_health_score": data_health_score,
        "findings_total": int(len(findings_df)),
        "findings_by_severity": by_severity,
    }
    (PROC / "run_summary.json").write_text(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    import sys

    sys.path.insert(0, str(ROOT))
    print(json.dumps(run(), indent=2))
