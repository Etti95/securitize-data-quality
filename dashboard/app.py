"""
Executive data quality & audit traceability dashboard.

Three-layer structure following stakeholder-first design:

  Executive Summary   data health score, open criticals, NAV trend
  Diagnostic          rule-level breakdown, findings by owner, anomalies
  Action              top interventions with suggested owner + rationale

Run: streamlit run dashboard/app.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
PROC = ROOT / "data" / "processed"

st.set_page_config(
    page_title="APCRED Data Quality",
    layout="wide",
    initial_sidebar_state="collapsed",
)

SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2}
SEVERITY_COLOR = {"critical": "#d62728", "high": "#ff7f0e", "medium": "#f2c037"}


@st.cache_data
def load():
    summary = json.loads((PROC / "run_summary.json").read_text())
    findings = pd.read_csv(PROC / "findings.csv") if (PROC / "findings.csv").exists() else pd.DataFrame()
    rules = pd.read_csv(PROC / "rule_results.csv")
    nav = pd.read_csv(RAW / "raw_nav_daily.csv")
    nav["nav_date"] = pd.to_datetime(nav["nav_date"])
    cap = pd.read_csv(RAW / "raw_cap_table.csv")
    try:
        anomalies = pd.read_csv(PROC / "anomaly_flags.csv")
    except FileNotFoundError:
        anomalies = pd.DataFrame()
    meta = json.loads((RAW / "_metadata.json").read_text())
    return summary, findings, rules, nav, cap, anomalies, meta


summary, findings, rules, nav, cap, anomalies, meta = load()

st.title("APCRED — Data Quality & Audit Traceability")
st.caption(
    f"{meta['fund_name']} · Period {meta['period']['start']} → {meta['period']['end']} · "
    f"Last run {summary['run_at']}"
)

# ---------------------------------------------------------------------------
# Executive Summary
# ---------------------------------------------------------------------------

st.subheader("Executive summary")

c1, c2, c3, c4 = st.columns(4)
health = summary["data_health_score"]
health_color = "normal" if health >= 80 else ("off" if health >= 60 else "inverse")
c1.metric("Data health score", f"{health}%", help="Share of rules passing this run")
c2.metric("Rules failed", f"{summary['rules_failed']} / {summary['rules_total']}")
c3.metric(
    "Critical findings",
    summary["findings_by_severity"].get("critical", 0),
    delta_color="inverse",
)
c4.metric(
    "Total findings",
    summary["findings_total"],
)

col_left, col_right = st.columns([2, 1])

with col_left:
    st.markdown("**NAV per token**")
    fig = px.line(nav, x="nav_date", y="nav_per_token")
    fig.update_layout(height=260, margin=dict(l=0, r=0, t=10, b=0), yaxis_title=None, xaxis_title=None)
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    st.markdown("**Findings by severity**")
    if not findings.empty:
        sev = (
            findings["severity"]
            .value_counts()
            .rename_axis("severity")
            .reset_index(name="count")
        )
        sev["order"] = sev["severity"].map(SEVERITY_ORDER)
        sev = sev.sort_values("order")
        fig2 = px.bar(
            sev,
            x="severity",
            y="count",
            color="severity",
            color_discrete_map=SEVERITY_COLOR,
        )
        fig2.update_layout(height=260, margin=dict(l=0, r=0, t=10, b=0), showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.success("No findings.")

# ---------------------------------------------------------------------------
# Diagnostic
# ---------------------------------------------------------------------------

st.subheader("Diagnostic")

tab_rules, tab_findings, tab_anomalies, tab_recon = st.tabs(
    ["Rule status", "Findings", "Transfer anomalies", "Reconciliation"]
)

with tab_rules:
    display = rules.copy()
    display["status"] = display["status"].map({"pass": "✓ pass", "fail": "✗ fail"})
    display["severity"] = display["severity"].fillna("—")
    st.dataframe(
        display[["rule_id", "rule_name", "status", "finding_count", "severity"]],
        hide_index=True,
        use_container_width=True,
    )

with tab_findings:
    if findings.empty:
        st.success("No findings in this run.")
    else:
        severities = st.multiselect(
            "Severity",
            options=["critical", "high", "medium"],
            default=["critical", "high", "medium"],
        )
        owners = st.multiselect(
            "Owner",
            options=sorted(findings["owner"].unique()),
            default=sorted(findings["owner"].unique()),
        )
        f = findings[findings["severity"].isin(severities) & findings["owner"].isin(owners)].copy()
        f["order"] = f["severity"].map(SEVERITY_ORDER)
        f = f.sort_values(["order", "rule_id"]).drop(columns=["order"])
        st.dataframe(f, hide_index=True, use_container_width=True)

with tab_anomalies:
    if anomalies.empty:
        st.info("Run `python -m anomaly.detector` to populate anomaly signals.")
    else:
        st.caption(
            f"{len(anomalies)} flagged transfer(s) across z-score and isolation forest signals."
        )
        st.dataframe(anomalies, hide_index=True, use_container_width=True)

with tab_recon:
    st.markdown(
        "Per-investor reconciliation between on-chain balance and transfer agent cap table. "
        "Any non-zero delta is investigated by Transfer Agent Ops."
    )
    # Quick on-chain roll up
    transfers = pd.read_csv(RAW / "raw_token_transfers.csv")
    incoming = transfers.groupby("to_address")["tokens"].sum()
    outgoing = transfers.groupby("from_address")["tokens"].sum()
    on_chain = incoming.sub(outgoing, fill_value=0).rename("on_chain")
    merged = cap.merge(on_chain, left_on="wallet", right_index=True, how="left").fillna(
        {"on_chain": 0}
    )
    merged["delta"] = (merged["ta_balance"] - merged["on_chain"]).round(2)
    breaches = merged[merged["delta"].abs() > 0.01].sort_values("delta", key=lambda s: s.abs(), ascending=False)
    st.metric("Investors with reconciliation delta", len(breaches))
    st.dataframe(
        breaches[["investor_id", "jurisdiction", "ta_balance", "on_chain", "delta"]],
        hide_index=True,
        use_container_width=True,
    )

# ---------------------------------------------------------------------------
# Action layer
# ---------------------------------------------------------------------------

st.subheader("Action — what to do next")

if findings.empty:
    st.success("No interventions required.")
else:
    by_rule = (
        findings.groupby(["rule_id", "severity", "owner"])
        .size()
        .reset_index(name="findings")
        .sort_values(["severity", "findings"], key=lambda s: s.map(SEVERITY_ORDER) if s.name == "severity" else s, ascending=[True, False])
    )
    remediation = {
        "REC-001": "Reconcile the on-chain vs cap table delta per investor; post any legitimate off-chain adjustments on-chain or reverse the TA entry.",
        "REC-002": "Investigate total supply drift; confirm whether mints/burns were missed or a TA adjustment is off-book.",
        "REC-003": "Match unmatched subscription against blockchain; if no mint exists, trigger settlement or reverse the TA record.",
        "KYC-001": "Freeze transfers to this wallet; compliance team to re-verify KYC before resuming activity.",
        "KYC-002": "Request accreditation documentation; hold positions until verified.",
        "WHT-001": "Immediate freeze on recipient wallet; investigate whitelisting gap.",
        "BAL-001": "Reverse the miskeyed redemption and re-run end-of-day cap table.",
        "CON-001": "Escalate to portfolio ops for concentration review and disclosure obligations.",
        "NAV-001": "Back-fill missing NAV from fund admin source-of-record before reporting.",
        "NAV-002": "Fund admin to re-validate pricing inputs for flagged day; document justification.",
        "ING-001": "Dedupe ingestion pipeline; add unique constraint on tx_hash in staging.",
    }
    by_rule["action"] = by_rule["rule_id"].map(remediation).fillna("Review and triage.")
    by_rule["severity"] = by_rule["severity"].str.upper()
    st.dataframe(
        by_rule[["severity", "rule_id", "findings", "owner", "action"]],
        hide_index=True,
        use_container_width=True,
    )

st.caption(
    "Prototype by Etiosa Richmore — built as a sample audit-traceability layer for a "
    "tokenized private credit fund. Synthetic data."
)
