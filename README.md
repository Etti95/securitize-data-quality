# APCRED — Data Quality & Audit Traceability Prototype

A working first version of the data quality and audit traceability layer I would build
during the first 30 days of the **Data & AI Generalist** role at Securitize.

Built against a synthetic Apollo-style tokenized private credit fund ("APCRED"), because
the real stack isn't accessible externally. Every table, rule, and output mirrors what a
transfer agent + fund admin need to reconcile for audit: on-chain transfers vs off-chain
cap table, KYC gating, NAV completeness, whitelisting, concentration, ingestion hygiene.

## What's in the box

| Layer              | What it is                                                          |
|--------------------|---------------------------------------------------------------------|
| Raw data           | 5 tables: transfers, cap table, NAV, subscriptions, KYC             |
| Validation engine  | 11 Python rules, severity-tiered, pure functions                    |
| SQL equivalents    | Same rules as warehouse-portable SQL (`sql/quality_checks.sql`)     |
| Anomaly detection  | Z-score + isolation forest on transfer-level features               |
| Dashboard          | Streamlit 3-layer: executive / diagnostic / action                  |
| Lineage            | Source → staging → marts, documented in `docs/lineage.md`           |

## Why this shape of prototype

The role owns data quality and audit traceability across a regulated financial
platform. What makes that job challenging is:

1. Knowing which checks are load-bearing for audit (SEC transfer agent obligations,
   Reg D 506(c) accreditation, fund admin NAV strike, ATS reporting).
2. Making findings *actionable* — severity, owner, remediation. Rather than dumping
   a failures table on Slack.
3. Keeping the system auditable itself: every finding traces to a rule, every rule
   traces to a source.

This prototype encodes all three.

## Running it

```bash
pip install -r requirements.txt

python scripts/generate_data.py          # generate synthetic dataset
PYTHONPATH=. python -m validation.engine # run rules, write findings + scorecard
PYTHONPATH=. python -m anomaly.detector  # score transfers for anomalies
streamlit run dashboard/app.py           # open dashboard
```

Everything except the dashboard runs in a few seconds on any laptop. Sample outputs
are committed under `data/` so the dashboard works without re-running the pipeline.

## Project layout

```
securitize-data-quality/
├── README.md
├── requirements.txt
├── scripts/
│   └── generate_data.py           # synthetic dataset generator
├── validation/
│   ├── rules.py                   # 11 data quality rules
│   └── engine.py                  # runs rules, emits findings + scorecard
├── anomaly/
│   └── detector.py                # z-score + isolation forest on transfers
├── sql/
│   └── quality_checks.sql         # warehouse-portable equivalent
├── dashboard/
│   └── app.py                     # Streamlit dashboard (exec / diag / action)
├── docs/
│   ├── business_problem.md
│   └── lineage.md                 # raw → staging → marts documentation
├── brief/
│   └── executive_brief.md         # one-pager for Compliance + Finance
└── data/
    ├── raw/                       # generated CSVs
    └── processed/                 # rule results, findings, anomalies
```

## Rule catalog

| ID        | Severity | Owner               | What it checks                                        |
|-----------|----------|---------------------|-------------------------------------------------------|
| REC-001   | critical | transfer_agent_ops  | Per-investor cap table balance vs on-chain rollup     |
| REC-002   | critical | fund_admin          | Total supply (mint − burn) vs cap table total         |
| REC-003   | high     | fund_admin          | Settled subscriptions have a matching on-chain mint   |
| KYC-001   | critical | compliance          | No transfers to wallets with expired KYC              |
| KYC-002   | high     | compliance          | US holders must have accreditation on file            |
| WHT-001   | critical | compliance          | Secondary transfers stay within whitelisted wallets   |
| BAL-001   | critical | transfer_agent_ops  | No negative cap table balances                        |
| CON-001   | high     | portfolio_ops       | No investor exceeds 10% concentration                 |
| NAV-001   | critical | fund_admin          | NAV exists for every business day                     |
| NAV-002   | high     | fund_admin          | Daily NAV move inside plausibility band (±3%)         |
| ING-001   | medium   | data_platform       | No duplicate tx_hash in transfer log                  |

## How this maps to the role

The Securitize brief lists data quality, audit traceability, AI tooling, dashboards,
and Compliance + Finance collaboration. This package produces each of those:

- **Data quality** → rules engine + SQL equivalents
- **Audit traceability** → lineage doc + severity-tiered, owner-tagged findings
- **AI tooling** → anomaly detection module with natural-language reasons
- **Dashboards** → Streamlit dashboard with exec / diag / action layers
- **Stakeholder output** → executive brief in `brief/`

## What production would look like

- Source the data from on-chain (Dune / Covalent / RPC) plus the transfer agent DB and
  fund admin drop
- Run the validation engine on a schedule (Airflow / Dagster) after nightly ingest
- Route findings to PagerDuty (critical), Linear (high), and a weekly digest (medium)
- Back the dashboard with Looker Studio on top of BigQuery marts instead of Streamlit
- Wire the anomaly detector into a streaming pipeline so flags land within minutes

The repo is shaped so any of those swaps replaces one module without touching the rest.
