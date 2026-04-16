# Outreach (English)

## Target: hiring manager / Head of Data / Compliance lead at Securitize

### Version A — LinkedIn / email (short)

Subject: Built a first version of an audit-traceability layer for a tokenized fund

Hi {{name}},

I saw the Data & AI Generalist opening and took the brief literally — built a
first version of the data quality and audit traceability layer I'd own in the
first 30 days.

It's a working prototype against a synthetic Apollo-style tokenized private
credit fund: 11 rules covering cap table ↔ on-chain reconciliation, KYC gating,
whitelisting, concentration, NAV completeness and plausibility, plus a small
anomaly layer on transfers. Rules are authored twice — Python engine and
warehouse-portable SQL — because the role context-switches between both.

Key findings from one run, on synthetic data with planted defects:

- 3 investors with cap-table-to-on-chain drift (largest ~$180k)
- 2 transfers into an expired-KYC wallet that the contract didn't reject
- 1 secondary transfer to a non-whitelisted wallet
- 1 concentration breach at 26% of outstanding tokens

The repo, executive brief, and dashboard are here: {{link}}.

I'd love 20 minutes to walk through how I'd adapt this to your actual stack
and what I'd build in weeks 1–4. Does next week work?

— Etiosa

---

### Version B — longer email (for a more detailed read)

Subject: Audit-ready data quality prototype for APCRED — 20 min?

Hi {{name}},

Rather than send a résumé, I built a working first version of the data
quality and audit traceability system I think the Data & AI Generalist role
is really about. It's shaped around a synthetic Apollo-style tokenized
private credit fund because that's the closest public analog I can build
against without touching your real warehouse.

What's in it:

1. Synthetic but realistic data for 5 tables: on-chain transfers, TA cap
   table, daily NAV, subscriptions/redemptions, KYC events. Defects are
   planted deliberately (off-chain-only TA adjustment, expired-KYC mint,
   non-whitelisted recipient, NAV gap, duplicate tx_hash, concentration
   breach) so the validation layer has real work to do.
2. An 11-rule validation engine in Python, severity-tiered (critical / high
   / medium) with owner routing and remediation text. Same rules expressed
   as warehouse-portable SQL for a BigQuery / dbt deployment.
3. A Streamlit dashboard with three layers: executive summary (data health
   score, critical count, NAV trend), diagnostic (rule status, per-owner
   findings, reconciliation view, anomalies), and action (what to fix,
   who owns it, when).
4. A light anomaly detector (z-score + isolation forest) on transfer-level
   features, with natural-language reasons for each flag.
5. Documentation: business framing, lineage (raw → staging → marts),
   executive brief for Compliance + Finance.

Why I built it this way:

The role isn't a pure analyst or engineer gig — it's the operator seat for
the data that underpins every transfer agent, fund admin, and ATS
obligation Securitize has. Writing the queries is the easy part. Harder is
making findings *actionable* (severity, owner, remediation) and *auditable*
(every finding traces back to a rule, every rule to a source). That's what
I optimized for.

What I'd do in my first 30 days on the real stack:

- Port these rules to your warehouse and schedule them after nightly ingest
- Wire finding routing (PagerDuty for critical, tickets for high, digest
  for medium) and a monthly audit export
- Meet Compliance and Finance to catch the rules I didn't think of — there
  will be several, because every fund's waterfall and every asset class
  has quirks
- Start the AI layer (anomaly detection + an internal chatbot over the
  findings + lineage) on top of the now-trustworthy data

Repo, executive brief, dashboard: {{link}}

Happy to walk through any of it — would next week work for a 20-minute call?

— Etiosa
