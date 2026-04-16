# APCRED Data Quality — Run Brief

**Audience:** Head of Compliance, Head of Finance, Fund Administration
**Prepared by:** Etiosa Richmore
**Period:** 2026-01-06 → 2026-04-03 (Q1, 63 business days)
**Classification:** Prototype on synthetic data

## Bottom line

A single validation run against the quarter's APCRED data surfaced **13 findings
across 9 of 11 rules**, including 9 critical items that would block a NAV strike
or investor statement if left unresolved. Two are structural (cap table drift,
total supply drift); the rest are point incidents with identifiable owners.

| Metric                    | Value        |
|---------------------------|--------------|
| Data health score         | 18.2%        |
| Rules failed              | 9 of 11      |
| Critical findings         | 9            |
| High findings             | 3            |
| Medium findings           | 1            |
| Unique anomalous transfers| 8            |

## What's happening

1. **Cap table ≠ on-chain reconciliation (REC-001, 3 investors).** The transfer
   agent book shows positions the blockchain doesn't back. Largest delta is
   USD ~180,000 on a single investor. Two others are small drift (rounding and
   a miskeyed redemption).
2. **Total supply drift (REC-002).** Outstanding tokens on-chain are off from
   cap table total by ~90k tokens. Consistent with the single-investor drift
   above — one adjustment posted on the TA ledger never made it on-chain.
3. **KYC gating failure (KYC-001, 2 transfers).** Mints landed in a wallet
   whose KYC expired before the transfer. The smart contract did not reject.
   Compliance freeze recommended pending re-verification.
4. **Whitelisting breach (WHT-001, 1 transfer).** 42,000 tokens sent to a
   wallet not on the investor whitelist. Recipient wallet needs immediate
   freeze and investigation of how the whitelist gap occurred.
5. **Concentration (CON-001, 1 investor).** One holder crossed 10% of
   outstanding tokens after a large primary market event. Portfolio Ops
   should confirm disclosure obligations.
6. **NAV completeness + plausibility (NAV-001, NAV-002).** One business day
   has no NAV recorded; one day shows a ~8% move that falls outside the
   private-credit plausibility band and needs fund admin justification.
7. **Ingestion hygiene (ING-001).** One duplicate tx_hash indicates a
   pipeline double-write — low severity but worth a unique constraint in staging.

## What we should do

| Action                                                    | Owner                | Window |
|-----------------------------------------------------------|----------------------|--------|
| Reconcile INV-0004 cap table delta; post or reverse       | Transfer Agent Ops   | 2 days |
| Back-fill missing NAV for 2026-02-24                      | Fund Administration  | 1 day  |
| Freeze KYC-expired wallet, re-verify                      | Compliance           | Today  |
| Investigate whitelist gap on WHT-001 recipient            | Compliance + Data    | Today  |
| Review concentration breach for disclosure                | Portfolio Ops        | 1 week |
| Add unique constraint on tx_hash in staging               | Data Platform        | 1 week |
| Document NAV 2026-03-09 pricing input                     | Fund Administration  | 2 days |

## What production looks like

- This same rule set runs nightly after the ingest batch finishes and posts to
  Compliance + Finance channels before anyone looks at the dashboard in the morning.
- Critical findings page on-call; high findings open tickets; medium findings roll
  into a weekly digest.
- A monthly audit pack exports findings + remediation log to meet transfer agent
  recordkeeping obligations.
- The anomaly detector feeds a review queue that Compliance clears daily, not a
  standalone alert.

## What is in / out of scope for this prototype

**In scope:** the reconciliation and compliance rules that would be load-bearing
for a real audit; an end-to-end validation + dashboard loop; a representative
anomaly signal; documentation and outreach.

**Out of scope (deliberately):** live on-chain data, secure warehouse credentials,
ATS-side trade reporting rules, corporate action processing, FX for multi-currency
vehicles, and distribution accounting. All of those slot into the existing shape
without refactoring.
