# Data Lineage

Three-tier model following the dbt / analytics-engineering convention. This
prototype is built locally but the structure mirrors what a production
BigQuery + dbt deployment would look like.

```
raw (source of record)        staging (cleaned)          marts (analytics-ready)
──────────────────────────    ─────────────────────      ───────────────────────
raw_token_transfers      →    stg_transfers         →    fct_daily_balances
raw_cap_table            →    stg_cap_table         →    fct_reconciliation
raw_nav_daily            →    stg_nav               →    dim_investors
raw_subscriptions        →    stg_subscriptions     →    fct_primary_market
raw_kyc_events           →    stg_kyc               →    dim_wallet_status
```

## Raw layer (source of record)

| Table                 | Source                             | Grain                |
|-----------------------|------------------------------------|----------------------|
| raw_token_transfers   | RPC indexer / Dune / Covalent      | one row per tx       |
| raw_cap_table         | Transfer agent system (end of day) | one row per investor |
| raw_nav_daily         | Fund admin NAV strike              | one row per day      |
| raw_subscriptions     | Investor portal + TA               | one row per request  |
| raw_kyc_events        | KYC vendor API                     | one row per investor |

## Staging layer (cleaned)

- Cast types, enforce column names, drop ingestion artifacts
- Dedupe on natural keys (tx_hash, investor_id + as_of, nav_date)
- Add partition / cluster keys

## Marts layer (analytics-ready)

- `fct_daily_balances`: per-wallet, per-day rollup of on-chain holdings
- `fct_reconciliation`: joins fct_daily_balances to stg_cap_table with delta
- `dim_investors`: one row per investor with KYC, jurisdiction, accreditation
- `fct_primary_market`: subscription → mint matching with lag
- `dim_wallet_status`: whitelisting + status (active / frozen / expired)

## Rule-to-source mapping

Every rule in `validation/rules.py` depends on a specific slice of the raw
layer. This mapping is what makes findings auditable:

| Rule    | Depends on                                               |
|---------|----------------------------------------------------------|
| REC-001 | raw_token_transfers, raw_cap_table                       |
| REC-002 | raw_token_transfers, raw_cap_table                       |
| REC-003 | raw_subscriptions, raw_token_transfers                   |
| KYC-001 | raw_token_transfers, raw_kyc_events                      |
| KYC-002 | raw_cap_table                                            |
| WHT-001 | raw_token_transfers, raw_cap_table                       |
| BAL-001 | raw_cap_table                                            |
| CON-001 | raw_cap_table                                            |
| NAV-001 | raw_nav_daily                                            |
| NAV-002 | raw_nav_daily                                            |
| ING-001 | raw_token_transfers                                      |

When a finding is raised, the auditor can trace:

    finding → rule_id → rule source tables → raw records

without ambiguity. That's the minimum bar for something the SEC examiner
would accept as a control.

## Tests (production equivalent)

The rules themselves are effectively the business-logic tests. A dbt-backed
deployment would add schema-level tests:

- `not_null` on: tx_hash, investor_id, wallet, nav_date, nav_per_token
- `unique` on: tx_hash (after dedupe), investor_id in raw_cap_table
- `accepted_values` on: event_type, kyc_status, action, jurisdiction
- `relationships` on: raw_cap_table.wallet → raw_kyc_events.wallet
