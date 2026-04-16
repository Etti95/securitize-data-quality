"""
Data quality rules for a tokenized private credit fund.

Each rule is a pure function taking a dict of DataFrames and returning a
DataFrame of findings. Severity tiers map to compliance escalation logic:

  critical  - blocks NAV strike / investor statements / regulatory filing
  high      - must resolve before next reporting cycle
  medium    - monitor and remediate

Rules encode things a transfer agent + fund admin actually watch: cap table
vs on-chain reconciliation, KYC gating, NAV completeness and plausibility,
whitelisting, concentration, subscription settlement, and ingestion hygiene.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Callable

import pandas as pd

TREASURY = "0x" + __import__("hashlib").sha1(b"SECURITIZE_TREASURY").hexdigest()[:40]
BURN = "0x000000000000000000000000000000000000dead"

# NAV movement above this in a single business day is flagged as implausible
NAV_DAILY_MOVE_THRESHOLD = 0.03

# Concentration limit for a single investor (% of outstanding tokens)
CONCENTRATION_LIMIT = 0.10

# Allowed reconciliation drift per investor (in tokens) before flagging
RECON_TOKEN_TOLERANCE = 0.01


@dataclass
class Finding:
    rule_id: str
    severity: str
    entity_type: str
    entity_id: str
    message: str
    owner: str

    def asdict(self):
        return self.__dict__


RuleFn = Callable[[dict[str, pd.DataFrame]], list[Finding]]


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

def rule_onchain_vs_cap_table(tables: dict[str, pd.DataFrame]) -> list[Finding]:
    """CRITICAL: transfer agent balance must reconcile with on-chain holdings."""
    transfers = tables["raw_token_transfers"]
    cap = tables["raw_cap_table"]

    incoming = transfers.groupby("to_address")["tokens"].sum()
    outgoing = transfers.groupby("from_address")["tokens"].sum()
    on_chain = incoming.sub(outgoing, fill_value=0).rename("on_chain_balance")

    merged = cap.merge(on_chain, left_on="wallet", right_index=True, how="left").fillna(
        {"on_chain_balance": 0}
    )
    merged["delta"] = merged["ta_balance"] - merged["on_chain_balance"]
    breaches = merged[merged["delta"].abs() > RECON_TOKEN_TOLERANCE]

    findings: list[Finding] = []
    for _, r in breaches.iterrows():
        findings.append(
            Finding(
                rule_id="REC-001",
                severity="critical",
                entity_type="investor",
                entity_id=r["investor_id"],
                message=(
                    f"Cap table balance {r['ta_balance']:,.2f} != on-chain balance "
                    f"{r['on_chain_balance']:,.2f} (delta {r['delta']:,.2f})"
                ),
                owner="transfer_agent_ops",
            )
        )
    return findings


def rule_total_supply_reconciles(tables: dict[str, pd.DataFrame]) -> list[Finding]:
    """CRITICAL: sum of cap table = total outstanding tokens (mints - burns)."""
    transfers = tables["raw_token_transfers"]
    cap = tables["raw_cap_table"]

    minted = transfers.loc[transfers["event_type"] == "mint", "tokens"].sum()
    burned = transfers.loc[transfers["event_type"] == "burn", "tokens"].sum()
    outstanding = minted - burned
    ta_total = cap["ta_balance"].sum()

    if abs(outstanding - ta_total) > 1.0:
        return [
            Finding(
                rule_id="REC-002",
                severity="critical",
                entity_type="fund",
                entity_id="APCRED",
                message=(
                    f"Total outstanding {outstanding:,.2f} != cap table total "
                    f"{ta_total:,.2f} (delta {outstanding - ta_total:,.2f})"
                ),
                owner="fund_admin",
            )
        ]
    return []


def rule_subscription_matches_mint(tables: dict[str, pd.DataFrame]) -> list[Finding]:
    """HIGH: every settled subscription must have a corresponding on-chain mint."""
    subs = tables["raw_subscriptions"]
    transfers = tables["raw_token_transfers"]

    settled_subs = subs[(subs["status"] == "settled") & (subs["action"] == "subscribe")].copy()
    mints = transfers[transfers["event_type"] == "mint"].copy()
    mints["block_date"] = pd.to_datetime(mints["block_timestamp"]).dt.date

    findings: list[Finding] = []
    for _, r in settled_subs.iterrows():
        window = mints[
            (mints["to_address"] == r["wallet"])
            & (mints["block_date"] >= r["request_date"])
            & (mints["block_date"] <= r["settled_date"] + timedelta(days=1))
            & (mints["tokens"].round(2) == round(r["tokens"], 2))
        ]
        if window.empty:
            findings.append(
                Finding(
                    rule_id="REC-003",
                    severity="high",
                    entity_type="subscription",
                    entity_id=r["request_id"],
                    message=(
                        f"Settled subscription {r['request_id']} for {r['tokens']:,.2f} tokens "
                        f"has no matching on-chain mint to {r['wallet'][:10]}..."
                    ),
                    owner="fund_admin",
                )
            )
    return findings


# ---------------------------------------------------------------------------
# KYC / regulatory
# ---------------------------------------------------------------------------

def rule_transfer_to_expired_kyc(tables: dict[str, pd.DataFrame]) -> list[Finding]:
    """CRITICAL: tokens cannot be received by a wallet whose KYC is expired at tx time."""
    transfers = tables["raw_token_transfers"]
    kyc = tables["raw_kyc_events"]

    transfers = transfers.copy()
    transfers["block_date"] = pd.to_datetime(transfers["block_timestamp"]).dt.date

    merged = transfers.merge(
        kyc[["wallet", "investor_id", "kyc_status", "kyc_expires"]],
        left_on="to_address",
        right_on="wallet",
        how="inner",
    )
    breaches = merged[
        (merged["kyc_status"] == "expired")
        | (pd.to_datetime(merged["kyc_expires"]).dt.date < merged["block_date"])
    ]

    findings: list[Finding] = []
    for _, r in breaches.iterrows():
        findings.append(
            Finding(
                rule_id="KYC-001",
                severity="critical",
                entity_type="transfer",
                entity_id=r["tx_hash"],
                message=(
                    f"Transfer to investor {r['investor_id']} on {r['block_date']} "
                    f"but KYC expired {r['kyc_expires']}"
                ),
                owner="compliance",
            )
        )
    return findings


def rule_us_holders_must_be_accredited(tables: dict[str, pd.DataFrame]) -> list[Finding]:
    """HIGH: US-domiciled holders must be accredited (Reg D 506(c))."""
    cap = tables["raw_cap_table"]
    breaches = cap[(cap["jurisdiction"] == "US") & (~cap["accredited"]) & (cap["ta_balance"] > 0)]
    return [
        Finding(
            rule_id="KYC-002",
            severity="high",
            entity_type="investor",
            entity_id=r["investor_id"],
            message=(
                f"US investor {r['investor_id']} holds {r['ta_balance']:,.2f} tokens "
                "without accreditation on file"
            ),
            owner="compliance",
        )
        for _, r in breaches.iterrows()
    ]


# ---------------------------------------------------------------------------
# Whitelisting / wallet hygiene
# ---------------------------------------------------------------------------

def rule_transfers_to_non_whitelisted(tables: dict[str, pd.DataFrame]) -> list[Finding]:
    """CRITICAL: secondary transfers must stay within the whitelisted wallet set."""
    transfers = tables["raw_token_transfers"]
    cap = tables["raw_cap_table"]

    whitelist = set(cap["wallet"]) | {TREASURY, BURN}
    breaches = transfers[
        (~transfers["to_address"].isin(whitelist)) & (transfers["event_type"] == "transfer")
    ]
    return [
        Finding(
            rule_id="WHT-001",
            severity="critical",
            entity_type="transfer",
            entity_id=r["tx_hash"],
            message=(
                f"Transfer {r['tx_hash'][:12]}... sent {r['tokens']:,.2f} tokens to "
                f"non-whitelisted wallet {r['to_address'][:12]}..."
            ),
            owner="compliance",
        )
        for _, r in breaches.iterrows()
    ]


# ---------------------------------------------------------------------------
# Balance + concentration
# ---------------------------------------------------------------------------

def rule_no_negative_balances(tables: dict[str, pd.DataFrame]) -> list[Finding]:
    """CRITICAL: no investor can hold a negative balance on the cap table."""
    cap = tables["raw_cap_table"]
    breaches = cap[cap["ta_balance"] < 0]
    return [
        Finding(
            rule_id="BAL-001",
            severity="critical",
            entity_type="investor",
            entity_id=r["investor_id"],
            message=f"Investor {r['investor_id']} has negative cap table balance {r['ta_balance']:,.2f}",
            owner="transfer_agent_ops",
        )
        for _, r in breaches.iterrows()
    ]


def rule_concentration_limit(tables: dict[str, pd.DataFrame]) -> list[Finding]:
    """HIGH: no single investor should exceed the concentration limit."""
    cap = tables["raw_cap_table"]
    total = cap["ta_balance"].clip(lower=0).sum()
    if total == 0:
        return []
    cap = cap.copy()
    cap["pct"] = cap["ta_balance"].clip(lower=0) / total
    breaches = cap[cap["pct"] > CONCENTRATION_LIMIT]
    return [
        Finding(
            rule_id="CON-001",
            severity="high",
            entity_type="investor",
            entity_id=r["investor_id"],
            message=(
                f"Investor {r['investor_id']} holds {r['pct']:.1%} of outstanding tokens "
                f"(limit {CONCENTRATION_LIMIT:.0%})"
            ),
            owner="portfolio_ops",
        )
        for _, r in breaches.iterrows()
    ]


# ---------------------------------------------------------------------------
# NAV completeness + plausibility
# ---------------------------------------------------------------------------

def rule_nav_business_day_completeness(tables: dict[str, pd.DataFrame]) -> list[Finding]:
    """CRITICAL: a NAV must exist for every business day in the period."""
    nav = tables["raw_nav_daily"].copy()
    nav["nav_date"] = pd.to_datetime(nav["nav_date"])
    start, end = nav["nav_date"].min(), nav["nav_date"].max()
    expected = pd.bdate_range(start, end)
    missing = expected.difference(nav["nav_date"])
    return [
        Finding(
            rule_id="NAV-001",
            severity="critical",
            entity_type="nav",
            entity_id=str(d.date()),
            message=f"No NAV recorded for business day {d.date()}",
            owner="fund_admin",
        )
        for d in missing
    ]


def rule_nav_daily_move_plausible(tables: dict[str, pd.DataFrame]) -> list[Finding]:
    """HIGH: daily NAV move above threshold requires review (private credit is stable)."""
    nav = tables["raw_nav_daily"].copy().sort_values("nav_date")
    nav["pct"] = nav["nav_per_token"].pct_change()
    breaches = nav[nav["pct"].abs() > NAV_DAILY_MOVE_THRESHOLD]
    return [
        Finding(
            rule_id="NAV-002",
            severity="high",
            entity_type="nav",
            entity_id=str(r["nav_date"]),
            message=(
                f"NAV move of {r['pct']:.2%} on {r['nav_date']} exceeds "
                f"{NAV_DAILY_MOVE_THRESHOLD:.0%} plausibility threshold"
            ),
            owner="fund_admin",
        )
        for _, r in breaches.iterrows()
    ]


# ---------------------------------------------------------------------------
# Ingestion hygiene
# ---------------------------------------------------------------------------

def rule_duplicate_tx_hash(tables: dict[str, pd.DataFrame]) -> list[Finding]:
    """MEDIUM: the same tx_hash should not appear twice (pipeline double-write)."""
    transfers = tables["raw_token_transfers"]
    dups = transfers[transfers.duplicated("tx_hash", keep=False)]
    seen: set[str] = set()
    findings: list[Finding] = []
    for _, r in dups.iterrows():
        if r["tx_hash"] in seen:
            continue
        seen.add(r["tx_hash"])
        findings.append(
            Finding(
                rule_id="ING-001",
                severity="medium",
                entity_type="transfer",
                entity_id=r["tx_hash"],
                message=f"Duplicate tx_hash {r['tx_hash'][:12]}... present in transfer log",
                owner="data_platform",
            )
        )
    return findings


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

RULES: list[tuple[str, RuleFn]] = [
    ("REC-001 On-chain vs cap table reconciliation", rule_onchain_vs_cap_table),
    ("REC-002 Total supply reconciliation", rule_total_supply_reconciles),
    ("REC-003 Subscription-to-mint match", rule_subscription_matches_mint),
    ("KYC-001 Transfers to expired KYC", rule_transfer_to_expired_kyc),
    ("KYC-002 US holders accreditation", rule_us_holders_must_be_accredited),
    ("WHT-001 Transfers to non-whitelisted wallets", rule_transfers_to_non_whitelisted),
    ("BAL-001 No negative balances", rule_no_negative_balances),
    ("CON-001 Concentration limit", rule_concentration_limit),
    ("NAV-001 NAV business day completeness", rule_nav_business_day_completeness),
    ("NAV-002 NAV daily move plausibility", rule_nav_daily_move_plausible),
    ("ING-001 Duplicate tx_hash", rule_duplicate_tx_hash),
]
