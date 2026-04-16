"""
Synthetic dataset for a tokenized private credit fund on a Securitize-style platform.

Produces five tables that mirror the real data shape a transfer agent + fund admin
would need to reconcile for audit:

  raw_token_transfers      on-chain ERC-20-style movements of fund tokens
  raw_cap_table            off-chain transfer agent record of investor holdings
  raw_nav_daily            daily NAV per token, computed by fund admin
  raw_kyc_events           KYC / accreditation status changes per investor
  raw_subscriptions        subscription + redemption requests (primary market)

Defects are planted deliberately so the validation engine has real findings to
surface. Defect types mirror incidents documented in tokenized-asset compliance
literature (cap table drift, stale KYC post-expiry, NAV gaps, non-whitelisted
recipients, concentration breaches, duplicate tx hashes, negative balances).
"""

from __future__ import annotations

import hashlib
import json
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

FUND_SYMBOL = "APCRED"
FUND_NAME = "Apollo-style Tokenized Private Credit Fund (synthetic)"
TOKEN_DECIMALS = 6
START_DATE = date(2026, 1, 6)
END_DATE = date(2026, 4, 3)
N_INVESTORS = 180

OUT_RAW = Path(__file__).resolve().parent.parent / "data" / "raw"
OUT_RAW.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _addr(seed: str) -> str:
    h = hashlib.sha1(seed.encode()).hexdigest()[:40]
    return "0x" + h


def _tx_hash(i: int) -> str:
    return "0x" + hashlib.sha256(f"tx{i}".encode()).hexdigest()


def _business_days(start: date, end: date) -> list[date]:
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


# ---------------------------------------------------------------------------
# Investor universe
# ---------------------------------------------------------------------------

@dataclass
class Investor:
    investor_id: str
    wallet: str
    jurisdiction: str
    accredited: bool
    kyc_status: str
    kyc_expires: date


def build_investors() -> list[Investor]:
    jurisdictions = ["US", "ES", "UK", "DE", "SG", "CH", "AE", "BR", "JP", "KY"]
    investors: list[Investor] = []
    for i in range(N_INVESTORS):
        inv_id = f"INV-{i+1:04d}"
        jur = random.choices(
            jurisdictions, weights=[35, 15, 10, 8, 8, 7, 5, 5, 4, 3]
        )[0]
        # US investors need accreditation
        accredited = True if jur == "US" else random.random() < 0.9
        kyc_status = random.choices(
            ["active", "expired", "pending"], weights=[92, 5, 3]
        )[0]
        kyc_expires = START_DATE + timedelta(
            days=random.randint(30, 365) if kyc_status == "active" else random.randint(-60, 20)
        )
        investors.append(
            Investor(
                investor_id=inv_id,
                wallet=_addr(inv_id),
                jurisdiction=jur,
                accredited=accredited,
                kyc_status=kyc_status,
                kyc_expires=kyc_expires,
            )
        )
    return investors


# ---------------------------------------------------------------------------
# NAV series
# ---------------------------------------------------------------------------

def build_nav(days: list[date]) -> pd.DataFrame:
    # Start at $1.0000, private credit targets ~9-10% annualized -> ~0.035%/day
    nav = 1.0000
    rows = []
    for d in days:
        drift = np.random.normal(0.00035, 0.0004)
        nav *= 1 + drift
        rows.append({"nav_date": d, "nav_per_token": round(nav, 6), "source": "fund_admin"})
    df = pd.DataFrame(rows)

    # PLANTED DEFECT: drop a mid-series NAV row (missing business day)
    drop_day = days[len(days) // 2 + 3]
    df = df[df["nav_date"] != drop_day].reset_index(drop=True)

    # PLANTED DEFECT: one NAV with an implausible jump (>5% daily)
    spike_idx = len(df) - 20
    df.loc[spike_idx, "nav_per_token"] = round(df.loc[spike_idx, "nav_per_token"] * 1.08, 6)

    return df


# ---------------------------------------------------------------------------
# Subscriptions / redemptions (primary market)
# ---------------------------------------------------------------------------

def build_subscriptions(investors: list[Investor], days: list[date]) -> pd.DataFrame:
    rows = []
    sub_id = 0
    # Primary market can only be executed by investors with active KYC at the
    # time of the request -- matches real onboarding gating on Securitize.
    for d in days:
        eligible = [
            inv for inv in investors
            if inv.kyc_status == "active" and inv.kyc_expires >= d
        ]
        if not eligible:
            continue
        n = random.randint(2, 6)
        for _ in range(n):
            inv = random.choice(eligible)
            sub_id += 1
            action = random.choices(["subscribe", "redeem"], weights=[75, 25])[0]
            tokens = round(random.uniform(5_000, 500_000), 2)
            rows.append(
                {
                    "request_id": f"REQ-{sub_id:06d}",
                    "investor_id": inv.investor_id,
                    "wallet": inv.wallet,
                    "action": action,
                    "tokens": tokens,
                    "request_date": d,
                    "settled_date": d + timedelta(days=random.choice([1, 1, 2, 3])),
                    "status": "settled",
                }
            )
    df = pd.DataFrame(rows)

    # PLANTED DEFECT: one subscription "settled" but with no on-chain mint (reconciliation fail)
    df.loc[5, "status"] = "settled"  # we'll not emit a transfer for this one (handled below)
    df.loc[5, "_unmatched_on_chain"] = True
    df["_unmatched_on_chain"] = df.get("_unmatched_on_chain", False).fillna(False)
    return df


# ---------------------------------------------------------------------------
# On-chain transfer log
# ---------------------------------------------------------------------------

TREASURY = _addr("SECURITIZE_TREASURY")
BURN_ADDR = "0x000000000000000000000000000000000000dead"


def build_transfers(
    investors: list[Investor],
    subs: pd.DataFrame,
    days: list[date],
) -> pd.DataFrame:
    rows = []
    tx = 0
    running: dict[str, float] = {inv.wallet: 0.0 for inv in investors}

    def emit(from_addr, to_addr, tokens, ts, kind):
        nonlocal tx
        tx += 1
        rows.append(
            {
                "tx_hash": _tx_hash(tx),
                "block_timestamp": ts,
                "from_address": from_addr,
                "to_address": to_addr,
                "tokens": round(tokens, 2),
                "event_type": kind,
            }
        )
        if from_addr in running:
            running[from_addr] -= tokens
        if to_addr in running:
            running[to_addr] += tokens

    primary_events = []
    for _, r in subs.iterrows():
        if r.get("_unmatched_on_chain"):
            continue
        ts = datetime.combine(r["settled_date"], datetime.min.time()) + timedelta(
            hours=random.randint(9, 17), minutes=random.randint(0, 59)
        )
        primary_events.append((ts, r))

    secondary_plan = []
    for d in days:
        n = random.randint(1, 4)
        for _ in range(n):
            ts = datetime.combine(d, datetime.min.time()) + timedelta(
                hours=random.randint(9, 17), minutes=random.randint(0, 59)
            )
            secondary_plan.append(ts)

    all_events = [("primary", ts, r) for ts, r in primary_events] + [
        ("secondary", ts, None) for ts in secondary_plan
    ]
    all_events.sort(key=lambda e: e[1])

    inv_by_wallet = {inv.wallet: inv for inv in investors}
    for kind, ts, r in all_events:
        if kind == "primary":
            if r["action"] == "subscribe":
                emit(TREASURY, r["wallet"], r["tokens"], ts, "mint")
            else:
                holder_balance = running.get(r["wallet"], 0)
                amount = min(r["tokens"], max(holder_balance, 0))
                if amount > 0:
                    emit(r["wallet"], BURN_ADDR, amount, ts, "burn")
        else:
            eligible_senders = [w for w, b in running.items() if b >= 2_000]
            if len(eligible_senders) < 2:
                continue
            # recipients must have active KYC -- in production the contract rejects otherwise
            eligible_recipients = [
                w for w in running
                if inv_by_wallet[w].kyc_status == "active"
                and inv_by_wallet[w].kyc_expires >= ts.date()
            ]
            if not eligible_recipients:
                continue
            sender_wallet = random.choice(eligible_senders)
            recipient = random.choice([w for w in eligible_recipients if w != sender_wallet])
            size = random.uniform(1_000, min(80_000, running[sender_wallet]))
            emit(sender_wallet, recipient, size, ts, "transfer")

    # PLANTED DEFECT: transfer to non-whitelisted wallet
    ts = datetime.combine(days[30], datetime.min.time()) + timedelta(hours=14)
    emit(investors[0].wallet, _addr("ROGUE_WALLET"), 42_000, ts, "transfer")

    # PLANTED DEFECT: duplicate tx_hash (ingestion double-write)
    if rows:
        dup = dict(rows[10])
        dup["block_timestamp"] = rows[10]["block_timestamp"] + timedelta(seconds=1)
        rows.append(dup)

    # PLANTED DEFECT: a whale transfer that pushes one holder over concentration limit
    whale_ts = datetime.combine(days[50], datetime.min.time()) + timedelta(hours=11)
    emit(TREASURY, investors[1].wallet, 12_500_000, whale_ts, "mint")

    # PLANTED DEFECT: post-expiry transfer into an investor whose KYC lapsed
    expired = next((i for i in investors if i.kyc_status == "expired"), None)
    if expired:
        ts = datetime.combine(days[40], datetime.min.time()) + timedelta(hours=15)
        emit(TREASURY, expired.wallet, 25_000, ts, "mint")

    df = pd.DataFrame(rows).sort_values("block_timestamp").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Cap table (off-chain transfer agent record)
# ---------------------------------------------------------------------------

def build_cap_table(investors: list[Investor], transfers: pd.DataFrame) -> pd.DataFrame:
    """
    Transfer agent's independently maintained cap table. In a perfect world
    this equals the on-chain ledger rolled up per investor. Here we inject
    drift the way a real TA system would: a manual adjustment not posted
    on-chain, plus a rounding-driven mismatch on one large holder.
    """
    balance: dict[str, float] = {inv.wallet: 0.0 for inv in investors}
    for _, r in transfers.iterrows():
        if r["from_address"] in balance:
            balance[r["from_address"]] -= r["tokens"]
        if r["to_address"] in balance:
            balance[r["to_address"]] += r["tokens"]

    rows = []
    for inv in investors:
        rows.append(
            {
                "investor_id": inv.investor_id,
                "wallet": inv.wallet,
                "jurisdiction": inv.jurisdiction,
                "accredited": inv.accredited,
                "ta_balance": round(balance[inv.wallet], 2),
                "as_of": END_DATE,
            }
        )
    df = pd.DataFrame(rows)

    # PLANTED DEFECT: off-chain manual adjustment that never hit the chain
    df.loc[3, "ta_balance"] = round(df.loc[3, "ta_balance"] + 180_000, 2)

    # PLANTED DEFECT: rounding error on a large holder
    df.loc[7, "ta_balance"] = round(df.loc[7, "ta_balance"] + 0.37, 2)

    # PLANTED DEFECT: negative balance due to miskeyed redemption
    df.loc[12, "ta_balance"] = -1_250.00

    return df


# ---------------------------------------------------------------------------
# KYC events
# ---------------------------------------------------------------------------

def build_kyc(investors: list[Investor]) -> pd.DataFrame:
    rows = []
    for inv in investors:
        rows.append(
            {
                "investor_id": inv.investor_id,
                "wallet": inv.wallet,
                "jurisdiction": inv.jurisdiction,
                "accredited": inv.accredited,
                "kyc_status": inv.kyc_status,
                "kyc_expires": inv.kyc_expires,
                "last_reviewed": START_DATE + timedelta(days=random.randint(-120, 60)),
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    days = _business_days(START_DATE, END_DATE)
    investors = build_investors()

    nav = build_nav(days)
    subs = build_subscriptions(investors, days)
    transfers = build_transfers(investors, subs, days)
    cap_table = build_cap_table(investors, transfers)
    kyc = build_kyc(investors)

    subs_out = subs.drop(columns=[c for c in subs.columns if c.startswith("_")])

    nav.to_csv(OUT_RAW / "raw_nav_daily.csv", index=False)
    subs_out.to_csv(OUT_RAW / "raw_subscriptions.csv", index=False)
    transfers.to_csv(OUT_RAW / "raw_token_transfers.csv", index=False)
    cap_table.to_csv(OUT_RAW / "raw_cap_table.csv", index=False)
    kyc.to_csv(OUT_RAW / "raw_kyc_events.csv", index=False)

    meta = {
        "fund_symbol": FUND_SYMBOL,
        "fund_name": FUND_NAME,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "row_counts": {
            "raw_nav_daily": len(nav),
            "raw_subscriptions": len(subs_out),
            "raw_token_transfers": len(transfers),
            "raw_cap_table": len(cap_table),
            "raw_kyc_events": len(kyc),
        },
        "period": {"start": str(START_DATE), "end": str(END_DATE)},
    }
    (OUT_RAW / "_metadata.json").write_text(json.dumps(meta, indent=2))
    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
