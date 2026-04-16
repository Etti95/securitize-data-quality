"""
Anomaly detection over on-chain transfers.

Two complementary signals:

  z-score              by event_type, flags rows > 3 sigma above mean size
  isolation forest     multivariate on (log_tokens, hour_of_day, day_of_week)

Output is a ranked table of flagged transfers with a human-readable reason.
This is deliberately simple -- in production the same interface would front a
larger model or a streaming pipeline.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
PROC = ROOT / "data" / "processed"
PROC.mkdir(parents=True, exist_ok=True)

Z_THRESHOLD = 3.0
IF_CONTAMINATION = 0.02


def _zscore_flags(transfers: pd.DataFrame) -> pd.DataFrame:
    out = []
    for event_type, group in transfers.groupby("event_type"):
        mean = group["tokens"].mean()
        std = group["tokens"].std(ddof=0) or 1.0
        group = group.copy()
        group["z"] = (group["tokens"] - mean) / std
        flagged = group[group["z"].abs() > Z_THRESHOLD].copy()
        flagged["reason"] = flagged.apply(
            lambda r: (
                f"{r['event_type']} size {r['tokens']:,.0f} is "
                f"{r['z']:.1f}σ from mean {mean:,.0f} for this event type"
            ),
            axis=1,
        )
        flagged["signal"] = "zscore"
        out.append(flagged[["tx_hash", "block_timestamp", "event_type", "tokens", "reason", "signal"]])
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def _iforest_flags(transfers: pd.DataFrame) -> pd.DataFrame:
    df = transfers.copy()
    df["log_tokens"] = np.log1p(df["tokens"].clip(lower=0))
    ts = pd.to_datetime(df["block_timestamp"])
    df["hour"] = ts.dt.hour
    df["dow"] = ts.dt.dayofweek

    X = df[["log_tokens", "hour", "dow"]].to_numpy()
    model = IsolationForest(contamination=IF_CONTAMINATION, random_state=42)
    df["if_flag"] = model.fit_predict(X)
    df["if_score"] = model.decision_function(X)

    flagged = df[df["if_flag"] == -1].copy()
    flagged["reason"] = flagged.apply(
        lambda r: (
            f"Unusual combination: {r['event_type']} of {r['tokens']:,.0f} tokens at "
            f"{r['hour']:02d}:00 on weekday {r['dow']} (isolation score {r['if_score']:.3f})"
        ),
        axis=1,
    )
    flagged["signal"] = "isolation_forest"
    return flagged[["tx_hash", "block_timestamp", "event_type", "tokens", "reason", "signal"]]


def run() -> dict:
    transfers = pd.read_csv(RAW / "raw_token_transfers.csv")
    transfers["block_timestamp"] = pd.to_datetime(transfers["block_timestamp"])

    z = _zscore_flags(transfers)
    f = _iforest_flags(transfers)
    combined = pd.concat([z, f], ignore_index=True)

    if not combined.empty:
        combined = combined.sort_values("tokens", ascending=False).reset_index(drop=True)

    combined.to_csv(PROC / "anomaly_flags.csv", index=False)

    summary = {
        "transfers_scored": int(len(transfers)),
        "zscore_flags": int(len(z)),
        "iforest_flags": int(len(f)),
        "unique_flagged_transfers": int(combined["tx_hash"].nunique()) if not combined.empty else 0,
    }
    (PROC / "anomaly_summary.json").write_text(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
