#!/usr/bin/env python3
"""Generate a small synthetic OHLC dataset for tv demo/testing.

Stdlib-only random walk so the repo stays clean of real market data
(which may carry redistribution restrictions). 60 trading days, single
ticker, daily bar. Format matches the Stooq / Yahoo "Date, Open, High,
Low, Close, Volume" convention so users can swap in real files without
friction.
"""
import csv
import datetime as dt
import random

random.seed(7)  # reproducible

def gen(n=60, start_price=100.0, start_date=dt.date(2024, 1, 2)):
    price = start_price
    day = start_date
    rows = []
    while len(rows) < n:
        if day.weekday() < 5:  # Mon–Fri only
            # intraday drift
            op = price * (1 + random.uniform(-0.005, 0.005))
            cl = op * (1 + random.uniform(-0.02, 0.02))
            hi = max(op, cl) * (1 + random.uniform(0, 0.01))
            lo = min(op, cl) * (1 - random.uniform(0, 0.01))
            vol = int(1_000_000 + random.uniform(-200_000, 500_000))
            rows.append({
                "Date":   day.isoformat(),
                "Open":   f"{op:.2f}",
                "High":   f"{hi:.2f}",
                "Low":    f"{lo:.2f}",
                "Close":  f"{cl:.2f}",
                "Volume": str(vol),
            })
            price = cl
        day += dt.timedelta(days=1)
    return rows


def write(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["Date", "Open", "High", "Low", "Close", "Volume"])
        w.writeheader()
        w.writerows(rows)

if __name__ == "__main__":
    import os, sys
    path = sys.argv[1] if len(sys.argv) > 1 else "data/finance/sample_ohlc.csv"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    write(path, gen())
    print(f"wrote {path}")
