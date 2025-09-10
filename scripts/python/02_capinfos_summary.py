#!/usr/bin/env python3
"""
Summarize PCAPs with capinfos into CSV.
Skips if CSV exists.
Requires: capinfos (Wireshark)
"""
from __future__ import annotations
import argparse, csv
from pathlib import Path
from helpers import load_config, run, ensure_dir

def capinfos_dict(path: Path) -> dict:
    res = run(["capinfos", str(path)])
    info = {}
    for line in res.stdout.splitlines():
        if ":" in line:
            k, v = line.split(":", 1)
            info[k.strip()] = v.strip()
    return info

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/config.python.example.json")
    args = ap.parse_args()
    cfg = load_config(args.config)

    root = Path(cfg["dataset_root"])
    split = cfg["split"]
    out_dir = root / cfg["outputs_dir"]
    ensure_dir(out_dir)
    out_csv = out_dir / "pcap_capinfos_summary.csv"
    if out_csv.exists():
        print(f"[SKIP] {out_csv.name} exists")
        return

    rows = []
    for pcap in cfg["pcaps"]:
        p = root / split / pcap
        d = capinfos_dict(p)
        d["File"] = pcap
        rows.append(d)

    keys = sorted({k for r in rows for k in r.keys()})
    with out_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)
    print(f"[OK] wrote {out_csv}")

if __name__ == "__main__":
    main()
