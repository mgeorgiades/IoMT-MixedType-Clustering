#!/usr/bin/env python3
"""
One-shot builder: PCAP -> JSON (tshark) -> Parquet (ijson).
Re-runs steps only if outputs are missing.
"""
from __future__ import annotations
import argparse, importlib

def run_module(modname: str, config: str):
    m = importlib.import_module(modname)
    # emulate `python module.py --config ...`
    m.main.__call__(**{}) if hasattr(m, "main") else None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/config.python.example.json")
    args = ap.parse_args()

    # we pass config via environment variable to keep it simple
    import os
    os.environ["IOTMIX_CFG"] = args.config

    # Step 1: PCAP -> JSON
    import scripts.python._run01 as _r01  # shim to pass arg
    _r01.run(args.config)

    # Step 2: capinfos summary (optional but nice)
    import scripts.python._run02 as _r02
    _r02.run(args.config)

    # Step 3: JSON -> Parquet
    import scripts.python._run03 as _r03
    _r03.run(args.config)

if __name__ == "__main__":
    main()
