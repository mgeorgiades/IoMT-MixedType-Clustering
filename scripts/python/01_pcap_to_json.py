#!/usr/bin/env python3
"""
Convert each PCAP to MQTT-only JSON using tshark.
Skips files that already exist.
Requires: tshark (Wireshark CLI)
"""
from __future__ import annotations
import argparse
from pathlib import Path
from helpers import load_config, ensure_dir, run

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/config.python.example.json")
    args = ap.parse_args()
    cfg = load_config(args.config)

    root = Path(cfg["dataset_root"])
    split = cfg["split"]
    json_dir = root / cfg["json_dir"]
    ensure_dir(json_dir)

    for pcap in cfg["pcaps"]:
        in_pcap = root / split / pcap
        out_json = json_dir / (Path(pcap).stem + "_mqtt.json")
        if out_json.exists():
            print(f"[SKIP] {out_json.name} exists")
            continue
        print(f"[tshark] {pcap} -> {out_json.name}")
        # Example: tshark -r file.pcap -Y mqtt -T json -n > out.json
        cmd = ["tshark", "-r", str(in_pcap), *cfg.get("tshark_args", [])]
        res = run(cmd)
        out_json.write_text(res.stdout)
        print(f"[OK] wrote {out_json}")

if __name__ == "__main__":
    main()
