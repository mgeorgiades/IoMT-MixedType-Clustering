#!/usr/bin/env python3
"""
Stream the MQTT-only JSON files (tshark) with ijson and build a feature table.
Saves a single Parquet (fast reload). Skips work if Parquet already exists.
"""
from __future__ import annotations
import argparse, time
from pathlib import Path
import pandas as pd
import ijson
from helpers import (
    load_config, ensure_dir, entropy, topic_depth, has_wildcard,
    non_ascii_ratio, extract_sensor_value, decode_hex_payload
)

def extract_row(pkt: dict, label: str):
    layers = pkt.get("_source", {}).get("layers", {})
    if "mqtt" not in layers:
        return None
    mqtt = layers["mqtt"]
    frame = layers.get("frame", {})
    ip    = layers.get("ip", {})
    tcp   = layers.get("tcp", {})

    msgtype = mqtt.get("mqtt.hdrflags_tree", {}).get("mqtt.msgtype", "")
    qos     = mqtt.get("mqtt.hdrflags_tree", {}).get("mqtt.qos", "")
    retain  = mqtt.get("mqtt.hdrflags_tree", {}).get("mqtt.retain", "")
    dup     = mqtt.get("mqtt.hdrflags_tree", {}).get("mqtt.dupflag", "")
    topic   = mqtt.get("mqtt.topic", "") or ""
    hexpl   = tcp.get("tcp.payload", "")
    msg     = decode_hex_payload(hexpl)

    return {
        "time": float(frame.get("frame.time_epoch", 0.0)),
        "length": int(frame.get("frame.len", 0) or 0),
        "ip.src": ip.get("ip.src", ""),
        "ip.dst": ip.get("ip.dst", ""),
        "mqtt.msgtype": msgtype,
        "mqtt.qos": qos,
        "mqtt.retain": retain,
        "mqtt.dup": dup,
        "mqtt.topic": topic,
        "mqtt.msg": msg,
        "topic_depth": topic_depth(topic),
        "topic_entropy": entropy(topic),
        "wildcard": has_wildcard(topic),
        "payload_entropy": entropy(msg),
        "non_ascii_ratio": non_ascii_ratio(msg),
        "sensor_value": extract_sensor_value(msg),
        "payload_length": len(msg),
        "label": label
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="configs/config.python.example.json")
    args = ap.parse_args()
    cfg = load_config(args.config)

    root = Path(cfg["dataset_root"])
    json_dir = root / cfg["json_dir"]
    out_dir = root / cfg["outputs_dir"]
    ensure_dir(out_dir)
    parquet_path = out_dir / cfg["parquet_name"]

    if parquet_path.exists():
        print(f"[SKIP] {parquet_path.name} exists")
        return

    all_rows = []
    t0 = time.time()
    for pcap in cfg["pcaps"]:
        stem = Path(pcap).stem
        label = cfg["labels"].get(stem, stem)
        json_path = json_dir / f"{stem}_mqtt.json"
        if not json_path.exists():
            raise FileNotFoundError(
                f"Missing {json_path}. Run 01_pcap_to_json.py first."
            )
        print(f"[parse] {json_path.name} -> label={label}")
        with json_path.open("r") as f:
            for pkt in ijson.items(f, "item"):
                row = extract_row(pkt, label)
                if row:
                    all_rows.append(row)

    df = pd.DataFrame(all_rows)
    # helpful dtypes
    for col in ("time","length","topic_depth","topic_entropy",
                "payload_entropy","non_ascii_ratio","sensor_value",
                "payload_length"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df.to_parquet(parquet_path, index=False)
    print(f"[OK] wrote {parquet_path} ({len(df):,} rows) in {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
