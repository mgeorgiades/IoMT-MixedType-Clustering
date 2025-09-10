#!/usr/bin/env python3
"""
Shared utilities for MQTT feature extraction from PCAP/JSON.
"""

from __future__ import annotations
import json, math, os, re, string, subprocess
from pathlib import Path
from typing import Dict, Any

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def load_config(cfg_path: str | Path) -> Dict[str, Any]:
    cfg_path = Path(cfg_path)
    with cfg_path.open("r") as f:
        return json.load(f)

def entropy(s: str | None) -> float:
    if not s or not isinstance(s, str):
        return 0.0
    probs = [s.count(c)/len(s) for c in dict.fromkeys(s)]
    return -sum(p*math.log(p, 2) for p in probs if p > 0)

def topic_depth(topic: str | None) -> int:
    return len(str(topic).split("/")) if topic else 0

def has_wildcard(topic: str | None) -> bool:
    t = topic or ""
    return "+" in t or "#" in t

def non_ascii_ratio(s: str | None) -> float:
    if not s:
        return 0.0
    return sum(1 for c in s if c not in string.printable) / len(s)

_sensor_pat = re.compile(r"\d+(?:\.\d+)?")

def extract_sensor_value(payload: str | None):
    if not payload:
        return None
    m = _sensor_pat.findall(payload)
    return float(m[-1]) if m else None

def decode_hex_payload(hex_with_colons: str | None) -> str:
    """Decode tcp.payload like '30:10:00:0c:...' into ascii (best effort)."""
    if not hex_with_colons:
        return ""
    try:
        raw = bytes.fromhex(hex_with_colons.replace(":", ""))
        # Try MQTT simple remaining-length strip (optional best-effort)
        try:
            if len(raw) > 2 and raw[0] == 0x30:
                rem = raw[1]
                raw = raw[2:2+rem]
        except Exception:
            pass
        s = raw.decode("ascii", errors="ignore")
        return s.strip()
    except Exception:
        return ""

def run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=True, text=True, capture_output=True)
