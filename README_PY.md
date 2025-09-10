# Python pipeline (PCAP → JSON → Parquet)

Create a single `mqtt_features.parquet` from raw PCAPs by filtering MQTT packets with `tshark` and streaming JSON into features.  
All steps **skip** work if the target file already exists (no re-compute).

---

## 1) Prerequisites

- **OS:** Ubuntu (tested on 23.04+)
- **System tools:** Wireshark CLI

    sudo apt-get update
    sudo apt-get install -y tshark wireshark-common

- **Python:** 3.11+

    pip install -r requirements-python.txt

---

## 2) Configure

Copy and edit the example (adjust `dataset_root`, `split`, and file names if needed):

    cp configs/config.python.example.json configs/config.python.json

**Key fields:**
- `dataset_root`: base path containing `train/` and/or `test/` PCAP folders
- `pcaps`: list of PCAP filenames under `${dataset_root}/${split}/`
- `labels`: mapping from PCAP stem → label
- `json_dir`: where MQTT-only JSON will be written (under `dataset_root`)
- `outputs_dir`: where final parquet/csv live (under `dataset_root`)
- `parquet_name`: output parquet filename (e.g., `mqtt_features.parquet`)

---

## 3) Run

**Option A — Step-by-step**

    # 1) PCAP → MQTT JSON (via tshark)
    python scripts/python/step01_pcap_to_json.py --config configs/config.python.json

    # 2) (Optional) PCAP summary via capinfos
    python scripts/python/step02_capinfos_summary.py --config configs/config.python.json

    # 3) JSON → Parquet (streamed with ijson)
    python scripts/python/step03_extract_from_json_to_parquet.py --config configs/config.python.json

**Option B — One-liner (no Makefile needed)**

    python scripts/python/step01_pcap_to_json.py --config configs/config.python.json && \
    python scripts/python/step02_capinfos_summary.py --config configs/config.python.json && \
    python scripts/python/step03_extract_from_json_to_parquet.py --config configs/config.python.json

**Option C — Makefile (if you added it)**

    make parquet CFG=configs/config.python.json

---

## 4) Outputs

- `${dataset_root}/${json_dir}/<pcap_stem>_mqtt.json`  *(intermediate)*
- `${dataset_root}/${outputs_dir}/pcap_capinfos_summary.csv`  *(optional summary)*
- `${dataset_root}/${outputs_dir}/mqtt_features.parquet`  **← main artifact**

---

## 5) Feature dictionary (columns in the Parquet)

- `time` (float, epoch), `length` (int)  
- `ip.src`, `ip.dst`  
- `mqtt.msgtype`, `mqtt.qos`, `mqtt.retain`, `mqtt.dup`  
- `mqtt.topic`, `mqtt.msg`  
- `topic_depth` (int), `topic_entropy` (float), `wildcard` (bool)  
- `payload_entropy` (float), `non_ascii_ratio` (float), `sensor_value` (float|NaN)  
- `payload_length` (int)  
- `label` (string; from config)

---

## 6) Tips & Troubleshooting

- **`tshark: command not found`** → Install Wireshark CLI (see prerequisites).
- **Empty Parquet** → Confirm your capture actually contains MQTT traffic; the filter `-Y mqtt` drops non-MQTT packets.
- **Performance** → JSON parsing uses `ijson` (streaming) to handle large files without exhausting RAM.
