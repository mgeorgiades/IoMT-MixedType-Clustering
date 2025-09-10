# Python pipeline (PCAP → JSON → Parquet)

These scripts create a single `mqtt_features.parquet` with packet-level MQTT features.

## Prereqs

- Ubuntu, Python 3.11+
- Wireshark CLI tools installed: `tshark` and `capinfos`
- `pip install -r requirements-python.txt`

## Configure

Copy the example and edit paths if needed:

```bash
cp configs/config.python.example.json configs/config.python.json
