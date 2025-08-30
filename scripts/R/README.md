# R scripts

- **01_cic-iomt2024_mqtt_mixed-clustering_repro.R**  
  Reproducible pipeline to benchmark mixed-type clustering methods, compute internal/external metrics, and write figures + tables.

## How to run (RStudio)
1. Open the script.
2. Session → Set Working Directory → *To Source File Location*.
3. Source the file, or run line-by-line.  
   Make sure `data/processed/mqtt_features.parquet` exists locally.

Outputs are written under `outputs/…` and include figures, CSV summaries, and reproducibility artefacts:
- `config.json`, `session_info.txt`, `git_hash.txt`, `input_sha256.txt`.
