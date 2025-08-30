# IoMT-MixedType-Clustering

Reproducible benchmarking of **mixed-type clustering** methods on **CICIoMT2024 (MQTT)** features.

**Author:** Michael Georgiades  
**License:** GPL-3.0

---

## Overview

This project evaluates clustering methods that handle **numeric + categorical** IoMT traffic features and reports both **external** (e.g., ARI, AMI, NMI) and **internal** (e.g., silhouette, separation, gamma) validation metrics. It also produces publication-ready figures and stability analyses.

Included method families:

- Gower + **PAM** / **Hierarchical (Ward.D2)**
- **FAMD** (mixed PCA) + k-means / **GMM** (mclust)
- **k-Prototypes**, **KAMILA**, Modha–Spangler weighted k-means
- Factor analysis + k-means
- **UMAP** + DBSCAN / GMM
- **SOM** (Kohonen) with post-clustering

---

## Dataset

- **CICIoMT2024** (MQTT subset) – cross-layer packet captures with benign + attack traffic  
  https://www.unb.ca/cic/datasets/iomt-dataset-2024.html

> This repository **does not** redistribute the dataset. Place your processed parquet locally at:
