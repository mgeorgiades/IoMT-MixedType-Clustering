#!/usr/bin/env Rscript
# =============================================================================
# IoMT MQTT Mixed-Type Clustering — Reproducible Main Script (R)
# -----------------------------------------------------------------------------
# Author:       Michael Georgiades
# Date:         (stamped at runtime; see run.log and config.json)
# Description:  Reproducible pipeline to benchmark mixed-type clustering
#               methods on CICIoMT2024 MQTT packet-level features, compute
#               external/internal validation metrics, and save figures + tables.
#
# Dataset:      CICIoMT2024 (MQTT subset)
#               https://www.unb.ca/cic/datasets/iomt-dataset-2024.html
#
# License:      GPL-3.0-only (match repository)
# Repo (planned): https://github.com/mgeorgiades/IoMT-MixedType-Clustering
#
# Usage example:
#   Rscript 01_cic-iomt2024_mqtt_mixed-clustering_repro.R \
#     --input data/processed/mqtt_features.parquet \
#     --out outputs/runs/$(date +%Y%m%d_%H%M%S) \
#     --iters 50 --k 6 --seed 123 --sample 1000
# =============================================================================

# -------- utilities -----------------------------------------------------------
stopf <- function(...) stop(sprintf(...), call. = FALSE)

# Minimal arg parser: supports --key value and --key=value
.parse_args <- function() {
  raw <- commandArgs(trailingOnly = TRUE)
  args <- list()
  i <- 1
  while (i <= length(raw)) {
    token <- raw[i]
    if (startsWith(token, "--")) {
      token <- sub("^--", "", token)
      if (grepl("=", token, fixed = TRUE)) {
        kv <- strsplit(token, "=", fixed = TRUE)[[1]]
        args[[kv[1]]] <- kv[2]
      } else {
        if (i == length(raw) || startsWith(raw[i+1], "--")) {
          args[[token]] <- TRUE
        } else {
          args[[token]] <- raw[i+1]
          i <- i + 1
        }
      }
    }
    i <- i + 1
  }
  args
}

# Detect directory where this script lives (works in Rscript & RStudio)
script_dir <- (function() {
  sp <- sub("^--file=", "", commandArgs()[grepl("^--file=", commandArgs())])
  if (length(sp) == 1 && nzchar(sp)) return(normalizePath(dirname(sp), winslash = "/"))
  if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
    return(normalizePath(dirname(rstudioapi::getSourceEditorContext()$path), winslash = "/"))
  }
  normalizePath(getwd(), winslash = "/")
})()

# Default config (override via CLI args)
cfg <- list(
  input  = file.path(script_dir, "mqtt_features.parquet"),
  out    = file.path(script_dir, "r_plots20"),
  iters  = 50,
  k      = 6,
  seed   = 123,
  sample = 1000
)

# Apply CLI overrides
args <- .parse_args()
for (nm in names(args)) {
  if (nm %in% names(cfg)) {
    val <- args[[nm]]
    if (nm %in% c("iters","k","seed","sample")) val <- as.integer(val)
    cfg[[nm]] <- val
  }
}

# Create output directory + start logging (console + file)
if (!dir.exists(cfg$out)) dir.create(cfg$out, recursive = TRUE, showWarnings = FALSE)
log_path <- file.path(cfg$out, "run.log")
lf <- file(log_path, open = "wt")
sink(lf, type = "output", split = TRUE)
sink(lf, type = "message", append = TRUE)
cat(sprintf("=== IoMT MQTT Clustering — run started at %s ===\n", format(Sys.time(), usetz = TRUE)))

# Deterministic RNG
set.seed(cfg$seed, kind = "L'Ecuyer-CMRG")

# ---------- dependencies (fail fast with helpful hint) -----------------------
need_pkgs <- c(
  # IO / meta
  "arrow", "jsonlite", "digest",
  # data wrangling
  "tibble", "dplyr", "tidyr", "readr",
  # clustering & validation
  "cluster", "mclust", "clustMixType", "kamila", "aricode", "clue",
  "clValid", "gower",
  # modeling / DR
  "FactoMineR", "psych", "kohonen", "dbscan", "umap", "ContaminatedMixt",
  # viz
  "ggplot2", "patchwork", "RColorBrewer", "gridExtra", "reshape2",
  # stats / metrics
  "pROC", "igraph", "combinat",
  # export
  "writexl"
)

missing <- need_pkgs[!vapply(need_pkgs, requireNamespace, quietly = TRUE, FUN.VALUE = logical(1))]
if (length(missing)) {
  cat("Missing packages:\n"); print(missing)
  stopf("Install missing packages, e.g.:\ninstall.packages(c(%s))",
        paste(sprintf('"%s"', missing), collapse = ", "))
}

# Attach frequently used packages
suppressPackageStartupMessages({
  library(arrow); library(jsonlite); library(digest)
  library(tibble); library(dplyr); library(tidyr); library(readr)
  library(ggplot2); library(patchwork); library(RColorBrewer); library(gridExtra); library(reshape2)
  library(cluster); library(mclust); library(clustMixType); library(kamila); library(aricode); library(clue)
  library(clValid); library(gower)
  library(FactoMineR); library(psych); library(kohonen); library(dbscan); library(umap); library(ContaminatedMixt)
  library(pROC); library(igraph); library(combinat); library(writexl)
})

# ---------- record reproducibility artifacts ---------------------------------
# 1) config.json
json_cfg_path <- file.path(cfg$out, "config.json")
jsonlite::write_json(cfg, json_cfg_path, pretty = TRUE, auto_unbox = TRUE)

# 2) session_info.txt
sess_path <- file.path(cfg$out, "session_info.txt")
writeLines(c(capture.output(sessionInfo())), con = sess_path)

# 3) git hash (if repo)
git_hash <- tryCatch(system("git rev-parse HEAD", intern = TRUE, ignore.stderr = TRUE),
                     error = function(e) NA_character_)
writeLines(if (length(git_hash) == 1 && nzchar(git_hash) && !grepl("fatal", git_hash))
             git_hash else "not-a-git-repo",
           file.path(cfg$out, "git_hash.txt"))

# 4) input SHA256
if (!file.exists(cfg$input)) stopf("Input parquet not found: %s", cfg$input)
writeLines(digest::digest(cfg$input, algo = "sha256", file = TRUE),
           file.path(cfg$out, "input_sha256.txt"))

# Helper: safe print that never interrupts a run
safe_print <- function(x, ...) {
  tryCatch({
    if (inherits(x, "data.frame") && !inherits(x, "tbl_df")) x <- tibble::as_tibble(x)
    print(x, ...)
  }, error = function(e) message("print skipped: ", e$message))
}

# Helper for output paths (centralized)
out_path <- function(...) file.path(cfg$out, ...)

# ---------- schema & data load -----------------------------------------------
cat_cols <- c("ip.src","ip.dst","mqtt.msgtype","mqtt.qos","mqtt.retain","mqtt.dup","mqtt.topic","mqtt.msg")
num_cols <- c("time","length","topic_depth","topic_entropy","payload_entropy","non_ascii_ratio","sensor_value","payload_length")
label_col <- "label" # optional ground truth

options(arrow.skip_nul = TRUE)
cat(sprintf("Reading parquet: %s\n", cfg$input))
df <- arrow::read_parquet(cfg$input)

# enforce schema
has <- names(df)
miss_cat <- setdiff(cat_cols, has)
miss_num <- setdiff(num_cols, has)
if (length(miss_cat) || length(miss_num)) {
  message("WARNING — missing expected columns:")
  if (length(miss_cat)) message("  categorical: ", paste(miss_cat, collapse = ", "))
  if (length(miss_num)) message("  numeric:     ", paste(miss_num, collapse = ", "))
}
present_cat <- intersect(cat_cols, has)
present_num <- intersect(num_cols, has)

df[present_cat] <- lapply(df[present_cat], as.factor)
df[present_num] <- lapply(df[present_num], function(x) suppressWarnings(as.numeric(x)))

used_cols <- c(present_cat, present_num, intersect(label_col, has))
df <- df[stats::complete.cases(df[, used_cols, drop = FALSE]), , drop = FALSE]
if (length(present_num)) {
  df <- df[apply(df[, present_num, drop = FALSE], 1, function(x) all(is.finite(x))), , drop = FALSE]
}
cat("Rows after cleaning:", nrow(df), "\n")
if (label_col %in% names(df)) {
  cat("Label Distribution:\n"); safe_print(table(df[[label_col]]))
}

# =============================================================================
#                              METRICS HELPERS
# =============================================================================

safe_ari <- function(truth, pred, drop_noise = FALSE, noise_label = 0) {
  stopifnot(length(truth) == length(pred))
  keep <- !is.na(truth) & !is.na(pred)
  if (drop_noise) keep <- keep & (pred != noise_label)
  if (!any(keep)) return(NA_real_)
  t <- as.vector(truth[keep]); p <- as.vector(pred[keep])
  if (length(unique(t)) < 2 || length(unique(p)) < 2) return(NA_real_)
  mclust::adjustedRandIndex(t, p)
}

safe_ami <- function(truth, pred, drop_noise = FALSE, noise_label = 0) {
  stopifnot(length(truth) == length(pred))
  keep <- !is.na(truth) & !is.na(pred)
  if (drop_noise) keep <- keep & (pred != noise_label)
  if (!any(keep)) return(NA_real_)
  t <- as.vector(truth[keep]); p <- as.vector(pred[keep])
  if (length(unique(t)) < 2 || length(unique(p)) < 2) return(NA_real_)
  aricode::AMI(t, p)
}

# Hungarian label mapping for supervised-style metrics
hungarian_map <- function(truth, pred) {
  truth <- factor(truth); pred <- factor(pred)
  tab0  <- table(truth, pred)
  tab <- tab0
  r <- nrow(tab); c <- ncol(tab)
  if (r < c) tab <- rbind(tab, matrix(0, nrow = c - r, ncol = c,
                                      dimnames = list(paste0(".d", seq_len(c - r)), colnames(tab))))
  if (c < r) tab <- cbind(tab, matrix(0, nrow = nrow(tab), ncol = r - c,
                                      dimnames = list(rownames(tab), paste0(".d", seq_len(r - c)))))
  cost <- max(tab) - tab
  assign <- clue::solve_LSAP(cost)
  inv <- setNames(seq_along(assign), colnames(tab)[assign])
  cmap <- setNames(rep(NA_character_, ncol(tab0)), colnames(tab0))
  for (cl in colnames(tab0)) {
    row_name <- rownames(tab)[ inv[cl] ]
    if (is.na(row_name) || startsWith(row_name, ".d")) {
      row_name <- rownames(tab0)[ which.max(tab0[, cl, drop = TRUE]) ]
    }
    cmap[cl] <- row_name
  }
  cmap
}

# Accuracy/Precision/Recall/Specificity/F1 (+ optional ROC-AUC) after mapping
cls_scores <- function(truth, pred, scores = NULL, drop_noise = FALSE, noise_label = 0) {
  stopifnot(length(truth) == length(pred))
  keep <- !is.na(truth) & !is.na(pred)
  if (drop_noise) keep <- keep & (pred != noise_label)
  if (!any(keep)) {
    nm <- c("acc","precision_macro","recall_macro","specificity_macro","f1_macro",
            "precision_weighted","recall_weighted","specificity_weighted","f1_weighted",
            "roc_auc_macro","roc_auc_weighted")
    out <- rep(NA_real_, length(nm)); names(out) <- nm; return(out)
  }
  truth <- factor(truth[keep]); pred <- factor(pred[keep])
  cmap <- hungarian_map(truth, pred)
  pred_mapped <- factor(cmap[as.character(pred)], levels = levels(truth))
  cm <- table(truth, pred_mapped)
  n  <- sum(cm); TP <- diag(cm)
  FP <- colSums(cm) - TP; FN <- rowSums(cm) - TP; TN <- n - TP - FP - FN
  support <- rowSums(cm)
  prec_i <- TP / (TP + FP); rec_i <- TP / (TP + FN)
  tnr_i  <- TN / (TN + FP)
  f1_i   <- 2 * prec_i * rec_i / (prec_i + rec_i)
  fix_na <- function(x) { x[!is.finite(x)] <- NA_real_; x }
  prec_i <- fix_na(prec_i); rec_i <- fix_na(rec_i); tnr_i <- fix_na(tnr_i); f1_i <- fix_na(f1_i)
  acc <- sum(TP) / n
  precision_macro <- mean(prec_i, na.rm = TRUE)
  recall_macro    <- mean(rec_i,  na.rm = TRUE)
  specificity_macro <- mean(tnr_i, na.rm = TRUE)
  f1_macro        <- mean(f1_i,   na.rm = TRUE)
  w <- support / sum(support)
  precision_weighted <- sum(prec_i * w, na.rm = TRUE)
  recall_weighted    <- sum(rec_i  * w, na.rm = TRUE)
  specificity_weighted <- sum(tnr_i * w, na.rm = TRUE)
  f1_weighted        <- sum(f1_i   * w, na.rm = TRUE)
  auc_macro <- NA_real_; auc_weighted <- NA_real_
  if (!is.null(scores)) {
    scores <- as.matrix(scores)[keep, , drop = FALSE]
    classes <- levels(truth)
    inv_map <- tapply(names(cmap), cmap, function(v) v[1])
    class_scores <- matrix(NA_real_, nrow(scores), length(classes),
                           dimnames = list(NULL, classes))
    for (cl in classes) {
      k <- inv_map[[cl]]
      if (length(k) == 1 && !is.null(k) && k %in% colnames(scores)) {
        class_scores[, cl] <- scores[, k]
      }
    }
    auc_per_class <- sapply(classes, function(cl) {
      s <- class_scores[, cl]; y <- as.integer(truth == cl)
      ok <- is.finite(s) & (y %in% c(0,1))
      if (sum(ok) > 1 && length(unique(y[ok])) == 2 && length(unique(s[ok])) > 1) {
        suppressWarnings(as.numeric(pROC::auc(pROC::roc(y[ok], s[ok], quiet = TRUE))))
      } else NA_real_
    })
    auc_macro    <- mean(auc_per_class, na.rm = TRUE)
    auc_weighted <- sum(auc_per_class * w, na.rm = TRUE)
  }
  c(acc = acc,
    precision_macro = precision_macro,
    recall_macro    = recall_macro,
    specificity_macro = specificity_macro,
    f1_macro        = f1_macro,
    precision_weighted = precision_weighted,
    recall_weighted    = recall_weighted,
    specificity_weighted = specificity_weighted,
    f1_weighted        = f1_weighted,
    roc_auc_macro      = auc_macro,
    roc_auc_weighted   = auc_weighted)
}

# =============================================================================
#                          METHODS RUNNER (one sample)
# =============================================================================
run_all_methods <- function(df_sample, true_k, iter_id,
                            cat_cols, num_cols, label_col) {
  metrics <- data.frame(Method = character(), ARI = numeric(), AMI = numeric(),
                        Silhouette = numeric(), Dunn = numeric(), Iteration = integer(),
                        stringsAsFactors = FALSE)
  s_list <- list()  # per-method score matrices (N x K) when available

  available_num_cols <- intersect(num_cols, colnames(df_sample))
  cols_all <- c(cat_cols, available_num_cols)

  # ---------- PAM (Gower) ----------
  dist_all <- cluster::daisy(df_sample[, cols_all], metric = "gower")
  pam_fit <- cluster::pam(dist_all, k = true_k)
  sil_pam <- cluster::silhouette(pam_fit$clustering, dist_all)
  metrics <- rbind(metrics, data.frame(
    Method="PAM", Iteration=iter_id,
    ARI=safe_ari(df_sample[[label_col]], pam_fit$clustering),
    AMI=safe_ami(df_sample[[label_col]], pam_fit$clustering),
    Silhouette=mean(sil_pam[,3]), Dunn=clValid::dunn(dist_all, pam_fit$clustering)
  ))
  pam_id <- pam_fit$id.med
  S <- -as.matrix(dist_all)[, pam_id, drop = FALSE]; colnames(S) <- as.character(seq_along(pam_id))
  s_list[["PAM"]] <- S

  # ---------- PAM Entropy weights ----------
  entropy_weights <- apply(df_sample[, available_num_cols, drop=FALSE], 2, function(x) {
    p <- table(x) / length(x); -sum(p * log(p + 1e-10))
  })
  entropy_weights <- entropy_weights / sum(entropy_weights)
  w_entropy <- c(rep(0, length(cat_cols)), entropy_weights)
  dist_entropy <- cluster::daisy(df_sample[, cols_all], metric = "gower", weights = w_entropy)
  pam_entropy <- cluster::pam(dist_entropy, k = true_k)
  sil_entropy <- cluster::silhouette(pam_entropy$clustering, dist_entropy)
  metrics <- rbind(metrics, data.frame(
    Method="PAM Entropy", Iteration=iter_id,
    ARI=safe_ari(df_sample[[label_col]], pam_entropy$clustering),
    AMI=safe_ami(df_sample[[label_col]], pam_entropy$clustering),
    Silhouette=mean(sil_entropy[,3]), Dunn=clValid::dunn(dist_entropy, pam_entropy$clustering)
  ))
  pamE_id <- pam_entropy$id.med
  S <- -as.matrix(dist_entropy)[, pamE_id, drop = FALSE]; colnames(S) <- as.character(seq_along(pamE_id))
  s_list[["PAM Entropy"]] <- S

  # ---------- PAM MQTT weights ----------
  mqtt_weights <- rep(1, length(available_num_cols)); names(mqtt_weights) <- available_num_cols
  for (nm in c("topic_entropy","payload_entropy","sensor_value")) {
    if (nm %in% names(mqtt_weights)) mqtt_weights[nm] <- 3
  }
  mqtt_weights <- mqtt_weights / sum(mqtt_weights)
  w_mqtt <- c(rep(0, length(cat_cols)), mqtt_weights)
  dist_mqtt <- cluster::daisy(df_sample[, cols_all], metric = "gower", weights = w_mqtt)
  pam_mqtt <- cluster::pam(dist_mqtt, k = true_k)
  sil_mqtt <- cluster::silhouette(pam_mqtt$clustering, dist_mqtt)
  metrics <- rbind(metrics, data.frame(
    Method="PAM MQTT", Iteration=iter_id,
    ARI=safe_ari(df_sample[[label_col]], pam_mqtt$clustering),
    AMI=safe_ami(df_sample[[label_col]], pam_mqtt$clustering),
    Silhouette=mean(sil_mqtt[,3]), Dunn=clValid::dunn(dist_mqtt, pam_mqtt$clustering)
  ))
  pamM_id <- pam_mqtt$id.med
  S <- -as.matrix(dist_mqtt)[, pamM_id, drop = FALSE]; colnames(S) <- as.character(seq_along(pamM_id))
  s_list[["PAM MQTT"]] <- S

  # ---------- KAMILA ----------
  if (length(available_num_cols)) {
    kam <- kamila::kamila(df_sample[, available_num_cols, drop=FALSE],
                          df_sample[, cat_cols, drop=FALSE],
                          numClust = true_k, numInit = 10)
    metrics <- rbind(metrics, data.frame(
      Method="KAMILA", Iteration=iter_id,
      ARI=safe_ari(df_sample[[label_col]], kam$finalMemb),
      AMI=safe_ami(df_sample[[label_col]], kam$finalMemb),
      Silhouette=NA, Dunn=NA
    ))
  }

  # ---------- KAMILA (PCA on numeric block) ----------
  if (length(available_num_cols)) {
    num_pca <- prcomp(df_sample[, available_num_cols, drop=FALSE], center = TRUE, scale. = TRUE)
    var_expl <- cumsum(num_pca$sdev^2 / sum(num_pca$sdev^2))
    k_pcs <- which(var_expl >= 0.90)[1]; if (is.na(k_pcs)) k_pcs <- min(10, ncol(num_pca$x))
    num_scores <- num_pca$x[, 1:k_pcs, drop = FALSE]
    kam_pca <- kamila::kamila(as.data.frame(num_scores),
                              as.data.frame(df_sample[, cat_cols, drop=FALSE]),
                              numClust=true_k, numInit=10)
    metrics <- rbind(metrics, data.frame(
      Method="KAMILA (PCA-num)", Iteration=iter_id,
      ARI=safe_ari(df_sample[[label_col]], kam_pca$finalMemb),
      AMI=safe_ami(df_sample[[label_col]], kam_pca$finalMemb),
      Silhouette=NA, Dunn=NA
    ))
  }

  # ---------- FAMD + kmeans ----------
  famd <- FactoMineR::FAMD(df_sample[, cols_all], ncp = 10, graph = FALSE)
  coords <- famd$ind$coord
  km <- kmeans(coords, centers = true_k)
  dist_famd <- dist(coords)
  sil_famd <- cluster::silhouette(km$cluster, dist_famd)
  metrics <- rbind(metrics, data.frame(
    Method="FAMD", Iteration=iter_id,
    ARI=safe_ari(df_sample[[label_col]], km$cluster),
    AMI=safe_ami(df_sample[[label_col]], km$cluster),
    Silhouette=mean(sil_famd[,3]), Dunn=clValid::dunn(dist_famd, km$cluster)
  ))
  K <- nrow(km$centers)
  S <- sapply(1:K, function(j) rowSums((coords - matrix(km$centers[j,], nrow(coords), ncol(coords), TRUE))^2))
  S <- -S; colnames(S) <- as.character(1:K); s_list[["FAMD"]] <- S

  # ---------- FAMD Weighted (same as FAMD+kmeans for now) ----------
  km_w <- kmeans(famd$ind$coord, centers = true_k)
  dist_fw <- dist(famd$ind$coord)
  sil_fw <- cluster::silhouette(km_w$cluster, dist_fw)
  metrics <- rbind(metrics, data.frame(
    Method="FAMD Weighted", Iteration=iter_id,
    ARI=safe_ari(df_sample[[label_col]], km_w$cluster),
    AMI=safe_ami(df_sample[[label_col]], km_w$cluster),
    Silhouette=mean(sil_fw[,3]), Dunn=clValid::dunn(dist_fw, km_w$cluster)
  ))
  K <- nrow(km_w$centers)
  S <- sapply(1:K, function(j) rowSums((famd$ind$coord - matrix(km_w$centers[j,], nrow(famd$ind$coord), ncol(famd$ind$coord), TRUE))^2))
  S <- -S; colnames(S) <- as.character(1:K); s_list[["FAMD Weighted"]] <- S

  # ---------- k-Prototypes ----------
  kproto_fit <- clustMixType::kproto(df_sample[, cols_all], k = true_k)
  metrics <- rbind(metrics, data.frame(
    Method="k-Prototypes", Iteration=iter_id,
    ARI=safe_ari(df_sample[[label_col]], kproto_fit$cluster),
    AMI=safe_ami(df_sample[[label_col]], kproto_fit$cluster),
    Silhouette=NA, Dunn=NA
  ))

  # ---------- Hierarchical (Gower, Ward.D2) ----------
  dist_hc <- cluster::daisy(df_sample[, cols_all], metric = "gower")
  hc_fit <- hclust(dist_hc, method = "ward.D2")
  hc_clusters <- cutree(hc_fit, k = true_k)
  sil_hc <- cluster::silhouette(hc_clusters, dist_hc)
  metrics <- rbind(metrics, data.frame(
    Method="Hierarchical (Gower)", Iteration=iter_id,
    ARI=safe_ari(df_sample[[label_col]], hc_clusters),
    AMI=safe_ami(df_sample[[label_col]], hc_clusters),
    Silhouette=mean(sil_hc[,3]), Dunn=clValid::dunn(dist_hc, hc_clusters)
  ))

  # ---------- FAMD + Mclust (GMM) ----------
  famd_m <- FactoMineR::FAMD(df_sample[, cols_all], ncp = 10, graph = FALSE)
  famd_coords <- famd_m$ind$coord
  mclust_fit <- mclust::Mclust(famd_coords, G = true_k)
  metrics <- rbind(metrics, data.frame(
    Method="FAMD + Mclust", Iteration=iter_id,
    ARI=safe_ari(df_sample[[label_col]], mclust_fit$classification),
    AMI=safe_ami(df_sample[[label_col]], mclust_fit$classification),
    Silhouette=NA, Dunn=NA
  ))
  S <- mclust_fit$z; colnames(S) <- as.character(1:ncol(S)); s_list[["FAMD + Mclust"]] <- S

  # ---------- SOM ----------
  som_df <- df_sample[, cols_all]
  som_df[] <- lapply(som_df, function(x) if (is.factor(x)) as.numeric(as.factor(x)) else x)
  som_matrix <- as.matrix(scale(som_df))
  grid <- kohonen::somgrid(xdim = 4, ydim = 3, topo = "hexagonal")
  som_model <- kohonen::som(som_matrix, grid = grid, rlen = 100)
  som_cluster <- cutree(hclust(dist(som_model$codes[[1]])), k = true_k)
  assignments <- som_cluster[som_model$unit.classif]
  assign("som_models", {tmp <- get("som_models", envir=.GlobalEnv); tmp[[iter_id]] <- som_model; tmp}, envir=.GlobalEnv)
  metrics <- rbind(metrics, data.frame(
    Method="SOM", Iteration=iter_id,
    ARI=safe_ari(df_sample[[label_col]], assignments),
    AMI=safe_ami(df_sample[[label_col]], assignments),
    Silhouette=NA, Dunn=NA
  ))

  # ---------- UMAP + DBSCAN ----------
  umap_input <- df_sample[, cols_all]
  umap_input[] <- lapply(umap_input, function(x) if (is.factor(x)) as.numeric(as.factor(x)) else x)
  umap_coords <- umap::umap(as.matrix(umap_input))$layout
  dbscan_result <- dbscan::dbscan(umap_coords, eps = 0.3, minPts = 10)
  db_clusters <- dbscan_result$cluster # 0 = noise
  if (length(unique(db_clusters)) > 1) {
    metrics <- rbind(metrics, data.frame(
      Method="UMAP + DBSCAN", Iteration=iter_id,
      ARI=safe_ari(df_sample[[label_col]], db_clusters, drop_noise = TRUE, noise_label = 0),
      AMI=safe_ami(df_sample[[label_col]], db_clusters, drop_noise = TRUE, noise_label = 0),
      Silhouette=NA, Dunn=NA
    ))
  }

  # ---------- UMAP + Mclust ----------
  gmm_fit <- mclust::Mclust(umap_coords, G = true_k)
  metrics <- rbind(metrics, data.frame(
    Method="UMAP + Mclust", Iteration=iter_id,
    ARI=safe_ari(df_sample[[label_col]], gmm_fit$classification),
    AMI=safe_ami(df_sample[[label_col]], gmm_fit$classification),
    Silhouette=NA, Dunn=NA
  ))

  # ---------- Modha–Spangler weighted KMeans ----------
  ms_df <- df_sample[, cols_all]
  ms_df[] <- lapply(ms_df, function(x) if (is.factor(x)) as.numeric(as.factor(x)) else x)
  init_km <- kmeans(scale(ms_df), centers = true_k, nstart = 10)
  centers <- init_km$centers
  within_var <- apply(scale(ms_df), 2, function(x) var(x[!is.na(x)]))
  between_var <- apply(centers, 2, var)
  weights <- between_var / (within_var + 1e-10)
  weighted_df <- scale(ms_df) * matrix(rep(weights, each = nrow(ms_df)), ncol = length(weights))
  ms_km <- kmeans(weighted_df, centers = true_k, nstart = 10)
  sil_ms <- cluster::silhouette(ms_km$cluster, dist(weighted_df))
  metrics <- rbind(metrics, data.frame(
    Method="Modha-Spangler KMeans", Iteration=iter_id,
    ARI=safe_ari(df_sample[[label_col]], ms_km$cluster),
    AMI=safe_ami(df_sample[[label_col]], ms_km$cluster),
    Silhouette=mean(sil_ms[,3]), Dunn=clValid::dunn(dist(weighted_df), ms_km$cluster)
  ))
  K <- nrow(ms_km$centers)
  S <- sapply(1:K, function(j) rowSums((weighted_df - matrix(ms_km$centers[j,], nrow(weighted_df), ncol(weighted_df), TRUE))^2))
  S <- -S; colnames(S) <- as.character(1:K); s_list[["Modha-Spangler KMeans"]] <- S

  # ---------- Factor Analysis + KMeans ----------
  if (length(available_num_cols)) {
    num_only <- df_sample[, available_num_cols, drop=FALSE]
    fa_res <- psych::fa(scale(num_only), nfactors = 5, rotate = "varimax", fm = "ml")
    fa_scores <- fa_res$scores
    fa_km <- kmeans(fa_scores, centers = true_k, nstart = 10)
    sil_fa <- cluster::silhouette(fa_km$cluster, dist(fa_scores))
    metrics <- rbind(metrics, data.frame(
      Method="FactorAnalyzer + KMeans", Iteration=iter_id,
      ARI=safe_ari(df_sample[[label_col]], fa_km$cluster),
      AMI=safe_ami(df_sample[[label_col]], fa_km$cluster),
      Silhouette=mean(sil_fa[,3]), Dunn=clValid::dunn(dist(fa_scores), fa_km$cluster)
    ))
    K <- nrow(fa_km$centers)
    S <- sapply(1:K, function(j) rowSums((fa_scores - matrix(fa_km$centers[j,], nrow(fa_scores), ncol(fa_scores), TRUE))^2))
    S <- -S; colnames(S) <- as.character(1:K); s_list[["FactorAnalyzer + KMeans"]] <- S
  }

  # ---------- Contaminated Normal Mixtures ----------
  cn_labels <- rep(NA_integer_, nrow(df_sample))
  if (length(available_num_cols)) {
    cn_data <- as.matrix(scale(df_sample[, available_num_cols, drop = FALSE]))
    tryCatch({
      cn_fit <- ContaminatedMixt::CNmixt(
        X = cn_data, G = true_k, model = "EEE", initialization = "kmeans"
      )
      lab <- ContaminatedMixt::getCluster(cn_fit)
      if (!is.null(lab) && length(lab) == nrow(df_sample)) {
        cn_labels <- as.integer(lab)
        metrics <- rbind(metrics, data.frame(
          Method="Contaminated Normal Mixtures", Iteration=iter_id,
          ARI=safe_ari(df_sample[[label_col]], cn_labels),
          AMI=safe_ami(df_sample[[label_col]], cn_labels),
          Silhouette=NA, Dunn=NA
        ))
      }
    }, error = function(e) {
      cat("ContaminatedMixt failed on iteration", iter_id, ":", conditionMessage(e), "\n")
    })
  }

  # Attach cluster labels and scores so caller can compute supervised metrics
  attr(metrics, "cluster_labels") <- list(
    PAM = pam_fit$clustering,
    `PAM Entropy` = pam_entropy$clustering,
    `PAM MQTT` = pam_mqtt$clustering,
    KAMILA = if (exists("kam")) kam$finalMemb else rep(NA, nrow(df_sample)),
    `KAMILA (PCA-num)` = if (exists("kam_pca")) kam_pca$finalMemb else rep(NA, nrow(df_sample)),
    FAMD = km$cluster,
    `FAMD Weighted` = km_w$cluster,
    `k-Prototypes` = kproto_fit$cluster,
    `Hierarchical (Gower)` = hc_clusters,
    `FAMD + Mclust` = mclust_fit$classification,
    SOM = assignments,
    `UMAP + DBSCAN` = if (exists("db_clusters")) db_clusters else rep(NA, nrow(df_sample)),
    `UMAP + Mclust` = if (exists("gmm_fit")) gmm_fit$classification else rep(NA, nrow(df_sample)),
    `Modha-Spangler KMeans` = ms_km$cluster,
    `FactorAnalyzer + KMeans` = if (exists("fa_km")) fa_km$cluster else rep(NA, nrow(df_sample)),
    `Contaminated Normal Mixtures` = cn_labels
  )
  attr(metrics, "cluster_scores") <- s_list
  metrics
}

# =============================================================================
#                         MAIN LOOP (Monte Carlo runs)
# =============================================================================
true_k <- cfg$k
n_iter <- cfg$iters

som_models <- list()
cluster_labels_all <- list()
cluster_scores_all <- list()
truth_list <- list()

results <- data.frame(
  Method = character(), ARI = numeric(), AMI = numeric(),
  Silhouette = numeric(), Dunn = numeric(), Iteration = integer(),
  stringsAsFactors = FALSE
)

for (i in seq_len(n_iter)) {
  cat("Iteration:", i, "\n")
  set.seed(cfg$seed + i)
  df_sample <- df[sample(nrow(df), cfg$sample), , drop = FALSE]
  truth_list[[i]] <- if (label_col %in% names(df_sample)) df_sample[[label_col]] else rep(NA, nrow(df_sample))

  metrics <- run_all_methods(df_sample, true_k, i, cat_cols, num_cols, label_col)
  results <- rbind(results, metrics)

  cluster_labels_all[[i]] <- attr(metrics, "cluster_labels")
  cluster_scores_all[[i]] <- attr(metrics, "cluster_scores")
}

# Save raw iteration metrics
readr::write_csv(results, out_path("metrics_all_iterations.csv"))

# =============================================================================
#           SUPERVISED-STYLE METRICS per method×iteration (Hungarian map)
# =============================================================================
cls_rows <- list()
for (i in seq_along(cluster_labels_all)) {
  truth <- truth_list[[i]]
  clist <- cluster_labels_all[[i]]
  slist <- if (length(cluster_scores_all) >= i) cluster_scores_all[[i]] else NULL
  for (m in names(clist)) {
    pred <- clist[[m]]
    if (is.null(pred) || all(is.na(pred)) || length(unique(pred)) < 2) next
    drop_noise <- identical(m, "UMAP + DBSCAN")
    scores <- if (!is.null(slist) && m %in% names(slist)) slist[[m]] else NULL
    s <- cls_scores(truth, pred, scores = scores, drop_noise = drop_noise, noise_label = 0)
    cls_rows[[length(cls_rows)+1]] <- cbind.data.frame(Iteration = i, Method = m, t(as.data.frame(s)), row.names = NULL)
  }
}
cls_df <- if (length(cls_rows)) do.call(rbind, cls_rows) else data.frame(Iteration=integer(), Method=character())
if (nrow(cls_df)) {
  readr::write_csv(cls_df, out_path("classification_metrics.csv"))
  results_with_cls <- dplyr::left_join(results, cls_df, by = c("Iteration","Method"))
  readr::write_csv(results_with_cls, out_path("metrics_with_classification.csv"))
}

# =============================================================================
#                       BOUNDS / DIAGNOSTIC TABLES
# =============================================================================
all_methods <- c(
  "PAM","PAM Entropy","PAM MQTT","KAMILA","KAMILA (PCA-num)",
  "FAMD","FAMD Weighted","k-Prototypes","Hierarchical (Gower)",
  "FAMD + Mclust","SOM","UMAP + DBSCAN","UMAP + Mclust",
  "Modha-Spangler KMeans","FactorAnalyzer + KMeans",
  "Contaminated Normal Mixtures"
)

qnum <- function(x, p) as.numeric(stats::quantile(x, probs = p, na.rm = TRUE, type = 7))
n_total_fn <- function(x) length(x)
n_valid_fn <- function(x) sum(is.finite(x))
prop_valid_fn <- function(x) mean(is.finite(x))
min_safe <- function(x) if (all(is.na(x))) NA_real_ else min(x, na.rm = TRUE)
median_safe <- function(x) if (all(is.na(x))) NA_real_ else median(x, na.rm = TRUE)
mean_safe <- function(x) if (all(is.na(x))) NA_real_ else mean(x, na.rm = TRUE)
max_safe <- function(x) if (all(is.na(x))) NA_real_ else max(x, na.rm = TRUE)
sd_safe <- function(x) {x <- x[is.finite(x)]; if (length(x) <= 1) NA_real_ else sd(x)}
var_safe <- function(x) {x <- x[is.finite(x)]; if (length(x) <= 1) NA_real_ else var(x)}
iqr_safe <- function(x) if (all(is.na(x))) NA_real_ else IQR(x, na.rm = TRUE)
mad_safe <- function(x) if (all(is.na(x))) NA_real_ else mad(x, na.rm = TRUE, constant = 1)
p05_fn <- function(x) qnum(x, 0.05); p95_fn <- function(x) qnum(x, 0.95)
trimmed10_fn <- function(x) if (all(is.na(x))) NA_real_ else mean(x, trim = 0.10, na.rm = TRUE)
range_safe <- function(x) if (all(is.na(x))) NA_real_ else diff(range(x, na.rm = TRUE))
se_safe <- function(x) {x <- x[is.finite(x)]; if (length(x) <= 1) NA_real_ else sd(x)/sqrt(length(x))}
ci95_lo_fn <- function(x) {x <- x[is.finite(x)]; if (length(x) <= 1) NA_real_ else mean(x) - 1.96*sd(x)/sqrt(length(x))}
ci95_hi_fn <- function(x) {x <- x[is.finite(x)]; if (length(x) <= 1) NA_real_ else mean(x) + 1.96*sd(x)/sqrt(length(x))}
cv_fn <- function(x) {x <- x[is.finite(x)]; m <- mean(x); s <- sd(x); if (!is.finite(m) || abs(m)<.Machine$double.eps) NA_real_ else s/m}

metric_cols <- c("ARI","AMI")
bounds_tbl <- results %>%
  dplyr::group_by(Method) %>%
  dplyr::summarise(
    dplyr::across(
      .cols = dplyr::all_of(metric_cols),
      .fns = list(
        n_total    = n_total_fn, n_valid = n_valid_fn, prop_valid = prop_valid_fn,
        min = min_safe, q1 = ~qnum(.x, .25), median = median_safe, mean = mean_safe,
        q3 = ~qnum(.x, .75), max = max_safe, sd = sd_safe, var = var_safe, iqr = iqr_safe,
        mad = mad_safe, p05 = p05_fn, p95 = p95_fn, trimmed10 = trimmed10_fn, range = range_safe,
        se = se_safe, ci95_lo = ci95_lo_fn, ci95_hi = ci95_hi_fn, cv = cv_fn
      ),
      .names = "{.col}_{.fn}"
    ),
    .groups = "drop"
  ) %>%
  dplyr::right_join(tibble::tibble(Method = all_methods), by = "Method") %>%
  dplyr::arrange(match(Method, all_methods))

readr::write_csv(bounds_tbl, out_path("bounds_table.csv"))
safe_print(bounds_tbl)

counts_tbl <- results %>% dplyr::count(Method) %>% dplyr::arrange(desc(n))
safe_print(counts_tbl)

diag_tbl <- results %>%
  dplyr::group_by(Method) %>%
  dplyr::summarise(n_iter = dplyr::n(),
                   n_valid_ari = sum(is.finite(ARI)),
                   n_valid_ami = sum(is.finite(AMI)), .groups = "drop") %>%
  dplyr::arrange(match(Method, all_methods))
readr::write_csv(diag_tbl, out_path("metric_counts_by_method.csv"))

# =============================================================================
#                        TRUTH + TOP-N GRID (FAMD / MDS)
# =============================================================================
top7_manual <- c("Hierarchical (Gower)","FAMD + Mclust","FAMD","KAMILA",
                 "Contaminated Normal Mixtures","FactorAnalyzer + KMeans","UMAP + Mclust")

plot_truth_plus_topN_grid <- function(methods  = c("Ground truth", top7_manual),
                                      embedding = c("FAMD2","MDS"),
                                      k = cfg$k, ncol = 4,
                                      point_size = 0.9, alpha = 0.7,
                                      base_theme = c("grey","minimal"),
                                      out = out_path("truth_plus_top7_grid.png"),
                                      drop_dbscan_noise_for_metrics = TRUE,
                                      text_size = 8) {
  stopifnot(label_col %in% names(df_sample_fixed))
  embedding  <- match.arg(embedding)
  base_theme <- match.arg(base_theme)
  theme_fn   <- switch(base_theme, grey = ggplot2::theme_grey, minimal = ggplot2::theme_minimal)

  df_vis <- df_sample_fixed
  avail_num <- intersect(num_cols, names(df_vis))
  cols_all <- c(cat_cols, avail_num)
  truth <- factor(df_vis[[label_col]])
  meth <- setdiff(methods, "Ground truth")

  famd_obj <- NULL; d_gower <- NULL; XY <- NULL; xlab <- ""; ylab <- ""
  if (embedding == "FAMD2") {
    famd_obj <- FactoMineR::FAMD(df_vis[, cols_all], ncp = 2, graph = FALSE)
    XY <- as.data.frame(famd_obj$ind$coord[, 1:2, drop = FALSE]); names(XY) <- c("X","Y")
    xlab <- "FAMD 1"; ylab <- "FAMD 2"
  } else {
    d_gower <- cluster::daisy(df_vis[, cols_all], metric = "gower")
    XY <- as.data.frame(cmdscale(as.matrix(d_gower), k = 2)); names(XY) <- c("X","Y")
    xlab <- "MDS 1"; ylab <- "MDS 2"
  }

  if (is.null(d_gower) && any(meth %in% c("PAM","PAM Entropy","PAM MQTT","Hierarchical (Gower)")))
    d_gower <- cluster::daisy(df_vis[, cols_all], metric = "gower")
  if (is.null(famd_obj) && any(meth %in% c("FAMD","FAMD Weighted","FAMD + Mclust")))
    famd_obj <- FactoMineR::FAMD(df_vis[, cols_all], ncp = 10, graph = FALSE)
  need_umap <- any(meth %in% c("UMAP + Mclust","UMAP + DBSCAN"))
  if (need_umap) {
    um_in <- df_vis[, cols_all]
    um_in[] <- lapply(um_in, function(x) if (is.factor(x)) as.numeric(as.factor(x)) else x)
    um_coords <- umap::umap(as.matrix(um_in))$layout
  }

  nmi_vs_truth <- function(pred, drop_noise = FALSE) {
    p <- pred; if (is.factor(p)) p <- as.character(p)
    keep <- !is.na(p)
    if (drop_noise) keep <- keep & !(p %in% c("0","Noise"))
    t <- truth[keep]; q <- factor(p[keep])
    if (length(unique(t)) < 2 || length(unique(q)) < 2) return(NA_real_)
    suppressWarnings(aricode::NMI(t, q))
  }
  fmt <- function(x) if (is.na(x)) "NA" else sprintf("%.2f", x)

  mk_panel <- function(title, colour_vec, legend_pos = NULL, legend_title = NULL,
                       legend_text_size = 5, legend_title_size = 6) {
    g <- ggplot2::ggplot(transform(XY, C = colour_vec), ggplot2::aes(X, Y, color = C)) +
      ggplot2::geom_point(alpha = alpha, size = point_size) +
      ggplot2::labs(title = title, x = xlab, y = ylab) +
      theme_fn(base_size = text_size) +
      theme(axis.title = element_text(size = text_size),
            axis.text  = element_text(size = text_size),
            plot.title = element_text(size = text_size, face = "bold"),
            strip.text = element_text(size = text_size),
            legend.position = "none")
    if (!is.null(legend_pos)) {
      g <- g +
        scale_color_discrete(drop = FALSE, name = legend_title) +
        guides(color = guide_legend(ncol = 1, byrow = TRUE,
                                    label.position = "right",
                                    keyheight = grid::unit(8, "pt"),
                                    keywidth  = grid::unit(10, "pt"),
                                    override.aes = list(shape = 16, size = 2.4, alpha = 1, stroke = 0))) +
        theme(legend.position = legend_pos, legend.justification = c(0, 0),
              legend.direction = "vertical",
              legend.title = element_text(size = legend_title_size, face = "bold", hjust = 0),
              legend.text  = element_text(size = legend_text_size))
    }
    g
  }

  compute_labels <- function(method_name) {
    tryCatch({
      switch(method_name,
        "Hierarchical (Gower)" = cutree(hclust(d_gower, method = "ward.D2"), k = k),
        "FAMD" = { km <- kmeans(famd_obj$ind$coord, centers = k, nstart = 20); km$cluster },
        "FAMD + Mclust" = mclust::Mclust(famd_obj$ind$coord, G = k)$classification,
        "KAMILA" = kamila::kamila(df_vis[, avail_num, drop=FALSE], df_vis[, cat_cols, drop=FALSE], numClust = k, numInit = 10)$finalMemb,
        "Contaminated Normal Mixtures" = {
          X <- scale(df_vis[, avail_num, drop=FALSE])
          fit <- ContaminatedMixt::CNmixt(X = as.matrix(X), G = k, model = "EEE", initialization = "kmeans")
          as.integer(ContaminatedMixt::getCluster(fit))
        },
        "FactorAnalyzer + KMeans" = {
          fa_res <- psych::fa(scale(df_vis[, avail_num, drop=FALSE]), nfactors = 5, rotate = "varimax", fm = "ml")
          kmeans(fa_res$scores, centers = k, nstart = 10)$cluster
        },
        "UMAP + Mclust" = mclust::Mclust(um_coords, G = k)$classification,
        stop(sprintf("Unsupported method: %s", method_name))
      )
    }, error = function(e) {
      labs <- rep(NA_integer_, nrow(df_vis))
      attr(labs, "err") <- conditionMessage(e)
      labs
    })
  }

  panels <- list()
  panels[["Ground truth"]] <- mk_panel(
    "Ground truth", truth, legend_pos = c(0.055, 0.09), legend_title = "Label", legend_text_size = 5, legend_title_size = 6
  )
  for (m in meth) {
    labs <- compute_labels(m)
    err  <- attr(labs, "err")
    drop_noise <- drop_dbscan_noise_for_metrics && identical(m, "UMAP + DBSCAN")
    nmi <- if (all(is.na(labs))) NA_real_ else nmi_vs_truth(labs, drop_noise)
    ari <- if (all(is.na(labs))) NA_real_ else safe_ari(truth, labs)
    ami <- if (all(is.na(labs))) NA_real_ else safe_ami(truth, labs)
    title <- if (is.null(err)) sprintf("%s — NMI %s | ARI %s | AMI %s", m, fmt(nmi), fmt(ari), fmt(ami))
             else sprintf("%s — FAILED (%s)", m, err)
    if (!is.factor(labs)) labs <- factor(paste0("C", labs))
    panels[[m]] <- mk_panel(title, labs)
  }
  g <- patchwork::wrap_plots(panels, ncol = ncol)
  nrows <- ceiling(length(panels)/ncol)
  ggplot2::ggsave(out, g, width = 3.1 * ncol, height = 2.6 * nrows, dpi = 1200, bg = "white")
  invisible(g)
}

# fixed sample for shared visualizations
set.seed(cfg$seed + 999)
df_sample_fixed <- df[sample(nrow(df), cfg$sample), , drop = FALSE]

plot_truth_plus_topN_grid(embedding = "FAMD2",
                          out = out_path("truth_plus_top7_grid_FAMD_GREY.png"),
                          base_theme = "grey", text_size = 6, ncol = 4)
plot_truth_plus_topN_grid(embedding = "MDS",
                          out = out_path("truth_plus_top7_grid_MDS_GREY.png"),
                          base_theme = "grey", text_size = 6, ncol = 4)

# =============================================================================
#               CORRELATION HEATMAPS (internal vs external)
# =============================================================================
# Internal index helpers
idx_avewithin <- function(d, z){
  n <- attr(d, "Size"); D <- as.matrix(d); labs <- factor(z)
  nums <- sapply(levels(labs), function(k){
    ix <- which(labs == k); if (length(ix) <= 1) return(NA_real_)
    m <- D[ix, ix]; mean(m[upper.tri(m)], na.rm=TRUE)
  })
  mean(nums, na.rm=TRUE)
}
idx_maxdiameter <- function(d, z){
  D <- as.matrix(d); labs <- factor(z)
  max(sapply(levels(labs), function(k){
    ix <- which(labs == k); if (length(ix) <= 1) return(0)
    max(D[ix, ix][upper.tri(D[ix, ix])], na.rm=TRUE)
  }), na.rm=TRUE)
}
idx_widestgap <- function(d, z){
  D <- as.matrix(d); labs <- factor(z)
  max(sapply(levels(labs), function(k){
    ix <- which(labs == k); if (length(ix) <= 1) return(0)
    W <- D[ix, ix]; W[diag(length(ix))] <- 0
    g <- igraph::graph_from_adjacency_matrix(W, mode="undirected", weighted=TRUE, diag=FALSE)
    mst <- igraph::mst(g, weights=E(g)$weight)
    if (ecount(mst) == 0) return(0)
    max(E(mst)$weight, na.rm=TRUE)
  }), na.rm=TRUE)
}
idx_separation <- function(d, z, p = 0.1){
  D <- as.matrix(d); labs <- factor(z); vals <- c()
  for(k in levels(labs)){
    ix <- which(labs == k); jx <- which(labs != k)
    if (length(ix)==0 || length(jx)==0) next
    dmin <- apply(D[ix, jx, drop=FALSE], 1, min)
    m <- max(1, floor(p*length(ix)))
    dmin <- sort(dmin, decreasing = FALSE)[seq_len(m)]
    vals <- c(vals, dmin)
  }
  sum(vals, na.rm=TRUE)
}
idx_pearson_gamma <- function(d, z){
  D <- as.matrix(d)
  upper <- which(upper.tri(D), arr.ind = TRUE)
  distv <- D[upper]; same <- as.integer(z[upper[,1]] != z[upper[,2]])
  suppressWarnings(cor(distv, same))
}
idx_entropy <- function(z){ p <- prop.table(table(z)); -sum(p * log(p)) }
idx_cvnnd <- function(d, z, k = 2){
  D <- as.matrix(d); labs <- factor(z); vals <- c()
  for (lev in levels(labs)){
    ix <- which(labs == lev); if (length(ix) <= k) next
    DD <- D[ix, ix]; diag(DD) <- Inf
    kdist <- apply(DD, 1, function(row) sort(row, partial = k)[k])
    cv <- sd(kdist)/mean(kdist); vals <- c(vals, rep(cv, length(ix)))
  }
  mean(vals, na.rm=TRUE)
}
idx_gaussianity <- function(X, z){
  labs <- factor(z); p <- ncol(X); ks <- c()
  for (lev in levels(labs)){
    ix <- which(labs == lev); if (length(ix) <= p + 2) next
    Xi <- scale(X[ix, , drop=FALSE], center=TRUE, scale=FALSE)
    S <- tryCatch(cov(Xi), error=function(e) NULL); if (is.null(S) || det(S) <= 1e-12) next
    invS <- tryCatch(solve(S), error=function(e) NULL); if (is.null(invS)) next
    md2 <- rowSums((Xi %*% invS) * Xi); F_emp <- ecdf(md2); xs <- sort(md2)
    diffs <- abs(F_emp(xs) - pchisq(xs, df=p)); ks <- c(ks, max(diffs))
  }
  if (length(ks)==0) return(NA_real_); mean(ks, na.rm=TRUE)
}
idx_asw <- function(d, z){ if (length(unique(z)) < 2) return(NA_real_); mean(cluster::silhouette(as.integer(factor(z)), d)[,3], na.rm=TRUE) }

# Build correlation table on fixed sample
df_sample_corr <- df_sample_fixed
available_num_cols <- intersect(num_cols, colnames(df_sample_corr))
cols_all <- c(cat_cols, available_num_cols)
truth_corr <- as.vector(df_sample_corr[[label_col]])

X_all <- df_sample_corr[, cols_all]
X_all[] <- lapply(X_all, function(x) if (is.factor(x)) as.numeric(as.factor(x)) else x)
X_all <- as.matrix(X_all)

dist_base <- cluster::daisy(df_sample_corr[, cols_all], metric = "gower")
famd_obj  <- FactoMineR::FAMD(df_sample_corr[, cols_all], ncp = 10, graph = FALSE)
famd_coords <- famd_obj$ind$coord
um_coords <- umap::umap(as.matrix(X_all))$layout
X_num <- as.matrix(scale(df_sample_corr[, available_num_cols, drop = FALSE]))

labels_on_fixed <- function(method_name){
  tryCatch({
    switch(method_name,
      "Hierarchical (Gower)" = cutree(hclust(dist_base, method = "ward.D2"), k = true_k),
      "FAMD"                = kmeans(famd_coords, centers = true_k, nstart = 20)$cluster,
      "FAMD + Mclust"       = mclust::Mclust(famd_coords, G = true_k)$classification,
      "KAMILA"              = kamila::kamila(df_sample_corr[, available_num_cols, drop = FALSE],
                                             df_sample_corr[, cat_cols, drop = FALSE],
                                             numClust = true_k, numInit = 10)$finalMemb,
      "Contaminated Normal Mixtures" = {
        fit <- ContaminatedMixt::CNmixt(X = X_num, G = true_k, model = "EEE", initialization = "kmeans")
        as.integer(ContaminatedMixt::getCluster(fit))
      },
      "FactorAnalyzer + KMeans" = {
        fa_res <- psych::fa(scale(df_sample_corr[, available_num_cols, drop = FALSE]),
                            nfactors = 5, rotate = "varimax", fm = "ml")
        kmeans(fa_res$scores, centers = true_k, nstart = 10)$cluster
      },
      "UMAP + Mclust" = mclust::Mclust(um_coords, G = true_k)$classification,
      stop(paste("Unsupported method:", method_name))
    )
  }, error = function(e) {
    warning(sprintf("Method %s failed: %s", method_name, conditionMessage(e)))
    rep(NA_integer_, nrow(df_sample_corr))
  })
}

methods_for_heatmap <- c("Hierarchical (Gower)","FAMD + Mclust","FAMD","KAMILA",
                         "Contaminated Normal Mixtures","FactorAnalyzer + KMeans","UMAP + Mclust")

# some extra external metrics
.choose2 <- function(x) x*(x-1)/2
fm_index <- function(truth, pred) {
  tab <- table(truth, pred); n_ij <- as.numeric(tab); a_i <- rowSums(tab); b_j <- colSums(tab)
  TP <- sum(.choose2(n_ij)); FP <- sum(.choose2(b_j)) - TP; FN <- sum(.choose2(a_i)) - TP
  if ((TP+FP)==0 || (TP+FN)==0) return(NA_real_); sqrt((TP/(TP+FP)) * (TP/(TP+FN)))
}
purity <- function(truth, pred) { tab <- table(truth, pred); sum(apply(tab, 2, max)) / sum(tab) }
b3_f1 <- function(truth, pred) {
  tab <- table(truth, pred); n <- sum(tab); rs <- rowSums(tab); cs <- colSums(tab)
  P <- sum(tab^2 / rep(cs, each = nrow(tab)), na.rm = TRUE) / n
  R <- sum(tab^2 / rep(rs, times = ncol(tab)), na.rm = TRUE) / n
  if (!is.finite(P) || !is.finite(R) || (P+R)==0) return(NA_real_); 2*P*R/(P+R)
}
hom_comp_v <- function(truth, pred) {
  tab <- table(truth, pred); n <- sum(tab); rs <- rowSums(tab); cs <- colSums(tab)
  H <- function(p) { p <- p[p>0]; -sum(p*log(p)) }
  p_c <- rs/n; p_k <- cs/n; H_C <- H(p_c); H_K <- H(p_k)
  nz <- tab > 0
  H_CK <- -sum(tab[nz] * (log(tab[nz]) - rep(log(cs), each=nrow(tab))[nz])) / n
  H_KC <- -sum(tab[nz] * (log(tab[nz]) - rep(log(rs), times=ncol(tab))[nz])) / n
  hom <- if (H_C > 0) 1 - H_CK / H_C else 1
  com <- if (H_K > 0) 1 - H_KC / H_K else 1
  v   <- if ((hom+com) > 0) 2*hom*com/(hom+com) else 0
  c(homogeneity = hom, completeness = com, v_measure = v)
}

index_rows <- list()
for (mname in methods_for_heatmap) {
  z_raw <- labels_on_fixed(mname)
  keep_ext <- !is.na(z_raw)
  truth <- truth_corr[keep_ext]; pred <- z_raw[keep_ext]
  ari <- safe_ari(truth, pred); ami <- safe_ami(truth, pred)
  nmi <- tryCatch(aricode::NMI(truth, pred), error=function(e) NA_real_)
  vi  <- tryCatch(aricode::VI(truth, pred),  error=function(e) NA_real_)
  fm  <- fm_index(truth, pred); pu <- purity(truth, pred); b3 <- b3_f1(truth, pred)
  hc  <- hom_comp_v(truth, pred)

  z_int <- as.integer(factor(z_raw))
  aw  <- idx_avewithin(dist_base, z_int)
  md  <- idx_maxdiameter(dist_base, z_int)
  wg  <- idx_widestgap(dist_base, z_int)
  si  <- idx_separation(dist_base, z_int)
  pg  <- idx_pearson_gamma(dist_base, z_int)
  en  <- idx_entropy(z_int)
  cvn <- idx_cvnnd(dist_base, z_int)
  nor <- idx_gaussianity(X_all, z_int)
  asw <- idx_asw(dist_base, z_int)

  index_rows[[length(index_rows)+1]] <- data.frame(
    Iteration = 1, Method = mname,
    ari = ari, ami = ami, nmi = nmi, neg_vi = ifelse(is.na(vi), NA, -vi),
    fm = fm, purity = pu, b3_f1 = b3,
    homogeneity = unname(hc["homogeneity"]),
    completeness = unname(hc["completeness"]),
    v_measure    = unname(hc["v_measure"]),
    aw = -aw, md = -md, wg = -wg, si = si, pg = pg, en = en, cvn = -cvn, nor = -nor, asw = asw,
    stringsAsFactors = FALSE
  )
}
index_df <- do.call(rbind, index_rows)

index_df$dm <- scale(index_df$asw + index_df$si - index_df$aw)[,1]
index_df$dc <- scale(index_df$pg - index_df$si)[,1]

locked_order <- c("asw","dm","md","pg","dc","si","wg","cvn","nor","en","aw","ari","ami","neg_vi")
cols_for_cor <- locked_order
M <- index_df[, cols_for_cor]
M <- M[, colSums(!is.na(M)) > 0, drop=FALSE]
cormat <- cor(M, use = "pairwise.complete.obs")
cm_long <- reshape2::melt(cormat, varnames = c("x","y"), value.name = "r")
cm_long$x <- factor(cm_long$x, levels = locked_order)
cm_long$y <- factor(cm_long$y, levels = rev(locked_order))

p_corr <- ggplot(cm_long, aes(x = x, y = y, fill = r)) +
  geom_tile(color = "grey30") +
  geom_text(aes(label = sprintf("%.2f", r)), size = 3, color = "black") +
  scale_fill_gradient2(limits=c(-1,1), midpoint = 0,
                       low = "#f3a6a6", mid = "white", high = "#4c5a74") +
  coord_fixed() +
  labs(title = "Correlation matrix of internal and external indexes", x = NULL, y = NULL) +
  theme_minimal(base_size = 12) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), panel.grid = element_blank(),
        plot.title = element_text(face="bold", hjust = 0.5))
ggsave(out_path("index_correlation_heatmap.png"), p_corr, width = 9, height = 8, dpi = 1200)

# Extended external set
locked_order_ext <- c("asw","dm","md","pg","dc","si","wg","cvn","nor","en","aw",
                      "ari","ami","nmi","fm","purity","b3_f1","homogeneity","completeness","v_measure","neg_vi")
cols2 <- intersect(locked_order_ext, names(index_df))
M2 <- index_df[, cols2]; M2 <- M2[, colSums(!is.na(M2)) > 0, drop = FALSE]
cormat2 <- cor(M2, use = "pairwise.complete.obs")
cm2_long <- reshape2::melt(cormat2, varnames = c("x","y"), value.name = "r")
cm2_long$x <- factor(cm2_long$x, levels = cols2); cm2_long$y <- factor(cm2_long$y, levels = rev(cols2))

p_corr_ext <- ggplot(cm2_long, aes(x = x, y = y, fill = r)) +
  geom_tile(color = "grey30") + geom_text(aes(label = sprintf("%.2f", r)), size = 3, color = "black") +
  scale_fill_gradient2(limits = c(-1, 1), midpoint = 0, low = "#f3a6a6", mid = "white", high = "#4c5a74") +
  coord_fixed() +
  labs(title = "Correlation matrix (internal + extended external metrics)", x = NULL, y = NULL) +
  theme_minimal(base_size = 12) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), panel.grid = element_blank(),
        plot.title = element_text(face = "bold", hjust = 0.5))
ggsave(out_path("index_correlation_heatmap_extended.png"), p_corr_ext, width = 10, height = 9, dpi = 1200)

internal <- c("asw","dm","md","pg","dc","si","wg","cvn","nor","en","aw")
external <- c("ari","ami","nmi","fm","purity","b3_f1","homogeneity","completeness","v_measure","neg_vi")
int_cols <- intersect(internal, names(index_df)); ext_cols <- intersect(external, names(index_df))
C <- cor(index_df[, int_cols], index_df[, ext_cols], use = "pairwise.complete.obs")
cr_long <- reshape2::melt(C, varnames = c("Internal","External"), value.name = "r")
cr_long$Internal <- factor(cr_long$Internal, levels = rev(int_cols))
cr_long$External <- factor(cr_long$External, levels = ext_cols)
p_cross <- ggplot(cr_long, aes(x = External, y = Internal, fill = r)) +
  geom_tile(color = "grey30") + geom_text(aes(label = sprintf("%.2f", r)), size = 3, color = "black") +
  scale_fill_gradient2(limits = c(-1, 1), midpoint = 0, low = "#f3a6a6", mid = "white", high = "#4c5a74") +
  labs(title = "Internal vs External (extended) — cross-correlation", x = NULL, y = NULL) +
  theme_minimal(base_size = 12) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), panel.grid = element_blank(),
        plot.title = element_text(face = "bold", hjust = 0.5))
ggsave(out_path("index_correlation_heatmap_cross.png"), p_cross, width = 10, height = 6.5, dpi = 1200)

# =============================================================================
#                     STABILITY: mean pairwise ARI per method
# =============================================================================
all_clusterings <- list()
for (i in seq_along(cluster_labels_all)) {
  clusters_iter <- cluster_labels_all[[i]]
  for (method in names(clusters_iter)) {
    labels <- clusters_iter[[method]]
    if (!is.null(labels) && length(unique(labels)) > 1) {
      all_clusterings[[paste(method, i, sep = "_")]] <- list(method = method, iteration = i, labels = labels)
    }
  }
}
compute_stability <- function(method_name, clusterings) {
  subset <- Filter(function(x) x$method == method_name, clusterings)
  if (length(subset) < 2) return(NA_real_)
  combos <- combn(length(subset), 2)
  aris <- sapply(seq_len(ncol(combos)), function(j) {
    i1 <- combos[1, j]; i2 <- combos[2, j]
    z1 <- subset[[i1]]$labels; z2 <- subset[[i2]]$labels
    keep <- !is.na(z1) & !is.na(z2)
    if (identical(method_name, "UMAP + DBSCAN")) keep <- keep & (z1 != 0) & (z2 != 0)
    if (!any(keep)) return(NA_real_)
    z1 <- as.vector(z1[keep]); z2 <- as.vector(z2[keep])
    if (length(unique(z1)) < 2 || length(unique(z2)) < 2) return(NA_real_)
    mclust::adjustedRandIndex(z1, z2)
  })
  mean(aris, na.rm = TRUE)
}
methods_all <- unique(sapply(all_clusterings, function(x) x$method))
stability_df <- data.frame(Method = methods_all,
                           Stability = sapply(methods_all, compute_stability, clusterings = all_clusterings))
readr::write_csv(stability_df, out_path("stability_mean_ari.csv"))

stability_df <- stability_df %>% arrange(Stability) %>% mutate(Method = factor(Method, levels = Method))
p_stab <- ggplot(stability_df, aes(x = Stability, y = Method)) +
  geom_col(fill = "#4A90E2", color = "black", width = 0.6) +
  scale_x_continuous(limits = c(0, 1), expand = c(0, 0)) +
  labs(title = "Clustering Stability Across Iterations", x = "Stability Score (0–1)", y = "Clustering Method") +
  theme_minimal(base_size = 14) +
  theme(panel.grid.major.y = element_blank(), panel.grid.minor = element_blank(),
        axis.text.y = element_text(size = 12, face = "bold"),
        axis.text.x = element_text(size = 12),
        plot.title = element_text(face = "bold", hjust = 0.5))
ggsave(out_path("stability_barplot.png"), p_stab, width = 10, height = 6, dpi = 600)

# =============================================================================
#                ANOVA + Tukey HSD (ARI, AMI, Silhouette)
# =============================================================================
anova_and_tukey <- function(metric) {
  formula <- as.formula(paste0(metric, " ~ Method"))
  aov_model <- aov(formula, data = results)
  tukey <- TukeyHSD(aov_model)
  png(out_path(paste0("tukey_", tolower(metric), "_plot.png")), width = 1200, height = 800)
  plot(tukey, las = 1); title(main = paste("Tukey HSD for", metric)); dev.off()
}
metrics_to_test <- c("ARI", "AMI", "Silhouette")
for (m in metrics_to_test) if (any(!is.na(results[[m]]))) anova_and_tukey(m)

# =============================================================================
#                    SILHOUETTE PLOTS (2x4 combined)
# =============================================================================
plot_silhouettes <- function(df_sample, method_name, cluster_labels, dist_matrix) {
  sil <- cluster::silhouette(cluster_labels, dist_matrix)
  suppressPackageStartupMessages(library(factoextra))
  fviz_silhouette(sil, print.summary = TRUE) +
    ggplot2::ggtitle(paste("Silhouette:", method_name)) +
    ggplot2::theme_minimal(base_size = 12) +
    ggplot2::scale_y_continuous(limits = c(0, 1)) +
    ggplot2::theme(legend.position = "none")
}
set.seed(cfg$seed + 444)
df_sil <- df[sample(nrow(df), cfg$sample), , drop = FALSE]
avail_num <- intersect(num_cols, colnames(df_sil))

dist_all <- cluster::daisy(df_sil[, c(cat_cols, avail_num)], metric = "gower")
pam_fit <- cluster::pam(dist_all, k = true_k)

entropy_weights <- apply(df_sil[, avail_num, drop=FALSE], 2, function(x) { p <- table(x)/length(x); -sum(p*log(p + 1e-10)) })
entropy_weights <- entropy_weights / sum(entropy_weights)
w_entropy <- c(rep(0, length(cat_cols)), entropy_weights)
dist_entropy <- cluster::daisy(df_sil[, c(cat_cols, avail_num)], metric = "gower", weights = w_entropy)
pam_entropy <- cluster::pam(dist_entropy, k = true_k)

mqtt_weights <- rep(1, length(avail_num)); names(mqtt_weights) <- avail_num
for (nm in c("topic_entropy","payload_entropy","sensor_value")) if (nm %in% names(mqtt_weights)) mqtt_weights[nm] <- 3
mqtt_weights <- mqtt_weights / sum(mqtt_weights)
w_mqtt <- c(rep(0, length(cat_cols)), mqtt_weights)
dist_mqtt <- cluster::daisy(df_sil[, c(cat_cols, avail_num)], metric = "gower", weights = w_mqtt)
pam_mqtt <- cluster::pam(dist_mqtt, k = true_k)

famd <- FactoMineR::FAMD(df_sil[, c(cat_cols, avail_num)], ncp = 10, graph = FALSE)
coords <- famd$ind$coord
famd_km <- kmeans(coords, centers = true_k)

famd_km_w <- kmeans(coords, centers = true_k)

dist_hc <- cluster::daisy(df_sil[, c(cat_cols, avail_num)], metric = "gower")
hc_fit <- hclust(dist_hc, method = "ward.D2")
hc_clusters <- cutree(hc_fit, k = true_k)

ms_df <- df_sil[, c(cat_cols, avail_num)]
ms_df[] <- lapply(ms_df, function(x) if (is.factor(x)) as.numeric(as.factor(x)) else x)
init_km <- kmeans(scale(ms_df), centers = true_k)
centers <- init_km$centers
within_var <- apply(scale(ms_df), 2, function(x) var(x[!is.na(x)]))
between_var <- apply(centers, 2, var)
weights <- between_var / (within_var + 1e-10)
weighted_df <- scale(ms_df) * matrix(rep(weights, each = nrow(ms_df)), ncol = length(weights))
ms_km <- kmeans(weighted_df, centers = true_k)

fa_res <- psych::fa(scale(df_sil[, avail_num, drop=FALSE]), nfactors = 5, rotate = "varimax", fm = "ml")
fa_scores <- fa_res$scores
fa_km <- kmeans(fa_scores, centers = true_k)

sil_plots <- list(
  plot_silhouettes(df_sil, "PAM", pam_fit$clustering, dist_all),
  plot_silhouettes(df_sil, "PAM Entropy", pam_entropy$clustering, dist_entropy),
  plot_silhouettes(df_sil, "PAM MQTT", pam_mqtt$clustering, dist_mqtt),
  plot_silhouettes(df_sil, "FAMD", famd_km$cluster, dist(coords)),
  plot_silhouettes(df_sil, "FAMD Weighted", famd_km_w$cluster, dist(coords)),
  plot_silhouettes(df_sil, "Hierarchical (Gower)", hc_clusters, dist_hc),
  plot_silhouettes(df_sil, "Modha-Spangler KMeans", ms_km$cluster, dist(weighted_df)),
  plot_silhouettes(df_sil, "FactorAnalyzer + KMeans", fa_km$cluster, dist(fa_scores))
)
combined_plot <- patchwork::wrap_plots(sil_plots, ncol = 4, nrow = 2) +
  patchwork::plot_annotation(title = "Silhouette Plots (2x4 Grid)", theme = theme(plot.title = element_text(size = 16, hjust = 0.5)))
ggsave(out_path("silhouette_combined_2x4.png"), combined_plot, width = 20, height = 10, dpi = 300)

# =============================================================================
#            FACETED VIOLIN + BOXPLOTS (raw / clipped / winsor / iqr)
# =============================================================================
df_long <- results %>%
  tidyr::pivot_longer(cols = c("ARI", "AMI"), names_to = "Metric", values_to = "Score") %>%
  dplyr::filter(!is.na(Score)) %>%
  dplyr::mutate(Score = dplyr::case_when(
    Metric == "AMI" ~ pmin(1, pmax(0, Score)),
    Metric == "ARI" ~ pmin(1, pmax(-1, Score)),
    TRUE ~ Score
  ))

winsorize_mm <- function(df, lower = 0.05, upper = 0.95) {
  df %>%
    dplyr::group_by(Method, Metric) %>%
    dplyr::mutate(lo = quantile(Score, lower, na.rm = TRUE),
                  hi = quantile(Score, upper, na.rm = TRUE),
                  Score_win = pmin(hi, pmax(lo, Score))) %>%
    dplyr::ungroup()
}
df_winsor <- winsorize_mm(df_long, 0.05, 0.95)

n_methods <- length(unique(df_long$Method))
w_in <- max(16, 1.0 * n_methods)

theme_journal <- function() {
  theme_minimal(base_size = 14) +
    theme(panel.grid.major.x = element_blank(),
          panel.grid.minor.x = element_blank(),
          panel.grid.minor.y = element_blank(),
          panel.grid.major.y = element_line(color = "grey80", linewidth = 0.3),
          panel.border       = element_rect(color = "grey70", fill = NA, linewidth = 0.5),
          axis.text.x        = element_text(angle = 45, hjust = 1),
          legend.position    = "none",
          plot.title         = element_text(face = "bold", hjust = 0.5))
}

stack_ami_ari <- function(p_top, p_bottom) {
  blank_x <- theme(axis.title.x = element_blank(), axis.text.x  = element_blank(), axis.ticks.x = element_blank())
  (p_top + blank_x + theme(plot.margin = margin(b = 6))) / (p_bottom + theme(plot.margin = margin(t = 6)))
}

p_free_ami <- ggplot(dplyr::filter(df_long, Metric == "AMI"),
                     aes(x = Method, y = Score, fill = Method)) +
  geom_violin(trim = FALSE, alpha = 0.6) +
  geom_boxplot(width = 0.1, outlier.size = 0.8, alpha = 0.8) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), legend.position = "none") +
  labs(title = "AMI", y = "Score", x = NULL)

p_free_ari <- ggplot(dplyr::filter(df_long, Metric == "ARI"),
                     aes(x = Method, y = Score, fill = Method)) +
  geom_violin(trim = FALSE, alpha = 0.6) +
  geom_boxplot(width = 0.1, outlier.size = 0.8, alpha = 0.8) +
  theme_minimal(base_size = 14) +
  theme(axis.text.x = element_text(angle = 45, hjust = 1), legend.position = "none") +
  labs(title = "ARI", y = "Score", x = "Clustering Method")

p_free_stack <- stack_ami_ari(p_free_ami, p_free_ari)
ggsave(out_path("faceted_violin_boxplot_ARI_AMI.png"), p_free_stack, width = w_in, height = 8, dpi = 1200)

mk_panel_styled <- function(df, metric_name, ylim_vec, title_text, y_col = "Score") {
  methods_levels <- all_methods
  df <- df %>% dplyr::filter(Metric == metric_name) %>% dplyr::mutate(Method = factor(Method, levels = methods_levels))
  n_methods <- length(levels(df$Method))
  palette_soft <- c(RColorBrewer::brewer.pal(9, "Pastel1"), RColorBrewer::brewer.pal(8, "Pastel2"))[1:n_methods]
  ggplot(df, aes(x = Method, y = .data[[y_col]], fill = Method)) +
    geom_violin(scale = "width", adjust = 1.2, width = 0.85, trim = FALSE, alpha = 0.85, color = NA) +
    geom_boxplot(width = 0.12, color = "black", fill = "grey30", alpha = 0.75, outlier.size = 0.9, outlier.alpha = 0.5) +
    stat_summary(fun = mean,   geom = "point", shape = 23, size = 2.8, fill = "white", color = "black") +
    stat_summary(fun = median, geom = "point", shape = 21, size = 2.2, fill = "white", color = "black") +
    scale_fill_manual(values = palette_soft) +
    coord_cartesian(ylim = ylim_vec) +
    theme_journal() +
    labs(title = title_text, x = NULL, y = paste(metric_name, "Score"))
}
p_clip_ami <- mk_panel_styled(df_long, "AMI", c(0, 1), "AMI (Clipped to [0, 1])") + theme(axis.title.x = element_blank(), axis.text.x  = element_blank(), axis.ticks.x = element_blank())
p_clip_ari <- mk_panel_styled(df_long, "ARI", c(0, 1), "ARI (Clipped to [0, 1])")
p_clipped <- p_clip_ami / p_clip_ari
ggsave(out_path("faceted_violin_boxplot_ARI_AMI_clipped.png"), p_clipped, width = w_in, height = 8, dpi = 1200, bg = "white")

# Winsorized, grouped by taxonomy (method short labels)
short_map <- c(
  "PAM"="PAM","k-Prototypes"="k-Proto","Hierarchical (Gower)"="HAC (Gower)","Modha-Spangler KMeans"="MS-kmeans",
  "PAM Entropy"="PAM (Ent.)","PAM MQTT"="PAM (MQTT)","FAMD + Mclust"="FAMD+GMM",
  "Contaminated Normal Mixtures"="CNMixt","KAMILA"="KAMILA","KAMILA (PCA-num)"="KAMILA (PCA)",
  "FAMD"="FAMD+kM","FAMD Weighted"="FAMD (wgt)","SOM"="SOM","UMAP + DBSCAN"="UMAP+DBSCAN",
  "UMAP + Mclust"="UMAP+GMM","FactorAnalyzer + KMeans"="FA+kmeans"
)
method_order_violin <- c("PAM","k-Prototypes","Hierarchical (Gower)","Modha-Spangler KMeans",
                         "PAM Entropy","PAM MQTT",
                         "FAMD + Mclust","Contaminated Normal Mixtures",
                         "KAMILA","KAMILA (PCA-num)","FAMD","FAMD Weighted","SOM",
                         "UMAP + DBSCAN","UMAP + Mclust","FactorAnalyzer + KMeans")
group_df <- data.frame(
  Method = method_order_violin,
  Group  = c(rep("Distance-Based", 4),
             rep("Weighted Similarity", 2),
             rep("Model-Based & Hybrid", 2),
             rep("Dimensionality Reduction–Aided", 8)),
  stringsAsFactors = FALSE
)
df_winsor_grp <- df_winsor %>%
  dplyr::left_join(group_df, by = "Method") %>%
  dplyr::mutate(Method_short = unname(ifelse(as.character(Method) %in% names(short_map),
                                             short_map[as.character(Method)], as.character(Method))))
base_methods <- method_order_violin
all_methods_short <- unname(ifelse(base_methods %in% names(short_map), short_map[base_methods], base_methods))
df_winsor_grp$Method_short <- factor(df_winsor_grp$Method_short, levels = all_methods_short)
palette_fixed <- setNames(c(RColorBrewer::brewer.pal(9, "Pastel1"),
                            RColorBrewer::brewer.pal(8, "Pastel2"))[1:length(all_methods_short)], all_methods_short)
mk_panel_styled_levels_facet <- function(df, metric_name, ylim_vec, y_col = "Score_win") {
  ggplot(dplyr::filter(df, Metric == metric_name),
         aes(x = Method_short, y = .data[[y_col]], fill = Method_short)) +
    geom_violin(scale = "width", adjust = 1.2, width = 0.85, trim = FALSE, alpha = 0.85, color = NA) +
    geom_boxplot(width = 0.12, color = "black", fill = "grey30", alpha = 0.75, outlier.size = 0.9, outlier.alpha = 0.5) +
    stat_summary(fun = mean,   geom = "point", shape = 23, size = 2.8, fill = "white", color = "black") +
    stat_summary(fun = median, geom = "point", shape = 21, size = 2.2, fill = "white", color = "black") +
    scale_fill_manual(values = palette_fixed, guide = "none") +
    coord_cartesian(ylim = ylim_vec) +
    facet_grid(~ Group, scales = "free_x", space = "free_x", switch = "x") +
    theme_journal() +
    theme(strip.placement = "outside", strip.background = element_rect(fill = "white", colour = "grey70"),
          strip.text = element_text(face = "bold"),
          panel.border = element_rect(color = "grey70", fill = NA, linewidth = 0.6),
          panel.spacing.x = unit(10, "pt")) +
    labs(title = NULL, x = NULL, y = paste(metric_name, "Score (Winsorized 5% tails)"))
}
p_win_ami_g <- mk_panel_styled_levels_facet(df_winsor_grp, "AMI", c(0, 1)) + theme(axis.title.x = element_blank(), axis.text.x  = element_blank(), axis.ticks.x = element_blank())
p_win_ari_g <- mk_panel_styled_levels_facet(df_winsor_grp, "ARI", c(0, 1)) + theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust = 1))
p_winsor_grouped_facets <- (p_win_ami_g / p_win_ari_g) +
  patchwork::plot_annotation(title = "AMI & ARI (winsorized 5% tails) grouped by taxonomy",
                             theme = theme(plot.title = element_text(hjust = 0.5, face = "bold")))
ggsave(out_path("faceted_violin_boxplot_ARI_AMI_winsor_grouped_facets.png"),
       p_winsor_grouped_facets, width = max(18, 1.2 * length(all_methods_short)), height = 8, dpi = 1200, bg = "white")

# Save detailed Excel for winsorized data
winsor_long <- df_winsor_grp %>%
  dplyr::transmute(Iteration = Iteration, Group = as.character(Group), Method = as.character(Method),
                   Method_short = as.character(Method_short), Metric = as.character(Metric),
                   Score_raw = Score, lo = lo, hi = hi, Score_win = Score_win) %>%
  dplyr::arrange(Group, Method_short, Metric, Iteration)
winsor_summary <- winsor_long %>%
  dplyr::group_by(Group, Method, Method_short, Metric) %>%
  dplyr::summarise(
    n = dplyr::n(),
    mean_raw = mean(Score_raw, na.rm = TRUE), sd_raw = sd(Score_raw, na.rm = TRUE),
    min_raw = min(Score_raw, na.rm = TRUE), q1_raw = quantile(Score_raw, 0.25, na.rm = TRUE),
    median_raw = median(Score_raw, na.rm = TRUE), q3_raw = quantile(Score_raw, 0.75, na.rm = TRUE),
    max_raw = max(Score_raw, na.rm = TRUE),
    mean_win = mean(Score_win, na.rm = TRUE), sd_win = sd(Score_win, na.rm = TRUE),
    min_win = min(Score_win, na.rm = TRUE), q1_win = quantile(Score_win, 0.25, na.rm = TRUE),
    median_win = median(Score_win, na.rm = TRUE), q3_win = quantile(Score_win, 0.75, na.rm = TRUE),
    max_win = max(Score_win, na.rm = TRUE),
    lo_used = dplyr::first(lo), hi_used = dplyr::first(hi), .groups = "drop"
  ) %>% dplyr::arrange(Group, Method_short, Metric)
palette_tbl <- tibble::tibble(Method_short = names(palette_fixed), FillHex = unname(palette_fixed)) %>%
  dplyr::mutate(Method_short = as.character(Method_short), FillHex = as.character(FillHex))
groups_tbl <- group_df %>%
  dplyr::mutate(Method = as.character(Method),
                Method_short = unname(ifelse(Method %in% names(short_map), short_map[Method], Method))) %>%
  dplyr::select(Group, Method, Method_short)
writexl::write_xlsx(
  x = list(winsor_long = winsor_long, winsor_summary = winsor_summary,
           palette = palette_tbl, groups = groups_tbl),
  path = out_path("winsorized_details.xlsx")
)

# IQR-filtered faceted plot
df_iqr <- df_long %>%
  dplyr::group_by(Metric, Method) %>%
  dplyr::mutate(q1 = quantile(Score, 0.25, na.rm = TRUE),
                q3 = quantile(Score, 0.75, na.rm = TRUE),
                iqr = q3 - q1, low = q1 - 1.5 * iqr, high = q3 + 1.5 * iqr) %>%
  dplyr::ungroup() %>%
  dplyr::filter(Score >= low, Score <= high)

mk_panel <- function(df, metric_name, ylim_vec, title_text, y_col = "Score", trim_violin = FALSE, hide_box_outliers = FALSE) {
  outlier_shape <- if (hide_box_outliers) NA else 16
  ggplot(dplyr::filter(df, Metric == metric_name), aes(x = Method, y = .data[[y_col]], fill = Method)) +
    geom_violin(scale = "width", adjust = 1.2, width = 0.8, trim = trim_violin, alpha = 0.6) +
    geom_boxplot(width = 0.12, outlier.shape = outlier_shape, alpha = 0.85) +
    coord_cartesian(ylim = ylim_vec) +
    theme_minimal(base_size = 14) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1), legend.position = "none") +
    labs(title = title_text, x = NULL, y = "Score")
}
p_iqr_ami <- mk_panel(df_iqr, "AMI", c(0, 1), "AMI (IQR-filtered: drop outside 1.5×IQR)")
p_iqr_ari <- mk_panel(df_iqr, "ARI", c(-1, 1), "ARI (IQR-filtered: drop outside 1.5×IQR)")
p_iqr <- stack_ami_ari(p_iqr_ami, p_iqr_ari)
ggsave(out_path("faceted_violin_boxplot_ARI_AMI_iqr.png"), p_iqr, width = w_in, height = 8, dpi = 1200)

# =============================================================================
#                        ARI/AMI BOUNDS SANITY PNG
# =============================================================================
tol <- 1e-9
bounds_global <- results %>%
  summarise(min_ARI = min(ARI, na.rm = TRUE), max_ARI = max(ARI, na.rm = TRUE),
            min_AMI = min(AMI, na.rm = TRUE), max_AMI = max(AMI, na.rm = TRUE)) %>%
  mutate(ok_ARI = (min_ARI >= -1 - tol & max_ARI <= 1 + tol),
         ok_AMI = (min_AMI >= 0  - tol & max_AMI <= 1 + tol))
readr::write_csv(bounds_global, out_path("ari_ami_bounds_global.csv"))
bounds_by_method <- results %>%
  group_by(Method) %>%
  summarise(min_ARI = min(ARI, na.rm = TRUE), max_ARI = max(ARI, na.rm = TRUE),
            min_AMI = min(AMI, na.rm = TRUE), max_AMI = max(AMI, na.rm = TRUE), .groups = "drop") %>%
  mutate(ok_ARI = (min_ARI >= -1 - tol & max_ARI <= 1 + tol),
         ok_AMI = (min_AMI >= 0  - tol & max_AMI <= 1 + tol))
readr::write_csv(bounds_by_method, out_path("ari_ami_bounds_by_method.csv"))
png(out_path("ari_ami_bounds_by_method_table.png"), width = 1600, height = 900, res = 150)
gridExtra::grid.table(bounds_by_method); dev.off()

# =============================================================================
#                            SOM VISUALIZATIONS
# =============================================================================
if (length(som_models) >= 1) {
  final_som <- som_models[[n_iter]]
  png(out_path("som_mapping.png"), width = 1200, height = 900); plot(final_som, type = "mapping", main = "SOM Node Mapping"); dev.off()
  png(out_path("som_distances.png"), width = 1200, height = 900); plot(final_som, type = "dist.neighbours", main = "SOM Neighbor Distances"); dev.off()
  png(out_path("som_codes.png"), width = 1200, height = 900); plot(final_som, type = "codes", main = "SOM Codebook Vectors"); dev.off()
  write.csv(final_som$codes[[1]], out_path("som_codebook_centers.csv"), row.names = FALSE)
}

# =============================================================================
#                                   FINISH
# =============================================================================
artifacts <- list.files(cfg$out, recursive = FALSE)
cat("\nArtifacts written to: ", cfg$out, "\n")
safe_print(artifacts)
cat("\nRun completed at: ", format(Sys.time(), usetz = TRUE), "\n")
sink(type = "message"); sink(type = "output"); close(lf)
