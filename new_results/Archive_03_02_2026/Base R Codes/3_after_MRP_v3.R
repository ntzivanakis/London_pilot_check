############################################################
# Poststrat collector + domain aggregation + CPI + mapping
# Assumes you already ran the MRP pipeline and have:
#   results/poststrat/<OUTCOME>/ward_mrp_estimates.csv
#
# What it does:
#  1) Reads all ward_mrp_estimates.csv under results/poststrat/
#  2) Builds a wide ward × outcome table of posterior means
#  3) Derives DOMAIN from outcome name (by default: SUBD__<DOMAIN>__...)
#  4) Computes domain scores (mean of outcomes in that domain)
#  5) Computes CPI (mean of domain z-scores) + optional rescale to 0-100
#  6) Builds Greater London ward geometry from LSOA shapefile + lookup
#  7) Saves tables + maps
############################################################

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(readr)
  library(purrr)
  library(sf)
  library(ggplot2)
})

# -------------------------
# Paths (edit if needed)
# -------------------------
POST_DIR <- file.path("results", "poststrat")

lsoa_shp_path     <- "data/shp/LSOA_2021_EW_BSC_V4.shp"
lsoa_to_wd_lad_fp <- "data/shp/LSOA_(2021)_to_Electoral_Ward_(2024)_to_LAD_(2024)_Best_Fit_Lookup_in_EW.csv"

OUT_SUMMARY_DIR <- file.path(POST_DIR, "_summary")
dir.create(OUT_SUMMARY_DIR, recursive = TRUE, showWarnings = FALSE)

# -------------------------
# Helpers
# -------------------------
std_names <- function(x) {
  x %>%
    tolower() %>%
    gsub("\ufeff", "", ., fixed = TRUE) %>%
    gsub("\\s+", "_", .) %>%
    gsub("[^a-z0-9_]+", "", .)
}

safe_z <- function(x) {
  mu <- mean(x, na.rm = TRUE)
  sdv <- sd(x, na.rm = TRUE)
  if (!is.finite(sdv) || sdv <= 0) return(rep(NA_real_, length(x)))
  (x - mu) / sdv
}

rescale_0_100 <- function(x) {
  rng <- range(x, na.rm = TRUE)
  if (!all(is.finite(rng)) || diff(rng) <= 0) return(rep(NA_real_, length(x)))
  100 * (x - rng[1]) / (rng[2] - rng[1])
}

# Domain parser:
# Default assumes outcomes look like: "SUBD__<DOMAIN>__<ITEM...>"
# If your names are different, modify this function.
parse_domain <- function(outcome) {
  parts <- strsplit(outcome, "__", fixed = TRUE)[[1]]
  if (length(parts) >= 2) return(parts[2])
  NA_character_
}

# -------------------------
# 1) Find and read all ward_mrp_estimates.csv
# -------------------------
files <- list.files(
  POST_DIR,
  pattern = "^ward_mrp_estimates\\.csv$",
  recursive = TRUE,
  full.names = TRUE
)

if (length(files) == 0) {
  stop("No ward_mrp_estimates.csv files found under: ", POST_DIR)
}

# outcome name = parent folder name (results/poststrat/<OUTCOME>/ward_mrp_estimates.csv)
get_outcome_from_path <- function(fp) basename(dirname(fp))

est_long <- purrr::map_dfr(files, function(fp) {
  outcome <- get_outcome_from_path(fp)
  x <- readr::read_csv(fp, show_col_types = FALSE)
  stopifnot(all(c("wd24cd","mean") %in% names(x)))
  x %>%
    transmute(
      wd24cd = as.character(wd24cd),
      outcome = outcome,
      mean = as.numeric(mean)
    )
})

# -------------------------
# 2) Wide ward × outcome table
# -------------------------
est_wide <- est_long %>%
  tidyr::pivot_wider(names_from = outcome, values_from = mean)

# Save raw combined means
readr::write_csv(est_long, file.path(OUT_SUMMARY_DIR, "ward_outcome_means_long.csv"))
readr::write_csv(est_wide, file.path(OUT_SUMMARY_DIR, "ward_outcome_means_wide.csv"))

# -------------------------
# 3) Compute domain scores
# -------------------------
outcomes <- setdiff(names(est_wide), "wd24cd")

outcome_domains <- tibble(
  outcome = outcomes,
  domain = vapply(outcomes, parse_domain, character(1))
)

# If some outcomes have NA domain, park them into "unknown"
outcome_domains <- outcome_domains %>%
  mutate(domain = ifelse(is.na(domain) | domain == "", "unknown", domain))

# Long with domain
est_domain_long <- est_long %>%
  left_join(outcome_domains, by = "outcome")

# Domain score per ward = mean of outcomes in that domain (you can swap to weighted if you want)
domain_scores <- est_domain_long %>%
  group_by(wd24cd, domain) %>%
  summarise(domain_mean = mean(mean, na.rm = TRUE), .groups = "drop") %>%
  tidyr::pivot_wider(names_from = domain, values_from = domain_mean, names_prefix = "DOM__")

# -------------------------
# 4) CPI: mean of domain z-scores (robust to differing scales)
# -------------------------
dom_cols <- names(domain_scores)[names(domain_scores) != "wd24cd"]

domain_scores_z <- domain_scores %>%
  mutate(across(all_of(dom_cols), safe_z, .names = "{.col}__z"))

dom_z_cols <- grep("__z$", names(domain_scores_z), value = TRUE)

cpi_tbl <- domain_scores_z %>%
  rowwise() %>%
  mutate(
    CPI_z = mean(c_across(all_of(dom_z_cols)), na.rm = TRUE)
  ) %>%
  ungroup() %>%
  mutate(
    CPI_0_100 = rescale_0_100(CPI_z)
  ) %>%
  select(wd24cd, CPI_z, CPI_0_100)

# Combined final ward table
ward_summary <- est_wide %>%
  left_join(domain_scores, by = "wd24cd") %>%
  left_join(cpi_tbl, by = "wd24cd")

readr::write_csv(ward_summary, file.path(OUT_SUMMARY_DIR, "ward_summary_outcomes_domains_cpi.csv"))

# -------------------------
# 5) Build Greater London ward geometry (from LSOA + lookup)
# -------------------------
lookup_lsoa <- readr::read_csv(lsoa_to_wd_lad_fp, show_col_types = FALSE)
names(lookup_lsoa) <- std_names(names(lookup_lsoa))

# Try to locate columns
wd_lookup_col   <- intersect(names(lookup_lsoa), c("wd24cd","ward24cd","electoral_ward_2024_code","ew24cd"))
lad_lookup_col  <- intersect(names(lookup_lsoa), c("lad24cd","lad_2024_code","local_authority_district_2024_code"))
lsoa_lookup_col <- intersect(names(lookup_lsoa), c("lsoa21cd","lsoa11cd","lsoa_code"))

if (length(wd_lookup_col) != 1 || length(lad_lookup_col) != 1 || length(lsoa_lookup_col) != 1) {
  stop("Could not uniquely identify wd/lad/lsoa cols in lookup. Found:\n",
       "wd: ", paste(wd_lookup_col, collapse = ", "), "\n",
       "lad: ", paste(lad_lookup_col, collapse = ", "), "\n",
       "lsoa: ", paste(lsoa_lookup_col, collapse = ", "))
}

wd_lookup_col  <- wd_lookup_col[1]
lad_lookup_col <- lad_lookup_col[1]
lsoa_lookup_col <- lsoa_lookup_col[1]

lookup2 <- lookup_lsoa %>%
  transmute(
    lsoa_code = as.character(.data[[lsoa_lookup_col]]),
    wd24cd    = as.character(.data[[wd_lookup_col]]),
    lad24cd   = as.character(.data[[lad_lookup_col]])
  ) %>%
  distinct()

# LSOA geometry
lsoa_sf <- sf::st_read(lsoa_shp_path, quiet = TRUE)
names(lsoa_sf) <- std_names(names(lsoa_sf))

lsoa_code_col <- intersect(names(lsoa_sf), c("lsoa21cd","lsoa11cd","lsoa_code"))
if (length(lsoa_code_col) != 1) stop("Could not uniquely identify LSOA code col in shapefile.")
lsoa_code_col <- lsoa_code_col[1]

lsoa_sf2 <- lsoa_sf %>%
  mutate(lsoa_code = as.character(.data[[lsoa_code_col]])) %>%
  left_join(lookup2, by = "lsoa_code") %>%
  filter(!is.na(wd24cd), !is.na(lad24cd)) %>%
  filter(str_detect(lad24cd, "^E09"))  # Greater London LADs

# Ward geometry by union of LSOAs
ward_sf <- lsoa_sf2 %>%
  group_by(wd24cd) %>%
  summarise(
    lad24cd = first(lad24cd),
    geometry = sf::st_union(geometry),
    .groups = "drop"
  ) %>%
  sf::st_make_valid()

# -------------------------
# 6) Join summary to geometry
# -------------------------
ward_map_df <- ward_sf %>%
  left_join(ward_summary, by = "wd24cd")

# Save geo-package for reuse
sf::st_write(ward_map_df, file.path(OUT_SUMMARY_DIR, "ward_summary.gpkg"),
             delete_dsn = TRUE, quiet = TRUE)

# -------------------------
# 7) Mapping utilities
# -------------------------
save_choropleth <- function(sf_df, value_col, title, out_png, subtitle = NULL) {
  p <- ggplot(sf_df) +
    geom_sf(aes(fill = .data[[value_col]]), colour = NA) +
    labs(title = title, subtitle = subtitle, fill = value_col) +
    theme_minimal() +
    theme(
      plot.background  = element_rect(fill = "white", colour = NA),
      panel.background = element_rect(fill = "white", colour = NA)
    )
  ggsave(out_png, p, width = 8, height = 8, dpi = 300, bg = "white")
}

# CPI maps
save_choropleth(
  ward_map_df,
  value_col = "CPI_0_100",
  title = "CPI (0–100), Greater London wards",
  subtitle = "CPI = mean of domain z-scores, rescaled to 0–100",
  out_png = file.path(OUT_SUMMARY_DIR, "map_CPI_0_100.png")
)

save_choropleth(
  ward_map_df,
  value_col = "CPI_z",
  title = "CPI (z-score), Greater London wards",
  subtitle = "Mean of domain z-scores",
  out_png = file.path(OUT_SUMMARY_DIR, "map_CPI_z.png")
)

# Domain maps (one per domain)
dom_value_cols <- grep("^DOM__", names(ward_map_df), value = TRUE)
for (v in dom_value_cols) {
  dom_name <- sub("^DOM__", "", v)
  save_choropleth(
    ward_map_df,
    value_col = v,
    title = paste0("Domain mean: ", dom_name, " (Greater London wards)"),
    subtitle = "Mean of outcomes within domain",
    out_png = file.path(OUT_SUMMARY_DIR, paste0("map_", v, ".png"))
  )
}

message("DONE.")
message("Outputs written to: ", OUT_SUMMARY_DIR)
message(" - ward_summary_outcomes_domains_cpi.csv")
message(" - ward_summary.gpkg")
message(" - map_CPI_0_100.png, map_CPI_z.png, and map_DOM__*.png")


# -------------------------
# 8) Clustering on 5 domains + "where to cut" + mapping with borough borders
# -------------------------
suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(sf)
  library(ggplot2)
  library(cluster)      # silhouette
})

# ---- Select the 5 domain columns you want to use
dom_value_cols <- grep("^DOM__", names(ward_map_df), value = TRUE)

# Drop "unknown" domain if present
dom_value_cols <- dom_value_cols[!grepl("^DOM__unknown$", dom_value_cols, ignore.case = TRUE)]

# Optionally force order:
# dom_value_cols <- c("DOM__Foundations", "DOM__Opportunities",
#                     "DOM__Power", "DOM__Belonging", "DOM__Health")

stopifnot(length(dom_value_cols) == 5)

# ---- Build matrix + standardize (clustering based on profile shape)
X <- ward_map_df %>%
  sf::st_drop_geometry() %>%
  select(wd24cd, all_of(dom_value_cols)) %>%
  mutate(across(all_of(dom_value_cols), as.numeric))

# Complete cases only for clustering
X_cc <- X %>% filter(if_all(all_of(dom_value_cols), ~ is.finite(.x)))

X_mat <- as.matrix(X_cc[, dom_value_cols])
rownames(X_mat) <- X_cc$wd24cd

# Global standardization (across all wards)
X_scaled <- scale(X_mat)

# ---- Distance + hierarchical clustering (Ward.D2)
d  <- dist(X_scaled, method = "euclidean")
hc <- hclust(d, method = "ward.D2")

# Save dendrogram
png(file.path(OUT_SUMMARY_DIR, "cluster_dendrogram_wardD2.png"),
    width = 1400, height = 900, res = 160)
plot(hc, labels = FALSE,
     main = "Ward clustering on 5 domains (Ward.D2)",
     xlab = "", sub = "")
dev.off()

# ---- Decide k via silhouette (k = 2..10)
k_grid <- 2:10
sil_tbl <- purrr::map_dfr(k_grid, function(k) {
  cl  <- cutree(hc, k = k)
  sil <- cluster::silhouette(cl, d)
  tibble(k = k, silhouette_avg = mean(sil[, "sil_width"]))
})

readr::write_csv(
  sil_tbl,
  file.path(OUT_SUMMARY_DIR, "cluster_silhouette_by_k.csv")
)

p_sil <- ggplot(sil_tbl, aes(k, silhouette_avg)) +
  geom_line() + geom_point() +
  labs(
    title = "Silhouette vs number of clusters (k)",
    subtitle = "Ward clustering on standardized 5-domain profiles",
    x = "k", y = "Average silhouette"
  ) +
  theme_minimal()

ggsave(
  file.path(OUT_SUMMARY_DIR, "cluster_silhouette_by_k.png"),
  p_sil, width = 7, height = 4, dpi = 200, bg = "white"
)

k_best <- sil_tbl$k[which.max(sil_tbl$silhouette_avg)]
message("Best k by silhouette: ", k_best)

# ---- Assign cluster labels (complete-case wards only)
cl_best <- cutree(hc, k = k_best)
cl_df <- tibble(
  wd24cd = names(cl_best),
  cluster = as.integer(cl_best)
)

# Defensive join: remove any existing cluster column first
ward_map_df <- ward_map_df %>%
  select(-any_of("cluster")) %>%
  left_join(cl_df, by = "wd24cd") %>%
  mutate(cluster = factor(cluster))

# ---- Characterise clusters (GLOBAL z-score means by domain)
X_scaled_df <- as_tibble(X_scaled, rownames = "wd24cd") %>%
  mutate(cluster = factor(cl_best[wd24cd]))

prof_tbl <- X_scaled_df %>%
  group_by(cluster) %>%
  summarise(
    n_wards = dplyr::n(),
    across(
      all_of(dom_value_cols),
      \(x) mean(x, na.rm = TRUE),
      .names = "{.col}__zmean"
    ),
    .groups = "drop"
  )

readr::write_csv(
  prof_tbl,
  file.path(OUT_SUMMARY_DIR, "cluster_profiles_zmean.csv")
)

# Long format for plotting
prof_long <- prof_tbl %>%
  pivot_longer(
    cols = ends_with("__zmean"),
    names_to = "domain",
    values_to = "zmean"
  ) %>%
  mutate(domain = gsub("^DOM__|__zmean$", "", domain))

p_prof <- ggplot(
  prof_long,
  aes(x = domain, y = zmean, group = cluster, colour = cluster)
) +
  geom_line() +
  geom_point() +
  labs(
    title = "Cluster profiles (mean z-score by domain)",
    subtitle = paste0("k = ", k_best, " clusters; values relative to London-wide mean"),
    x = "", y = "Mean z-score (global)"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 20, hjust = 1))

ggsave(
  file.path(OUT_SUMMARY_DIR, paste0("cluster_profiles_k", k_best, ".png")),
  p_prof, width = 9, height = 4.5, dpi = 200, bg = "white"
)

# ---- Borough (LAD) boundaries
borough_sf <- ward_sf %>%
  group_by(lad24cd) %>%
  summarise(geometry = sf::st_union(geometry), .groups = "drop") %>%
  sf::st_make_valid()

# ---- Map: wards coloured by cluster + borough borders
p_cluster_map <- ggplot() +
  geom_sf(data = ward_map_df, aes(fill = cluster), colour = NA) +
  geom_sf(data = borough_sf, fill = NA, colour = "white", linewidth = 0.4) +
  labs(
    title = paste0("Ward clusters from 5 domains (k = ", k_best, ")"),
    subtitle = "Wards coloured by cluster; borough (LAD) boundaries in white",
    fill = "Cluster"
  ) +
  theme_minimal() +
  theme(
    plot.background  = element_rect(fill = "white", colour = NA),
    panel.background = element_rect(fill = "white", colour = NA)
  )

ggsave(
  file.path(OUT_SUMMARY_DIR, paste0("map_clusters_k", k_best, ".png")),
  p_cluster_map, width = 8.8, height = 8.8, dpi = 300, bg = "white"
)

message("DONE CLUSTERING.")
message("Outputs written to: ", OUT_SUMMARY_DIR)
message(" - cluster_dendrogram_wardD2.png")
message(" - cluster_silhouette_by_k.csv / .png")
message(" - cluster_profiles_zmean.csv / cluster_profiles_k*.png")
message(" - map_clusters_k*.png")