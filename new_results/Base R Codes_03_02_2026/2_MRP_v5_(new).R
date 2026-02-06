############################################################
# MRP pipeline (Nomis Census 2021 CSV wide tables)
# - Robust header-skip + safe fallbacks
# - Stable brms fit (high iterations, strong control)
# - Poststratification (ward-level posterior draws) [RAM-safe]
# - Diagnostics (Rhat, ESS, divergences, MCSE stability)
# - London-only ward map (white background) saved to results/poststrat
#
# EXTENDED:
# - Adds 2 extra poststratified groups:
#   (1) econ3: active / student / inactive  (from TS066)
#   (2) edu2 : degree_plus / non_degree     (from TS067)
#
# IMPORTANT FIX:
# - employment_status is coded numerically (1..15, 97, and negatives for missing/refusal/dk)
#   so econ3 MUST be derived from numeric codes (NOT string matching).
#
# METHODOLOGICAL FIX (ROBUST):
# - Poststrat cell weights are constructed via IPF (iterative proportional fitting),
#   so the joint distribution matches ALL ward-level marginals exactly (age, sex, econ3, edu2).
# - Seed joint is taken from microdata joint distribution (London-only), with small smoothing.
#
# AGE CONSISTENCY FIX:
# - TS066 and TS067 are 16+; we therefore use AGE BANDS FOR 16+ only.
#
# SAFETY:
# - Any predictor with <2 observed levels in dty is automatically dropped from the model
#   (prevents "contrasts can be applied only to factors with 2 or more levels").
#
# LONDON FIX:
# - Poststratification grid is built ONLY for Greater London wards (LAD codes start with E09)
#   *before* prediction, to avoid massive slowdowns and RAM issues.
#
# MULTI-OUTCOME:
# - Fits and poststratifies a separate MRP model for EACH subdomain outcome column.
#   Default: all columns starting with "SUBD__"
#
# OUTPUTS (per outcome):
# - results/poststrat/<OUTCOME>/ward_mrp_estimates.csv
# - results/poststrat/<OUTCOME>/ward_mrp_stability_mcse.csv
# - results/poststrat/<OUTCOME>/fit_diagnostics.txt
# - mrp_outputs/<OUTCOME>/mrp_brms_fit_stable.rds
############################################################

# =========================
# 0) Packages
# =========================
pkgs <- c(
  "data.table","dplyr","stringr","purrr","tidyr","forcats",
  "brms","posterior","ggplot2","tibble","readr","sf"
)
invisible(lapply(pkgs, function(p) if (!requireNamespace(p, quietly = TRUE)) install.packages(p)))
invisible(lapply(pkgs, library, character.only = TRUE))

options(mc.cores = max(2, parallel::detectCores() - 1))
set.seed(123)

# =========================
# 1) Paths
# =========================
DT_PATH    <- "results/individual/person_master_all.csv.gz"
CENSUS_DIR <- "census_data"
OUT_DIR    <- "mrp_outputs"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

lsoa_shp_path     <- "data/shp/LSOA_2021_EW_BSC_V4.shp"
lsoa_to_wd_lad_fp <- "data/shp/LSOA_(2021)_to_Electoral_Ward_(2024)_to_LAD_(2024)_Best_Fit_Lookup_in_EW.csv"

POST_DIR <- file.path("results", "poststrat")
dir.create(POST_DIR, recursive = TRUE, showWarnings = FALSE)

# =========================
# 2) Helpers
# =========================
`%||%` <- function(x, y) if (!is.null(x)) x else y

std_names <- function(x) {
  x %>%
    tolower() %>%
    gsub("\ufeff", "", ., fixed = TRUE) %>%
    gsub("\\s+", "_", .) %>%
    gsub("[^a-z0-9_]+", "", .)
}

detect_csv_header_row <- function(path, pattern = "mnemonic", n_max = 500) {
  lines <- readLines(path, n = n_max, warn = FALSE, encoding = "UTF-8")
  lines <- sub("^\ufeff", "", lines)
  hit <- which(grepl(pattern, tolower(lines), fixed = TRUE))[1]
  if (is.na(hit)) {
    hit <- which(grepl("ward", tolower(lines)) & grepl("mnemonic", tolower(lines)))[1]
  }
  if (is.na(hit)) stop("Could not find a header row containing '", pattern, "' in: ", path)
  hit
}

read_nomis_wide_csv <- function(path) {
  header_line <- detect_csv_header_row(path, pattern = "mnemonic", n_max = 500)
  df <- data.table::fread(
    path,
    skip = header_line - 1,
    sep = ",",
    quote = "\"",
    fill = TRUE,
    blank.lines.skip = TRUE,
    showProgress = FALSE,
    encoding = "UTF-8"
  )
  df <- as.data.frame(df)
  names(df) <- std_names(names(df))
  df <- df %>% dplyr::select(where(~ !all(is.na(.x))))
  df
}

find_ward_code_col <- function(df) {
  nms <- names(df)
  if ("mnemonic" %in% nms) return("mnemonic")
  hit <- nms[which(sapply(df, function(x) any(stringr::str_detect(as.character(x), "^E05"))))[1]]
  if (length(hit) == 0 || is.na(hit)) stop("Cannot find ward code column (mnemonic / E05* codes).")
  hit
}

wide_to_marginal <- function(df_wide, ward_code_col = "mnemonic", drop_cols = NULL) {
  drop_cols <- drop_cols %||% character(0)
  
  id_cols <- unique(c(ward_code_col))
  if (names(df_wide)[1] != ward_code_col) id_cols <- unique(c(names(df_wide)[1], ward_code_col))
  id_cols <- id_cols[id_cols %in% names(df_wide)]
  
  keep <- setdiff(names(df_wide), drop_cols)
  
  df_wide %>%
    dplyr::select(all_of(intersect(keep, names(df_wide)))) %>%
    tidyr::pivot_longer(
      cols = -all_of(id_cols),
      names_to = "category",
      values_to = "value"
    ) %>%
    dplyr::transmute(
      wd24cd   = as.character(.data[[ward_code_col]]),
      category = as.character(category),
      value    = suppressWarnings(as.numeric(value))
    ) %>%
    dplyr::filter(!is.na(wd24cd), !is.na(category), !is.na(value))
}

safe_read_and_melt_csv <- function(path) {
  tryCatch({
    dfw <- read_nomis_wide_csv(path)
    ward_code_col <- find_ward_code_col(dfw)
    dfw[[ward_code_col]] <- as.character(dfw[[ward_code_col]])
    dfw <- dfw %>% dplyr::filter(!is.na(.data[[ward_code_col]]), .data[[ward_code_col]] != "")
    marg <- wide_to_marginal(dfw, ward_code_col = ward_code_col)
    list(wide = dfw, ward_code_col = ward_code_col, marg = marg)
  }, error = function(e) {
    message("FAILED reading: ", basename(path), "\n", conditionMessage(e))
    NULL
  })
}

parse_age_from_ts007_col <- function(cat) {
  x <- tolower(cat)
  x <- gsub("_", " ", x)
  if (grepl("under\\s*1", x)) return(0L)
  n <- suppressWarnings(as.integer(stringr::str_extract(x, "\\b[0-9]{1,3}\\b")))
  if (!is.na(n)) return(n)
  NA_integer_
}

make_age_band_16plus <- function(age_single_year) {
  cut(
    age_single_year,
    breaks = c(15, 24, 34, 44, 54, 64, 74, Inf),
    labels = c("16-24","25-34","35-44","45-54","55-64","65-74","75+"),
    right = TRUE
  )
}

dump_categories <- function(marg, fname) {
  if (is.null(marg)) return(invisible(NULL))
  cats <- sort(unique(marg$category))
  writeLines(cats, file.path(OUT_DIR, fname))
  invisible(cats)
}

harmonise_levels <- function(marg, var, dt_levels, recode_map = NULL) {
  if (is.null(marg)) return(NULL)
  x <- marg
  if (!is.null(recode_map)) {
    x$category <- dplyr::recode(x$category, !!!recode_map, .default = x$category)
  }
  x <- x %>% dplyr::filter(category %in% dt_levels)
  if (nrow(x) == 0) {
    message(sprintf("[%s] Harmonisation produced 0 rows (no overlap).", var))
    return(NULL)
  }
  chk <- x %>% dplyr::count(wd24cd) %>% dplyr::summarise(min_n = min(n), max_n = max(n))
  message(sprintf("[%s] categories per ward after harmonisation: min=%s max=%s", var, chk$min_n, chk$max_n))
  x
}

keep_if_2plus_levels <- function(df, vars) {
  kept <- character(0)
  dropped <- character(0)
  
  for (v in vars) {
    if (!v %in% names(df)) {
      dropped <- c(dropped, v)
      next
    }
    x <- droplevels(df[[v]])
    if (nlevels(x) >= 2) kept <- c(kept, v) else dropped <- c(dropped, v)
  }
  list(kept = kept, dropped = dropped)
}

safe_div <- function(num, den) {
  out <- num / den
  out[!is.finite(out)] <- 0
  out
}

# IPF on a 4D array
ipf_4d <- function(seed_arr, target_list, max_iter = 200, tol = 1e-8) {
  x <- seed_arr
  
  A <- dim(x)[1]; S <- dim(x)[2]; E <- dim(x)[3]; Q <- dim(x)[4]
  ta <- as.numeric(target_list$age);  stopifnot(length(ta) == A)
  ts <- as.numeric(target_list$sex);  stopifnot(length(ts) == S)
  te <- as.numeric(target_list$econ); stopifnot(length(te) == E)
  tq <- as.numeric(target_list$edu);  stopifnot(length(tq) == Q)
  
  total_target <- mean(c(sum(ta), sum(ts), sum(te), sum(tq)))
  if (!is.finite(total_target) || total_target <= 0) return(NULL)
  
  # Force the four marginals to be on the same total scale (numerically stable).
  ta <- ta * (total_target / max(sum(ta), 1))
  ts <- ts * (total_target / max(sum(ts), 1))
  te <- te * (total_target / max(sum(te), 1))
  tq <- tq * (total_target / max(sum(tq), 1))
  
  for (it in seq_len(max_iter)) {
    marg_a <- apply(x, 1, sum)
    fa <- safe_div(ta, pmax(marg_a, 1e-30))
    for (i in seq_len(A)) x[i,,,] <- x[i,,,] * fa[i]
    
    marg_s <- apply(x, 2, sum)
    fs <- safe_div(ts, pmax(marg_s, 1e-30))
    for (j in seq_len(S)) x[,j,,] <- x[,j,,] * fs[j]
    
    marg_e <- apply(x, 3, sum)
    fe <- safe_div(te, pmax(marg_e, 1e-30))
    for (k in seq_len(E)) x[,,k,] <- x[,,k,] * fe[k]
    
    marg_q <- apply(x, 4, sum)
    fq <- safe_div(tq, pmax(marg_q, 1e-30))
    for (m in seq_len(Q)) x[,,,m] <- x[,,,m] * fq[m]
    
    da <- max(abs(apply(x, 1, sum) - ta) / pmax(ta, 1), na.rm = TRUE)
    ds <- max(abs(apply(x, 2, sum) - ts) / pmax(ts, 1), na.rm = TRUE)
    de <- max(abs(apply(x, 3, sum) - te) / pmax(te, 1), na.rm = TRUE)
    dq <- max(abs(apply(x, 4, sum) - tq) / pmax(tq, 1), na.rm = TRUE)
    dmax <- max(c(da, ds, de, dq), na.rm = TRUE)
    
    if (is.finite(dmax) && dmax < tol) break
  }
  
  x
}

complete_marg <- function(marg_df, ward, levels_vec) {
  v <- marg_df %>%
    dplyr::filter(wd24cd == ward) %>%
    dplyr::select(category, value)
  v$value[!is.finite(v$value) | is.na(v$value)] <- 0
  out <- rep(0, length(levels_vec)); names(out) <- levels_vec
  out[v$category] <- v$value
  out
}

# Robust draw subsample for prediction:
# returns integer row indices (1..ndraws(fit)) for slicing posterior_epred output
get_draw_idx_for_prediction <- function(fit, ndraws) {
  n_all <- posterior::ndraws(fit)
  if (!is.finite(n_all) || n_all <= 0) stop("ndraws(fit) is not positive.")
  ndraws <- as.integer(min(ndraws, n_all))
  if (ndraws < 1) stop("Requested ndraws < 1.")
  sort(sample.int(n_all, size = ndraws, replace = FALSE))
}

# =========================
# 3) Read microdata
# =========================
dt <- data.table::fread(DT_PATH)

stopifnot(all(c("wd24cd","lad24cd","age_band","sex_cat") %in% names(dt)))
if (!("employment_status" %in% names(dt))) stop("dt is missing column 'employment_status'.")
if (!("edu_cat" %in% names(dt))) stop("dt is missing column 'edu_cat'.")

outcome_vars <- grep("^SUBD__", names(dt), value = TRUE)
if (length(outcome_vars) == 0) stop("No outcome columns found matching '^SUBD__' in dt.")

# =========================
# TEST MODE
# =========================
TEST_MODE <- FALSE
TEST_N_OUTCOMES <- 1
if (TEST_MODE) {
  outcome_vars <- outcome_vars[seq_len(min(TEST_N_OUTCOMES, length(outcome_vars)))]
  message("TEST_MODE=TRUE -> running only: ", paste(outcome_vars, collapse = ", "))
}

# Basic cleaning for predictors
dt <- dt %>%
  dplyr::mutate(
    wd24cd  = as.character(wd24cd),
    lad24cd = as.character(lad24cd),
    age_band = as.factor(age_band),
    sex_cat  = as.factor(sex_cat)
  ) %>%
  dplyr::filter(
    !is.na(wd24cd), !is.na(lad24cd),
    !is.na(age_band), !is.na(sex_cat),
    as.character(age_band) != "", as.character(sex_cat) != ""
  ) %>%
  dplyr::mutate(
    age_band = droplevels(age_band),
    sex_cat  = droplevels(sex_cat)
  )

# econ3 from numeric employment codes (IMPORTANT FIX)
dt <- dt %>%
  dplyr::mutate(
    emp_code = suppressWarnings(as.integer(as.character(employment_status))),
    emp_code = ifelse(emp_code %in% c(-9L, -2L, -1L), NA_integer_, emp_code),
    econ3 = dplyr::case_when(
      is.na(emp_code) ~ NA_character_,
      emp_code == 7L  ~ "student",
      emp_code %in% c(1L,2L,3L,5L,9L,10L,11L,12L,13L,14L,15L) ~ "active",
      emp_code %in% c(4L,6L,8L,97L) ~ "inactive",
      TRUE ~ NA_character_
    ),
    econ3 = factor(econ3, levels = c("active","student","inactive"))
  )

# edu2 (simple split; adjust if you have a better mapping)
dt <- dt %>%
  dplyr::mutate(
    edu2 = dplyr::case_when(
      stringr::str_detect(tolower(as.character(edu_cat)),
                          "level\\s*4|level_4|degree|higher|university|ba|bsc|msc|phd") ~ "degree_plus",
      TRUE ~ "non_degree"
    ),
    edu2 = factor(edu2, levels = c("non_degree","degree_plus"))
  )

# =========================
# 4) Read Nomis Census wide CSVs
# =========================
census_files <- list(
  age  = file.path(CENSUS_DIR, "TS007.csv"),
  sex  = file.path(CENSUS_DIR, "TS008.csv"),
  econ = file.path(CENSUS_DIR, "TS066.csv"),
  educ = file.path(CENSUS_DIR, "TS067.csv")
)

missing_files <- names(census_files)[!file.exists(unlist(census_files))]
if (length(missing_files) > 0) stop("Missing required census files: ", paste(missing_files, collapse = ", "))

age_obj  <- safe_read_and_melt_csv(census_files$age)
sex_obj  <- safe_read_and_melt_csv(census_files$sex)
econ_obj <- safe_read_and_melt_csv(census_files$econ)
educ_obj <- safe_read_and_melt_csv(census_files$educ)

m_age_raw  <- age_obj$marg  %||% NULL
m_sex_raw  <- sex_obj$marg  %||% NULL
m_econ_raw <- econ_obj$marg %||% NULL
m_educ_raw <- educ_obj$marg %||% NULL

dump_categories(m_age_raw,  "nomis_categories_TS007_age_cols.txt")
dump_categories(m_sex_raw,  "nomis_categories_TS008_sex_cols.txt")
dump_categories(m_econ_raw, "nomis_categories_TS066_econ_cols.txt")
dump_categories(m_educ_raw, "nomis_categories_TS067_educ_cols.txt")

# =========================
# 4b) London ward universe (from lookup)
# =========================
lookup_lsoa <- readr::read_csv(lsoa_to_wd_lad_fp, show_col_types = FALSE)
names(lookup_lsoa) <- std_names(names(lookup_lsoa))

wd_lookup_col  <- intersect(names(lookup_lsoa), c("wd24cd","ward24cd","electoral_ward_2024_code","ew24cd"))
lad_lookup_col <- intersect(names(lookup_lsoa), c("lad24cd","lad_2024_code","local_authority_district_2024_code"))
stopifnot(length(wd_lookup_col) == 1, length(lad_lookup_col) == 1)
wd_lookup_col  <- wd_lookup_col[1]
lad_lookup_col <- lad_lookup_col[1]

ward_to_lad_all <- lookup_lsoa %>%
  dplyr::transmute(
    wd24cd  = as.character(.data[[wd_lookup_col]]),
    lad24cd = as.character(.data[[lad_lookup_col]])
  ) %>%
  dplyr::distinct() %>%
  dplyr::filter(!is.na(wd24cd), !is.na(lad24cd))

# Guardrail: each ward must map to exactly one LAD (in this lookup)
ward_lad_counts <- ward_to_lad_all %>% dplyr::count(wd24cd)
if (any(ward_lad_counts$n != 1)) {
  bad <- ward_lad_counts %>% dplyr::filter(n != 1) %>% dplyr::pull(wd24cd)
  stop("Lookup has wards mapping to multiple LADs (first few): ", paste(head(bad, 10), collapse = ", "))
}

london_wards <- ward_to_lad_all %>%
  dplyr::filter(grepl("^E09", lad24cd)) %>%
  dplyr::pull(wd24cd) %>%
  unique() %>%
  sort()

message("London ward universe size: ", length(london_wards))

# Restrict microdata to London universe (seed + model)
dt_london <- dt %>%
  dplyr::filter(wd24cd %in% london_wards, grepl("^E09", lad24cd)) %>%
  dplyr::mutate(
    # Ensure consistent age band definition (16+)
    age_band = dplyr::recode(as.character(age_band), "18-24" = "16-24"),
    age_band = ifelse(as.character(age_band) == "0-17", NA_character_, as.character(age_band)),
    age_band = factor(age_band, levels = c("16-24","25-34","35-44","45-54","55-64","65-74","75+")),
    sex_cat  = droplevels(sex_cat)
  ) %>%
  dplyr::filter(!is.na(age_band)) %>%
  dplyr::mutate(
    age_band = droplevels(age_band),
    sex_cat  = droplevels(sex_cat),
    econ3    = droplevels(econ3),
    edu2     = droplevels(edu2)
  )

# Keep only outcomes that exist in dt_london
outcome_vars <- intersect(outcome_vars, names(dt_london))
if (length(outcome_vars) == 0) stop("No SUBD__ outcomes remain after restricting to dt_london.")

# =========================
# 4c) Harmonise marginals (then restrict to London wards)
# =========================
sex_levels <- levels(dt_london$sex_cat)
m_sex <- harmonise_levels(m_sex_raw, "sex_cat", sex_levels, recode_map = NULL)

age_levels <- levels(droplevels(dt_london$age_band))
message("Age bands in dt_london: ", paste(age_levels, collapse = ", "))

m_age <- NULL
if (!is.null(m_age_raw)) {
  age_num <- vapply(m_age_raw$category, parse_age_from_ts007_col, integer(1))
  if (all(is.na(age_num))) stop("Could not parse ages from TS007 columns.")
  
  m_age <- m_age_raw %>%
    dplyr::mutate(age_num = age_num) %>%
    dplyr::filter(is.finite(age_num), !is.na(age_num), age_num >= 16) %>%
    dplyr::mutate(category = as.character(make_age_band_16plus(age_num))) %>%
    dplyr::filter(!is.na(category)) %>%
    dplyr::group_by(wd24cd, category) %>%
    dplyr::summarise(value = sum(value), .groups = "drop")
  
  extra_age <- setdiff(unique(m_age$category), age_levels)
  if (length(extra_age) > 0) {
    message("Dropping age bands not present in dt_london: ", paste(extra_age, collapse = ", "))
  }
  m_age <- m_age %>% dplyr::filter(category %in% age_levels)
  m_age <- harmonise_levels(m_age, "age_band", age_levels)
}

dt_london <- dt_london %>%
  dplyr::mutate(age_band = factor(age_band, levels = age_levels))

econ_levels <- levels(dt_london$econ3)
m_econ3 <- NULL
if (!is.null(m_econ_raw)) {
  m_econ3 <- m_econ_raw %>%
    dplyr::mutate(
      cat_std = std_names(category),
      econ3 = dplyr::case_when(
        cat_std == "economically_active_excluding_fulltime_students" ~ "active",
        cat_std == "economically_active_and_a_fulltime_student"      ~ "student",
        cat_std == "economically_inactive"                           ~ "inactive",
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::filter(!is.na(econ3)) %>%
    dplyr::group_by(wd24cd, econ3) %>%
    dplyr::summarise(value = sum(value), .groups = "drop") %>%
    dplyr::transmute(wd24cd = wd24cd, category = econ3, value = value)
  
  m_econ3 <- harmonise_levels(m_econ3, "econ3", econ_levels)
}

edu2_levels <- levels(dt_london$edu2)
m_edu2 <- NULL
if (!is.null(m_educ_raw)) {
  m_edu2 <- m_educ_raw %>%
    dplyr::mutate(
      edu2 = dplyr::case_when(
        str_detect(tolower(category), "level_4|level4|or_above|above") ~ "degree_plus",
        TRUE ~ "non_degree"
      )
    ) %>%
    dplyr::filter(!is.na(edu2)) %>%
    dplyr::group_by(wd24cd, edu2) %>%
    dplyr::summarise(value = sum(value), .groups = "drop") %>%
    dplyr::transmute(wd24cd = wd24cd, category = edu2, value = value)
  
  m_edu2 <- harmonise_levels(m_edu2, "edu2", edu2_levels)
}

stopifnot(!is.null(m_age), !is.null(m_sex), !is.null(m_econ3), !is.null(m_edu2))

# Restrict marginals to London wards
m_age   <- m_age   %>% dplyr::filter(wd24cd %in% london_wards)
m_sex   <- m_sex   %>% dplyr::filter(wd24cd %in% london_wards)
m_econ3 <- m_econ3 %>% dplyr::filter(wd24cd %in% london_wards)
m_edu2  <- m_edu2  %>% dplyr::filter(wd24cd %in% london_wards)

# =========================
# 5) Build IPF-based poststrat_cells (London only)
# =========================
ward_list <- Reduce(intersect, list(
  unique(m_age$wd24cd),
  unique(m_sex$wd24cd),
  unique(m_econ3$wd24cd),
  unique(m_edu2$wd24cd)
)) %>% sort()

seed_tab <- dt_london %>%
  dplyr::filter(!is.na(age_band), !is.na(sex_cat), !is.na(econ3), !is.na(edu2)) %>%
  dplyr::count(age_band, sex_cat, econ3, edu2, name = "n")

age_lv <- levels(dt_london$age_band)
sex_lv <- levels(dt_london$sex_cat)
eco_lv <- levels(dt_london$econ3)
edu_lv <- levels(dt_london$edu2)

seed_arr <- array(
  0,
  dim = c(length(age_lv), length(sex_lv), length(eco_lv), length(edu_lv)),
  dimnames = list(age_lv, sex_lv, eco_lv, edu_lv)
)

if (nrow(seed_tab) == 0) stop("Seed joint from dt_london is empty (age/sex/econ3/edu2).")

for (r in seq_len(nrow(seed_tab))) {
  i <- match(as.character(seed_tab$age_band[r]), age_lv)
  j <- match(as.character(seed_tab$sex_cat[r]), sex_lv)
  k <- match(as.character(seed_tab$econ3[r]), eco_lv)
  m <- match(as.character(seed_tab$edu2[r]), edu_lv)
  if (all(is.finite(c(i,j,k,m)))) seed_arr[i,j,k,m] <- seed_arr[i,j,k,m] + seed_tab$n[r]
}

# Small smoothing to avoid structural zeros in seed
seed_arr <- seed_arr + 1e-6

poststrat_cells <- purrr::map_dfr(ward_list, function(wd) {
  a <- complete_marg(m_age,   wd, age_lv)
  s <- complete_marg(m_sex,   wd, sex_lv)
  e <- complete_marg(m_econ3, wd, eco_lv)
  q <- complete_marg(m_edu2,  wd, edu_lv)
  
  if (sum(a) <= 0 || sum(s) <= 0 || sum(e) <= 0 || sum(q) <= 0) return(NULL)
  
  joint <- ipf_4d(
    seed_arr = seed_arr,
    target_list = list(age = a, sex = s, econ = e, edu = q),
    max_iter = 200,
    tol = 1e-8
  )
  if (is.null(joint)) return(NULL)
  
  grid <- expand.grid(
    age_band = age_lv,
    sex_cat  = sex_lv,
    econ3    = eco_lv,
    edu2     = edu_lv,
    stringsAsFactors = FALSE
  )
  grid$wd24cd <- wd
  
  N_vec <- numeric(nrow(grid))
  for (idx in seq_len(nrow(grid))) {
    i <- match(grid$age_band[idx], age_lv)
    j <- match(grid$sex_cat[idx],  sex_lv)
    k <- match(grid$econ3[idx],    eco_lv)
    m <- match(grid$edu2[idx],     edu_lv)
    N_vec[idx] <- joint[i,j,k,m]
  }
  
  grid$N <- as.numeric(N_vec)
  grid$N[!is.finite(grid$N) | is.na(grid$N)] <- 0
  grid$N <- pmax(grid$N, 0)
  grid
})

# Join LAD codes + enforce London LAD pattern
poststrat_cells <- poststrat_cells %>%
  dplyr::left_join(ward_to_lad_all, by = "wd24cd") %>%
  dplyr::filter(grepl("^E09", lad24cd)) %>%
  dplyr::mutate(
    age_band = factor(age_band, levels = age_lv),
    sex_cat  = factor(sex_cat,  levels = sex_lv),
    econ3    = factor(econ3,    levels = eco_lv),
    edu2     = factor(edu2,     levels = edu_lv),
    wd24cd   = as.character(wd24cd),
    lad24cd  = as.character(lad24cd),
    N        = as.numeric(N)
  )

stopifnot(all(is.finite(poststrat_cells$N)), all(poststrat_cells$N >= 0))

message("Poststrat cells built with IPF.")
message("London wards in poststrat: ", length(unique(poststrat_cells$wd24cd)))
message("Total poststrat weight (sum N): ", signif(sum(poststrat_cells$N), 6))

# =========================
# 6) Fit + poststrat for each subdomain outcome
# =========================
DO_MAP <- FALSE   # set TRUE only if you want maps

candidate_vars <- c("age_band","sex_cat","econ3","edu2")

# =========================
# 7) Geometry inputs (optional map)
# =========================
lsoa_sf <- NULL
ward_sf <- NULL
if (DO_MAP) {
  lsoa_sf <- st_read(lsoa_shp_path, quiet = TRUE)
  names(lsoa_sf) <- std_names(names(lsoa_sf))
  
  lsoa_code_col <- intersect(names(lsoa_sf), c("lsoa21cd","lsoa11cd","lsoa_code"))
  stopifnot(length(lsoa_code_col) == 1)
  lsoa_code_col <- lsoa_code_col[1]
  
  lsoa_lookup_col <- intersect(names(lookup_lsoa), c("lsoa21cd","lsoa11cd","lsoa_code"))
  stopifnot(length(lsoa_lookup_col) == 1)
  lsoa_lookup_col <- lsoa_lookup_col[1]
  
  lookup2 <- lookup_lsoa %>%
    dplyr::transmute(
      lsoa_code = as.character(.data[[lsoa_lookup_col]]),
      wd24cd    = as.character(.data[[wd_lookup_col]]),
      lad24cd   = as.character(.data[[lad_lookup_col]])
    ) %>%
    dplyr::distinct()
  
  lsoa_sf2 <- lsoa_sf %>%
    dplyr::mutate(lsoa_code = as.character(.data[[lsoa_code_col]])) %>%
    dplyr::left_join(lookup2, by = "lsoa_code") %>%
    dplyr::filter(!is.na(wd24cd), !is.na(lad24cd), grepl("^E09", lad24cd))
  
  ward_sf <- lsoa_sf2 %>%
    dplyr::group_by(wd24cd) %>%
    dplyr::summarise(
      lad24cd  = dplyr::first(lad24cd),
      geometry = sf::st_union(geometry),
      .groups = "drop"
    ) %>%
    sf::st_make_valid()
}

# =========================
# 8) Main loop over outcomes
# =========================
for (outcome in outcome_vars) {
  
  message("\n============================================================")
  message("OUTCOME: ", outcome)
  message("============================================================")
  
  OUT_DIR_Y <- file.path(OUT_DIR, outcome)
  dir.create(OUT_DIR_Y, showWarnings = FALSE, recursive = TRUE)
  
  POST_DIR_Y <- file.path(POST_DIR, outcome)
  dir.create(POST_DIR_Y, showWarnings = FALSE, recursive = TRUE)
  
  if (!outcome %in% names(dt_london)) {
    message("Skipping (missing outcome column): ", outcome)
    next
  }
  
  # Build analysis dataset for this outcome
  dty <- dt_london %>%
    dplyr::mutate(y = suppressWarnings(as.numeric(.data[[outcome]]))) %>%
    dplyr::filter(!is.na(y)) %>%
    dplyr::select(y, wd24cd, lad24cd, all_of(candidate_vars)) %>%
    dplyr::mutate(across(all_of(candidate_vars), droplevels))
  
  if (nrow(dty) < 200) {
    message("Skipping (too few non-missing rows): n=", nrow(dty))
    next
  }
  
  # Drop predictors with <2 observed levels for this outcome
  lvl_check <- keep_if_2plus_levels(dty, candidate_vars)
  cell_vars_model <- lvl_check$kept
  
  if (length(lvl_check$dropped) > 0) {
    message("Dropping predictors with <2 observed levels in dty: ", paste(lvl_check$dropped, collapse = ", "))
  }
  if (length(cell_vars_model) < 2) {
    message("Skipping (need >=2 predictors after dropping).")
    next
  }
  
  message("Model predictors used: ", paste(cell_vars_model, collapse = ", "))
  
  # Ensure grouping variables are factors (needed for stable nesting and prediction)
  dty <- dty %>%
    tidyr::drop_na() %>%
    dplyr::mutate(
      lad24cd = factor(lad24cd),
      wd24cd  = factor(wd24cd)
    ) %>%
    dplyr::mutate(across(all_of(candidate_vars), droplevels))
  
  rhs_terms <- paste(cell_vars_model, collapse = " + ")
  formula_mrp <- bf(as.formula(
    paste0("y ~ 1 + ", rhs_terms, " + (1 | lad24cd/wd24cd)")
  ))
  
  priors <- c(
    prior(normal(0, 5), class = "Intercept"),
    prior(normal(0, 1), class = "b"),
    prior(exponential(1), class = "sd", group = "lad24cd"),
    prior(exponential(4), class = "sd", group = "lad24cd:wd24cd"),
    prior(exponential(1), class = "sigma")
  )
  
  message("Default priors for this model (for debugging naming):")
  print(brms::default_prior(formula_mrp, data = dty, family = gaussian()))
  
  fit <- brm(
    formula  = formula_mrp,
    data     = dty,
    family   = gaussian(),
    prior    = priors,
    chains   = 4,
    iter = 8000,
    warmup = 4000,
    seed     = 123,
    control = list(adapt_delta = 0.999, max_treedepth = 16),
    backend  = "cmdstanr"
  )
  
  saveRDS(fit, file.path(OUT_DIR_Y, "mrp_brms_fit_stable.rds"))
  
  # -------------------------
  # Posterior draw subsample (do NOT mutate fit)
  # -------------------------
  DRAWS_USE <- min(8000, posterior::ndraws(fit))
  set.seed(123)
  draw_idx <- get_draw_idx_for_prediction(fit, DRAWS_USE)
  n_draws  <- length(draw_idx)
  
  stopifnot(n_draws == DRAWS_USE)
  stopifnot(all(draw_idx >= 1 & draw_idx <= posterior::ndraws(fit)))
  stopifnot(n_draws >= 200)
  
  message("ndraws(fit)  = ", posterior::ndraws(fit))
  message("ndraws(used) = ", n_draws)
  
  # -------------------------
  # Build outcome-specific poststrat cells (predictor-level aligned)
  # - Keep only predictor levels present in training data
  # -------------------------
  fit_levels <- lapply(cell_vars_model, function(v) levels(dty[[v]]))
  names(fit_levels) <- cell_vars_model
  
  poststrat_cells_outcome <- poststrat_cells
  
  for (v in cell_vars_model) {
    poststrat_cells_outcome <- poststrat_cells_outcome %>%
      dplyr::filter(as.character(.data[[v]]) %in% fit_levels[[v]])
  }
  
  # For prediction, store training factor levels for LAD (used to anchor LAD)
  lad_levels_fit <- levels(dty$lad24cd)
  
  # =========================
  # 9) Diagnostics
  # =========================
  diag <- list(
    summary = summary(fit),
    max_rhat = max(brms::rhat(fit), na.rm = TRUE),
    min_neff_ratio = min(brms::neff_ratio(fit), na.rm = TRUE),
    n_divergent = sum(brms::nuts_params(fit)$Parameter == "divergent__" &
                        brms::nuts_params(fit)$Value == 1)
  )
  
  print(diag$summary)
  message("max Rhat: ", signif(diag$max_rhat, 4))
  message("min n_eff ratio: ", signif(diag$min_neff_ratio, 4))
  message("divergences: ", diag$n_divergent)
  
  writeLines(
    c(
      paste0("outcome=", outcome),
      paste0("n_rows=", nrow(dty)),
      paste0("max_rhat=", diag$max_rhat),
      paste0("min_neff_ratio=", diag$min_neff_ratio),
      paste0("n_divergent=", diag$n_divergent),
      paste0("model_predictors=", paste(cell_vars_model, collapse = ",")),
      paste0("poststrat_draws_used=", n_draws)
    ),
    file.path(POST_DIR_Y, "fit_diagnostics.txt")
  )
  
  # =========================
  # 10) Poststratification (London-only, RAM-safe)
  # =========================
  ward_levels <- sort(unique(poststrat_cells$wd24cd))  # full London universe from poststrat grid
  ward_draws  <- matrix(NA_real_, nrow = n_draws, ncol = length(ward_levels))
  colnames(ward_draws) <- ward_levels
  
  has_microdata <- ward_levels %in% as.character(unique(dty$wd24cd))
  names(has_microdata) <- ward_levels
  
  message("London wards in poststrat universe: ", length(ward_levels))
  message("London wards with microdata rows:    ", sum(has_microdata))
  message("Posterior draws used for poststrat:  ", n_draws)
  
  # Extrapolation control:
  # - TRUE  -> produce estimates for wards without microdata using sampled new random effects
  # - FALSE -> leave unsupported wards as NA (strict no-extrapolation)
  EXTRAPOLATE_ALL_WARDS <- TRUE
  
  message("Extrapolate to wards without microdata: ", EXTRAPOLATE_ALL_WARDS)
  
  message("Computing ward-level draws (London-only, ward-by-ward)...")
  pb <- utils::txtProgressBar(min = 0, max = length(ward_levels), style = 3)
  
  for (j in seq_along(ward_levels)) {
    g <- ward_levels[j]
    
    # Extract poststrat cells for this ward (may be empty if IPF failed for that ward)
    sub <- poststrat_cells_outcome[poststrat_cells_outcome$wd24cd == g, , drop = FALSE]
    if (nrow(sub) == 0) {
      ward_draws[, j] <- NA_real_
      utils::setTxtProgressBar(pb, j)
      next
    }
    
    # Guardrail: a ward must map to exactly one LAD in poststrat cells
    if (length(unique(sub$lad24cd)) != 1) {
      stop("Ward ", g, " has multiple lad24cd in poststrat cells. Check ward_to_lad_all.")
    }
    
    # Keep required columns
    sub <- sub %>% dplyr::select(wd24cd, lad24cd, all_of(cell_vars_model), N)
    
    # Align predictor factor levels to training
    sub <- sub %>%
      dplyr::mutate(
        across(all_of(cell_vars_model), ~ factor(.x, levels = fit_levels[[cur_column()]]))
      )
    
    if (anyNA(sub[, cell_vars_model, drop = FALSE])) {
      stop("NA introduced in newdata after predictor level alignment for ward ", g,
           " (poststrat has levels not present in training data).")
    }
    
    # Normalize weights
    w <- as.numeric(sub$N)
    if (all(w <= 0 | is.na(w))) {
      ward_draws[, j] <- NA_real_
      utils::setTxtProgressBar(pb, j)
      next
    }
    
    denom <- sum(w)
    if (!is.finite(denom) || denom <= 0) {
      ward_draws[, j] <- NA_real_
      utils::setTxtProgressBar(pb, j)
      next
    }
    w_norm <- w / denom
    
    # Grouping variables handling:
    # - Anchor LAD to training to avoid accidental LAD extrapolation.
    # - Allow new ward levels when extrapolating (brms will sample new group-level effects).
    sub <- sub %>%
      dplyr::mutate(
        lad24cd = factor(as.character(lad24cd), levels = lad_levels_fit),
        wd24cd  = factor(as.character(wd24cd))
      )
    
    # If a ward maps to an LAD that is not in training, decide what to do:
    # - With London-only restrictions this should be rare, but we handle it safely.
    if (anyNA(sub$lad24cd)) {
      if (EXTRAPOLATE_ALL_WARDS) {
        # If LAD is unseen, we cannot anchor it; mark NA rather than erroring.
        ward_draws[, j] <- NA_real_
        utils::setTxtProgressBar(pb, j)
        next
      } else {
        stop("NA in lad24cd after LAD level alignment for ward ", g,
             " (LAD not present in training).")
      }
    }
    
    # Predict
    mu_g <- brms::posterior_epred(
      fit,
      newdata = sub,
      re_formula = NULL,
      allow_new_levels = EXTRAPOLATE_ALL_WARDS,
      sample_new_levels = if (EXTRAPOLATE_ALL_WARDS) "gaussian" else "none"
    )
    
    # Subsample posterior draws
    mu_g <- mu_g[draw_idx, , drop = FALSE]
    
    # Handle possible 3D output
    if (length(dim(mu_g)) == 3) {
      mu_g <- mu_g[, , 1, drop = TRUE]
    }
    
    # Ensure correct orientation: draws x cells
    if (nrow(mu_g) != n_draws && ncol(mu_g) == n_draws) {
      mu_g <- t(mu_g)
    }
    
    if (nrow(mu_g) != n_draws || ncol(mu_g) != length(w_norm)) {
      stop(
        "Unexpected posterior_epred shape in ward ", g,
        ": dim(mu_g)=", paste(dim(mu_g), collapse="x"),
        " but expected ", n_draws, "x", length(w_norm)
      )
    }
    
    # Poststratify: weighted average across cells for each draw
    ward_draws[, j] <- rowSums(mu_g * rep(w_norm, each = n_draws))
    
    rm(mu_g); gc(FALSE)
    utils::setTxtProgressBar(pb, j)
  }
  close(pb)
  
  # Summarise ward-level posterior draws
  ward_N <- as.numeric(tapply(poststrat_cells$N, poststrat_cells$wd24cd, sum)[ward_levels])
  
  ward_est <- data.frame(
    wd24cd = ward_levels,
    mean   = colMeans(ward_draws, na.rm = TRUE),
    sd     = apply(ward_draws, 2, sd, na.rm = TRUE),
    q2.5   = apply(ward_draws, 2, quantile, probs = 0.025, na.rm = TRUE),
    q50    = apply(ward_draws, 2, quantile, probs = 0.50,  na.rm = TRUE),
    q97.5  = apply(ward_draws, 2, quantile, probs = 0.975, na.rm = TRUE),
    N      = ward_N,
    has_microdata = as.logical(has_microdata[ward_levels])
  )
  
  write.csv(ward_est, file.path(POST_DIR_Y, "ward_mrp_estimates.csv"), row.names = FALSE)
  
  # =========================
  # 11) Stability (MCSE)
  # =========================
  mcse_vec <- apply(ward_draws, 2, function(x) {
    if (all(is.na(x))) return(NA_real_)
    posterior::mcse_mean(x)
  })
  post_sd <- apply(ward_draws, 2, sd, na.rm = TRUE)
  
  # Robust rel_mcse (avoid Inf when post_sd == 0)
  rel_mcse <- ifelse(is.finite(post_sd) & post_sd > 0, mcse_vec / post_sd, NA_real_)
  
  stab <- data.frame(
    wd24cd   = ward_levels,
    mean     = colMeans(ward_draws, na.rm = TRUE),
    post_sd  = post_sd,
    mcse     = mcse_vec,
    rel_mcse = rel_mcse,
    has_microdata = as.logical(has_microdata[ward_levels])
  )
  
  write.csv(stab, file.path(POST_DIR_Y, "ward_mrp_stability_mcse.csv"), row.names = FALSE)
  message("MCSE stability summary (rel_mcse):")
  print(summary(stab$rel_mcse))
  
  # =========================
  # 12) Map (optional, per outcome)
  # =========================
  if (DO_MAP && !is.null(ward_sf)) {
    ward_map <- ward_sf %>% dplyr::left_join(ward_est, by = "wd24cd")
    bb <- sf::st_bbox(ward_map)
    
    p_london <- ggplot(ward_map) +
      geom_sf(aes(fill = mean), colour = NA) +
      coord_sf(
        xlim = c(bb["xmin"], bb["xmax"]),
        ylim = c(bb["ymin"], bb["ymax"]),
        datum = NA
      ) +
      labs(
        title = paste0("Poststratified outcome (Greater London wards): ", outcome),
        subtitle = if (EXTRAPOLATE_ALL_WARDS) {
          "Includes extrapolated wards (sampled new ward effects when microdata missing)."
        } else {
          "Wards without microdata support are left blank (NA)."
        },
        fill  = "Mean"
      ) +
      theme_minimal() +
      theme(
        plot.background  = element_rect(fill = "white", colour = NA),
        panel.background = element_rect(fill = "white", colour = NA)
      )
    
    ggsave(
      filename = file.path(POST_DIR_Y, "ward_poststrat_map_mean_london.png"),
      plot = p_london,
      width = 8, height = 8, dpi = 300,
      bg = "white"
    )
  }
  
  # =========================
  # 13) Reporting: which London wards had no microdata?
  # =========================
  missing_london_wards <- ward_levels[!has_microdata[ward_levels]]
  writeLines(missing_london_wards, file.path(POST_DIR_Y, "london_wards_without_microdata.txt"))
  
  message("Saved outputs to:")
  message(" - ", POST_DIR_Y)
  message(" - ", OUT_DIR_Y)
}

message("\nDONE. Multi-outcome MRP + IPF poststrat finished.")
message("Base poststrat cells used (IPF) were built once and reused for all outcomes.")