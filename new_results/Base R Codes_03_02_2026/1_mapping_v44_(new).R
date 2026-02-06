# =====================================================================
# UKHLS → Citizen Prosperity Index (CPI) at selectable geographies
# Supported levels: MSOA, WARD, LAD
#
# DEFAULTS (IMPORTANT):
#   - pooling_mode = "independent"   → independent pooling across waves (e.g., n/m/l)
#       * one row per pidp
#       * movers handled: each person counted once, in their latest available geography
#       * IMPORTANT FIX: robust hidp detection per wave + true per-person latest-nonmissing pooling for person vars
#   - pooling_mode = "latest"        → latest wave only (default wave = "n" if available)
#   - mapping logic is UNCHANGED (LSOA→WARD/LAD (+MSOA))
#   - composites are normalized to 0–10 BEFORE aggregation; then aggregation only
#   - outputs include prints + maps directly (no patch blocks)
# =====================================================================

# ----------------------------- STEP 0. CONFIG -------------------------

data_dir            <- "./data/ukhls"
id_dir              <- "./data/id"
lsoa_shp_path       <- "data/shp/LSOA_2021_EW_BSC_V4.shp"
lsoa_to_wd_lad_fp   <- "data/shp/LSOA_(2021)_to_Electoral_Ward_(2024)_to_LAD_(2024)_Best_Fit_Lookup_in_EW.csv"
lsoa_to_msoa_lad_fp <- "data/shp/PCD_OA21_LSOA21_MSOA21_LAD_FEB25_UK_LU.csv"

out_base_dir        <- "./results"

pooling_mode        <- "independent"   # "independent" or "latest"
pooling_waves       <- c("n","m","l")  # newest -> older (Wave 14/13/12)
latest_wave_default <- "n"

MIN_COVERAGE        <- 0.35

only_london         <- TRUE
aggregation_levels  <- c("WARD","LAD")

# --------------------------- STEP 1. PACKAGES -------------------------
suppressWarnings({
  pkgs <- c("data.table","stringr","tools","readr","dplyr","rstudioapi",
            "sf","janitor","stringi","ggplot2","viridis","readxl","ineq")
  to_install <- setdiff(pkgs, rownames(installed.packages()))
  if (length(to_install)) install.packages(to_install, quiet = TRUE)
})
library(data.table); library(stringr); library(tools); library(readr)
library(dplyr); library(rstudioapi); library(sf); library(janitor)
library(stringi); library(ggplot2); library(viridis); library(readxl)
library(ineq)

try(setwd(dirname(dirname(rstudioapi::getActiveDocumentContext()$path))), silent = TRUE)

# ----------------------- STEP 2. HELPERS ---------------------

load_wave_geo <- function(filename, vars = NULL) {
  # filename: e.g. "l_indresp", "l_hhresp", "m_hhresp"
  # vars: character vector of raw UKHLS vars to keep (optional)
  
  if (!exists("tables", envir = .GlobalEnv, inherits = TRUE)) {
    stop("load_wave_geo(): 'tables' not found in .GlobalEnv. Run STEP 3 (READ UKHLS TABLES) first.")
  }
  if (!exists("mapping_by_wave", envir = .GlobalEnv, inherits = TRUE)) {
    stop("load_wave_geo(): 'mapping_by_wave' not found in .GlobalEnv. Run STEP 5 (Build mapping_by_wave) first.")
  }
  
  tables_local <- get("tables", envir = .GlobalEnv)
  if (!filename %chin% names(tables_local)) {
    stop(sprintf("load_wave_geo(): table '%s' not found in 'tables'.", filename))
  }
  
  DT <- data.table::copy(tables_local[[filename]])
  data.table::setDT(DT)
  
  w <- tolower(substr(filename, 1, 1))
  map_all <- get("mapping_by_wave", envir = .GlobalEnv)
  map_w <- map_all[wave == w]
  
  if (is.null(map_w) || nrow(map_w) == 0) {
    stop(sprintf("load_wave_geo(): no mapping rows found for wave '%s'.", w))
  }
  
  # Identify the HIDP column (wave-specific) and standardize to n_hidp if present
  hid_col <- detect_hidp_col(DT, w)
  if (!is.na(hid_col) && hid_col %chin% names(DT) && hid_col != "n_hidp") {
    data.table::setnames(DT, hid_col, "n_hidp")
  }
  
  # Keep only requested vars + keys (pidp/hidp if available)
  keep_keys <- c("pidp", "n_hidp")
  keep_vars <- unique(c(vars, keep_keys))
  keep_vars <- keep_vars[keep_vars %chin% names(DT)]
  DT <- DT[, ..keep_vars]
  
  # Prepare mapping columns (always attach these)
  geo_cols <- intersect(c("pidp","n_hidp","lsoa21cd","wd24cd","lad24cd","msoa21cd"), names(map_w))
  map_w2 <- map_w[, ..geo_cols]
  
  # Join: prefer pidp-join if possible, else hidp-join
  if ("pidp" %chin% names(DT) && "pidp" %chin% names(map_w2)) {
    data.table::setkey(DT, pidp)
    data.table::setkey(map_w2, pidp)
    out <- merge(DT, map_w2, by = "pidp", all.x = TRUE, sort = FALSE)
  } else if ("n_hidp" %chin% names(DT) && "n_hidp" %chin% names(map_w2)) {
    data.table::setkey(DT, n_hidp)
    data.table::setkey(map_w2, n_hidp)
    out <- merge(DT, map_w2, by = "n_hidp", all.x = TRUE, sort = FALSE)
  } else {
    stop(sprintf(
      "load_wave_geo(): cannot join geo mapping for '%s' (need pidp or hidp).",
      filename
    ))
  }
  
  data.table::setDT(out)
  out
}

clean_header <- function(x){
  x <- stringi::stri_trans_general(x, "Latin-ASCII")
  x <- gsub("[^A-Za-z0-9]+","_",x)
  tolower(janitor::make_clean_names(x))
}

read_lookup_any <- function(path){
  ext <- tolower(tools::file_ext(path))
  if (ext %in% c("xlsx","xls")) {
    return(readxl::read_excel(path))
  } else {
    tr1 <- try(readr::read_csv(path, show_col_types=FALSE, guess_max=1e6,
                               locale=readr::locale(encoding="UTF-8")), silent=TRUE)
    if (!inherits(tr1,"try-error")) return(tr1)
    tr2 <- try(readr::read_csv(path, show_col_types=FALSE, guess_max=1e6,
                               locale=readr::locale(encoding="Latin1")), silent=TRUE)
    if (!inherits(tr2,"try-error")) return(tr2)
    readr::read_delim(path, delim=",", quote="\"", escape_double=TRUE,
                      show_col_types=FALSE, guess_max=1e6)
  }
}

minmax01 <- function(x) {
  rng <- range(x, na.rm = TRUE)
  if (!is.finite(rng[1]) || !is.finite(rng[2]) || rng[1] == rng[2]) {
    return(ifelse(is.na(x), NA_real_, 0.5))
  }
  (x - rng[1]) / (rng[2] - rng[1])
}

minmax0_10 <- function(x) {
  rng <- range(x, na.rm = TRUE)
  if (!is.finite(rng[1]) || !is.finite(rng[2]) || rng[1] == rng[2]) {
    return(ifelse(is.na(x), NA_real_, 5))
  }
  10 * (x - rng[1]) / (rng[2] - rng[1])
}

`%||%` <- function(a,b) if (!is.null(a) && length(a) > 0) a else b

collapse_by_pidp <- function(DT) {
  if (is.null(DT) || nrow(DT) == 0) return(NULL)
  DT <- DT[!is.na(pidp)]
  DT[, .(
    v = {
      vv <- suppressWarnings(as.numeric(v))
      if (all(is.na(vv))) NA_real_ else mean(vv, na.rm = TRUE)
    }
  ), by = pidp]
}

prefer_latest <- function(DT, key_cols, wave_col = "wave", wave_priority = c("n","m","l")) {
  if (!wave_col %in% names(DT)) return(unique(DT, by = key_cols))
  DT[, wave_rank := match(get(wave_col), wave_priority)]
  data.table::setorderv(DT, c(key_cols, "wave_rank"))
  out <- DT[!duplicated(DT[, ..key_cols])]
  out[, wave_rank := NULL]
  out
}

# --- NEW: robust wave HIDP detection + normalization ------------------

detect_hidp_col <- function(DT, wave_letter) {
  wave_letter <- tolower(wave_letter)
  candidates <- c(paste0(wave_letter, "_hidp"), "n_hidp", "hidp")
  candidates <- candidates[candidates %in% names(DT)]
  if (!length(candidates)) return(NA_character_)
  candidates[1]
}

detect_pidp_col <- function(DT) if ("pidp" %in% names(DT)) "pidp" else NA_character_

normalize_indresp_pidp_hidp <- function(DT, wave_letter) {
  wave_letter <- tolower(wave_letter)
  pid_col <- detect_pidp_col(DT)
  hid_col <- detect_hidp_col(DT, wave_letter)
  if (is.na(pid_col) || is.na(hid_col)) return(NULL)
  out <- unique(DT[!is.na(get(pid_col)) & !is.na(get(hid_col)),
                   .(pidp = get(pid_col), n_hidp = get(hid_col))])
  out[, wave := wave_letter]
  out
}

# --- NEW: per-pidp latest-NONMISSING pooling across waves (not “first table wins”) ----
pool_latest_nonmissing <- function(rows, wave_order = c("n","m","l")) {
  # rows: data.table with pidp, wave, v
  if (is.null(rows) || !nrow(rows)) return(NULL)
  rows <- rows[!is.na(pidp)]
  rows[, wave_rank := match(wave, wave_order)]
  setorder(rows, pidp, wave_rank)
  
  # pick first non-missing within pidp by wave order; if all missing keep first row
  out <- rows[, {
    ok <- which(is.finite(v))
    .SD[ if (length(ok)) ok[1] else 1 ]
  }, by = pidp]
  
  out[, c("wave_rank") := NULL]
  out[, wave := NULL]
  out[, .(pidp, v)]
}

# Transform registry
.transform_registry <- list(
  none = function(x, ...) x,
  flip = function(x, ...) ifelse(is.finite(x), -x, x),
  median_center = function(x, ...) { m <- stats::median(x, na.rm=TRUE); ifelse(is.finite(x), x - m, x) },
  median_ratio  = function(x, ...) { m <- stats::median(x, na.rm=TRUE); if (!is.finite(m) || m == 0) return(x); ifelse(is.finite(x), x / m, x) },
  median_absdev = function(x, ...) { m <- stats::median(x, na.rm=TRUE); ifelse(is.finite(x), abs(x - m), x) },
  log1p = function(x, ...) ifelse(is.finite(x) & x >= 0, log1p(x), NA_real_),
  zscore = function(x, ...) { m <- mean(x, na.rm=TRUE); s <- stats::sd(x, na.rm=TRUE); if (!is.finite(s) || s == 0) return(x); ifelse(is.finite(x), (x - m)/s, x) },
  rank01 = function(x, ...) { r <- rank(x, na.last="keep", ties.method="average"); ifelse(is.na(r), NA_real_, (r - 1) / (max(r, na.rm=TRUE) - 1)) },
  winsor = function(x, probs=c(0.01,0.99), ...) { q <- stats::quantile(x, probs=probs, na.rm=TRUE, names=FALSE); pmin(pmax(x, q[1]), q[2]) }
)

apply_transform <- function(x, transform=NULL, ...) {
  if (is.null(transform)) return(x)
  if (is.character(transform) || is.function(transform)) transform <- list(transform)
  if (is.list(transform)) {
    for (t in transform) {
      if (is.function(t)) {
        x <- t(x, ...)
      } else if (is.character(t) && length(t) == 1) {
        f <- .transform_registry[[t]]
        if (is.null(f)) warning(sprintf("Unknown transform '%s' — ignored.", t)) else x <- f(x, ...)
      } else {
        warning("Unsupported element in 'transform' — ignored.")
      }
    }
    return(x)
  }
  warning("Unsupported 'transform' argument — ignored.")
  x
}

# --------------------- STEP 3. READ UKHLS TABLES ---------------------

read_clean_tab <- function(fp) {
  DT <- fread(fp, sep = "\t", na.strings = c("", "NA"), showProgress = FALSE, fill = TRUE, quote = "")
  num_cols <- names(DT)[vapply(DT, is.numeric, logical(1))]
  if (length(num_cols)) {
    for (cn in num_cols) {
      v <- DT[[cn]]
      v[v < 0] <- NA_real_   # keep your original convention for negative numeric values
      set(DT, j = cn, value = v)
    }
  }
  DT
}

get_wave <- function(fp) tolower(substr(basename(fp), 1, 1))

tab_files_all <- list.files(data_dir, pattern = "^[a-z]_.*\\.tab$", full.names = TRUE)
stopifnot(length(tab_files_all) > 0)

if (pooling_mode == "independent") {
  wave_order <- pooling_waves
} else {
  wave_order <- latest_wave_default
}

tab_files <- tab_files_all[get_wave(tab_files_all) %in% wave_order]
stopifnot(length(tab_files) > 0)

tables <- setNames(lapply(tab_files, read_clean_tab),
                   nm = tools::file_path_sans_ext(basename(tab_files)))

# ----------------- STEP 4. PERSON CORE: pidp ↔ hidp (FIXED) -----------------

indresp_names <- names(tables)[grepl("^[a-z]_indresp$", names(tables))]
stopifnot(length(indresp_names) >= 1)

# FIX: do NOT require "n_hidp" to exist; detect <wave>_hidp / n_hidp / hidp
indresp_stack <- rbindlist(lapply(indresp_names, function(nm) {
  w <- tolower(substr(nm, 1, 1))
  DT <- tables[[nm]]
  normalize_indresp_pidp_hidp(DT, w)
}), use.names = TRUE, fill = TRUE)
stopifnot(nrow(indresp_stack) > 0)

# For household joins we keep a “latest hidp per pidp”, but NOTE:
# for true independent pooling of household vars we will use wave-specific joins (see pull_var_dt())
person_core <- prefer_latest(indresp_stack, key_cols = c("pidp"),
                             wave_col = "wave", wave_priority = wave_order)[, !"wave"]
setkey(person_core, pidp)

# ----------------- STEP 5. GEO LOOKUPS: hidp → LSOA → WARD/LAD (+MSOA) -----------------

id_files <- file.path(id_dir, paste0(wave_order, "_lsoa21_protect.tab"))
id_files <- id_files[file.exists(id_files)]
stopifnot(length(id_files) >= 1)

pick_col <- function(nm, w, base){
  patt_pref <- paste0("^", w, "_?", base, "$")
  c1 <- nm[grepl(patt_pref, nm, ignore.case=TRUE)][1]
  if (!is.na(c1)) return(c1)
  c2 <- nm[grepl(paste0("^", base, "$"), nm, ignore.case=TRUE)][1]
  if (!is.na(c2)) return(c2)
  if (base=="lsoa21"){
    c3 <- nm[grepl(paste0("^", w, "_?lsoa(20)?21$"), nm, ignore.case=TRUE)][1]
    if (!is.na(c3)) return(c3)
    c4 <- nm[grepl("^lsoa(20)?21$", nm, ignore.case=TRUE)][1]
    if (!is.na(c4)) return(c4)
  }
  NA_character_
}

idtab_stack <- rbindlist(lapply(seq_along(id_files), function(i) {
  fp <- id_files[i]
  w  <- get_wave(fp)
  DT <- fread(fp, sep = "\t", showProgress = FALSE, fill = TRUE)
  hid_col  <- pick_col(names(DT), w, "hidp")
  lsoa_col <- pick_col(names(DT), w, "lsoa21")
  if (is.na(hid_col) || is.na(lsoa_col)) return(NULL)
  out <- unique(DT[, .(
    n_hidp   = get(hid_col),
    lsoa21cd = toupper(trimws(as.character(get(lsoa_col))))
  )])
  out <- out[!is.na(n_hidp) & n_hidp != ""]
  out[, wave := w]
  out
}), use.names = TRUE, fill = TRUE)
stopifnot(nrow(idtab_stack) > 0)

idtab <- prefer_latest(idtab_stack, key_cols = c("n_hidp"),
                       wave_col = "wave", wave_priority = wave_order)[, !"wave"]
setkey(idtab, n_hidp)

lk_raw <- read_lookup_any(lsoa_to_wd_lad_fp)
names(lk_raw) <- clean_header(names(lk_raw))
cand_lsoa21 <- grep("(^|_)lsoa(_)?(20)?21(_)?(cd|code)$", names(lk_raw), value=TRUE)
cand_lad24  <- grep("(^|_)(lad|ltla)(24|_?2024)?(_)?(cd|code)$", names(lk_raw), value=TRUE)
cand_wd24   <- grep("(^|_)(ward|wd)(24|_?2024)?(_)?(cd|code)$", names(lk_raw), value=TRUE)
stopifnot(length(cand_lsoa21)>=1, length(cand_lad24)>=1)

lk_wd_lad <- lk_raw |>
  dplyr::transmute(
    lsoa21cd = toupper(trimws(.data[[cand_lsoa21[1]]])),
    wd24cd   = if (length(cand_wd24) >= 1) toupper(trimws(.data[[cand_wd24[1]]])) else NA_character_,
    lad24cd  = toupper(trimws(.data[[cand_lad24[1]]]))
  ) |>
  dplyr::distinct()

lk_lsoa_msoa <- NULL
if (!is.na(lsoa_to_msoa_lad_fp) && nzchar(lsoa_to_msoa_lad_fp) && file.exists(lsoa_to_msoa_lad_fp)) {
  lu_mraw <- read_lookup_any(lsoa_to_msoa_lad_fp)
  names(lu_mraw) <- clean_header(names(lu_mraw))
  cand_lsoa_c <- grep("(^|_)lsoa(_)?(20)?21(_)?(cd|code)$", names(lu_mraw), value = TRUE)
  cand_msoa_c <- grep("(^|_)(msoa|mssoa)(20)?21(_)?(cd|code)$", names(lu_mraw), value = TRUE)
  cand_lad_c  <- grep("(^|_)(lad|ltla)(24|_?2024)?(_)?(cd|code)$", names(lu_mraw), value = TRUE)
  
  if (length(cand_lsoa_c) >= 1 && length(cand_msoa_c) >= 1) {
    lk_lsoa_msoa <- lu_mraw |>
      dplyr::transmute(
        lsoa21cd = toupper(trimws(.data[[cand_lsoa_c[1]]])),
        msoa21cd = toupper(trimws(.data[[cand_msoa_c[1]]])),
        lad24cd  = if (length(cand_lad_c) >= 1) toupper(trimws(.data[[cand_lad_c[1]]])) else NA_character_
      ) |>
      dplyr::distinct() |>
      dplyr::filter(!is.na(lsoa21cd) & nzchar(lsoa21cd),
                    !is.na(msoa21cd) & nzchar(msoa21cd))
  }
}

# Build wave-specific mapping backbone: (pidp, wave, n_hidp, lsoa, ward/lad/msoa)
# FIX: wave-specific hidp detection inside indresp
build_mapping_by_wave <- function(wave_letter) {
  wave_letter <- tolower(wave_letter)
  
  nm_ind <- paste0(wave_letter, "_indresp")
  if (!nm_ind %in% names(tables)) return(NULL)
  DT <- tables[[nm_ind]]
  
  core <- normalize_indresp_pidp_hidp(DT, wave_letter)
  if (is.null(core) || !nrow(core)) return(NULL)
  core <- unique(core[, .(pidp, n_hidp, wave)])
  setkey(core, n_hidp)
  
  id_fp <- file.path(id_dir, sprintf("%s_lsoa21_protect.tab", wave_letter))
  if (!file.exists(id_fp)) return(NULL)
  id_raw <- fread(id_fp, sep = "\t", fill = TRUE, showProgress = FALSE)
  
  hid_col  <- pick_col(names(id_raw), wave_letter, "hidp")
  lsoa_col <- pick_col(names(id_raw), wave_letter, "lsoa21")
  if (is.na(hid_col) || is.na(lsoa_col)) return(NULL)
  
  id_wave <- unique(id_raw[, .(
    n_hidp   = get(hid_col),
    lsoa21cd = toupper(trimws(as.character(get(lsoa_col))))
  )])
  id_wave <- id_wave[!is.na(n_hidp) & n_hidp != ""]
  setkey(id_wave, n_hidp)
  
  map <- merge(core, id_wave, by = "n_hidp", all.x = TRUE, sort = FALSE)
  map <- merge(map, as.data.table(lk_wd_lad), by = "lsoa21cd", all.x = TRUE, sort = FALSE)
  
  if (!is.null(lk_lsoa_msoa)) {
    map <- merge(map, as.data.table(lk_lsoa_msoa), by = "lsoa21cd", all.x = TRUE, sort = FALSE)
    if ("lad24cd.x" %in% names(map) && "lad24cd.y" %in% names(map)) {
      map[, lad24cd := fifelse(is.na(lad24cd.x) | lad24cd.x=="", lad24cd.y, lad24cd.x)]
      map[, c("lad24cd.x","lad24cd.y") := NULL]
    }
  } else {
    map[, msoa21cd := NA_character_]
  }
  
  if (isTRUE(only_london) && "lad24cd" %in% names(map)) map <- map[grepl("^E090", lad24cd)]
  if ("lad24cd" %in% names(map)) map <- map[lad24cd != "E09000001"]
  
  map <- map[, .(pidp, wave, n_hidp, lsoa21cd, wd24cd, lad24cd, msoa21cd)]
  setkey(map, pidp, wave)
  map
}

mapping_by_wave <- rbindlist(lapply(wave_order, build_mapping_by_wave), use.names = TRUE, fill = TRUE)
stopifnot(nrow(mapping_by_wave) > 0)

# Independent pooling mapping: one row per pidp, latest available wave (wave_order is newest -> older)
wave_priority_dt <- data.table(wave = wave_order, pri = seq_along(wave_order))
mapping_by_wave2 <- merge(copy(mapping_by_wave), wave_priority_dt, by = "wave", all.x = TRUE)
setorder(mapping_by_wave2, pidp, pri)

mapping_independent <- mapping_by_wave2[
  order(pidp, pri)
][, {
  ok <- which(!is.na(wd24cd) & wd24cd != "")
  .SD[ if (length(ok)) ok[1] else 1 ]
}, by = pidp][, pri := NULL]
setkey(mapping_independent, pidp)

latest_wave <- if (latest_wave_default %in% wave_order) latest_wave_default else wave_order[1]
mapping_latest <- mapping_by_wave[wave == latest_wave]
setkey(mapping_latest, pidp)

persons_geo <- if (pooling_mode == "independent") copy(mapping_independent) else copy(mapping_latest)

# ---------------- STEP 6. COLUMN INDEX (availability check) ----------
col_index <- rbindlist(
  lapply(names(tables), function(nm) data.table(table = nm, var = names(tables[[nm]]))),
  use.names = TRUE
)
var_exists <- function(varname) varname %chin% col_index$var

# ---------------- STEP 7. VARIABLE PULL (FIXED for independent pooling) -----

# FIX: pull variables using per-wave values, then per-pidp latest-nonmissing across waves
pull_var_dt <- function(varname) {
  rows <- col_index[var == varname]
  if (nrow(rows) == 0) return(NULL)
  
  rows[, wave := tolower(substr(table, 1, 1))]
  rows <- rows[wave %chin% wave_order]
  if (nrow(rows) == 0) return(NULL)
  
  # prefer newest waves first
  rows[, wave_rank := match(wave, wave_order)]
  setorder(rows, wave_rank)
  
  # 1) Person-level tables (pidp present): collect per wave, then pool latest-nonmissing
  collected <- list()
  for (tb in rows$table) {
    DT <- tables[[tb]]
    if (("pidp" %in% names(DT)) && (varname %in% names(DT))) {
      w <- tolower(substr(tb, 1, 1))
      tmp <- unique(DT[, .(pidp, v = get(varname))])
      tmp <- collapse_by_pidp(tmp)
      tmp[, wave := w]
      collected[[length(collected) + 1]] <- tmp
    }
  }
  if (length(collected)) {
    allv <- rbindlist(collected, use.names = TRUE, fill = TRUE)
    return(pool_latest_nonmissing(allv[, .(pidp, wave, v)], wave_order = wave_order))
  }
  
  # 2) Household-level tables: wave-specific hidp join (critical for movers)
  collected_hh <- list()
  for (tb in rows$table) {
    DT <- tables[[tb]]
    if (!(varname %in% names(DT))) next
    w <- tolower(substr(tb, 1, 1))
    
    hid_col <- detect_hidp_col(DT, w)
    if (is.na(hid_col)) next
    
    # wave-specific pidp↔hidp
    pidp_hidp_w <- indresp_stack[wave == w, .(pidp, n_hidp)]
    if (!nrow(pidp_hidp_w)) next
    setkey(pidp_hidp_w, n_hidp)
    
    tmp_hh <- unique(DT[, .(n_hidp = get(hid_col), v = get(varname))])
    tmp_hh <- tmp_hh[!is.na(n_hidp)]
    setkey(tmp_hh, n_hidp)
    
    res <- merge(pidp_hidp_w, tmp_hh, by = "n_hidp", all.x = TRUE, sort = FALSE)
    VT  <- unique(res[, .(pidp, v)])
    VT  <- collapse_by_pidp(VT)
    VT[, wave := w]
    collected_hh[[length(collected_hh) + 1]] <- VT
  }
  
  if (length(collected_hh)) {
    allv <- rbindlist(collected_hh, use.names = TRUE, fill = TRUE)
    return(pool_latest_nonmissing(allv[, .(pidp, wave, v)], wave_order = wave_order))
  }
  
  NULL
}

# ---------------- STEP 7B. BASE INDIVIDUAL (MRP) FEATURES (NEW) ----------------
# These are NOT CPI proxies; they are “cell-defining” and control covariates for MRP / modeling.
# They will be merged into person_features once, early, and kept throughout.

add_base_mrp_features <- function(persons_geo) {
  base <- copy(persons_geo[, .(pidp, lsoa21cd, wd24cd, lad24cd, msoa21cd)])
  setkey(base, pidp)
  
  safe_join <- function(dt_base, varname, newname = NULL) {
    VT <- pull_var_dt(varname)
    if (is.null(VT)) return(dt_base)
    setkey(VT, pidp)
    out <- merge(dt_base, VT, by = "pidp", all.x = TRUE, sort = FALSE)
    nm <- newname %||% varname
    setnames(out, "v", nm, skip_absent = TRUE)
    out
  }
  
  # --- Core demographics
  base <- safe_join(base, paste0(latest_wave, "_dvage"), "age")     # if exists
  if (!("age" %in% names(base))) base <- safe_join(base, "n_dvage", "age")
  
  base <- safe_join(base, "n_sex", "sex")                           # 1 male, 2 female (typical)
  base <- safe_join(base, "n_racel_dv", "ethnicity")                 # if present
  if (!("ethnicity" %in% names(base))) base <- safe_join(base, "n_race_dv", "ethnicity")
  
  base <- safe_join(base, "n_hiqual_dv", "education_hiqual")        # highest qual
  base <- safe_join(base, "n_jbstat", "employment_status")          # labour market status (if present)
  base <- safe_join(base, "n_jbft_dv", "fulltime_parttime")         # FT/PT
  base <- safe_join(base, "n_hhsize", "hh_size")                    # household size (via hh tables OK)
  base <- safe_join(base, "n_hsownd", "tenure_hsownd")              # tenure / ownership
  base <- safe_join(base, "n_fihhmnnet1_dv", "net_hh_income")       # income
  base <- safe_join(base, "n_fihhmngrs_dv", "gross_hh_income")      # income
  
  # --- Derived, MRP-friendly categorical variables
  dt <- as.data.table(base)
  
  # Clean numeric where sensible
  dt[, age := suppressWarnings(as.numeric(age))]
  dt[, sex := suppressWarnings(as.numeric(sex))]
  dt[, education_hiqual := suppressWarnings(as.numeric(education_hiqual))]
  dt[, employment_status := suppressWarnings(as.numeric(employment_status))]
  dt[, tenure_hsownd := suppressWarnings(as.numeric(tenure_hsownd))]
  dt[, hh_size := suppressWarnings(as.numeric(hh_size))]
  
  # Age bands (editable)
  dt[, age_band := cut(
    age,
    breaks = c(-Inf, 17, 24, 34, 44, 54, 64, 74, Inf),
    labels = c("0-17","18-24","25-34","35-44","45-54","55-64","65-74","75+"),
    right = TRUE
  )]
  
  # Sex label (UKHLS convention often: 1=male, 2=female)
  dt[, sex_cat := fifelse(sex == 1, "male", fifelse(sex == 2, "female", NA_character_))]
  
  # Education: coarse buckets from hiqual_dv (keep robust to unknown coding)
  dt[, edu_cat := fcase(
    education_hiqual == 1, "degree_plus",
    education_hiqual %in% c(2,3), "higher_ed_below_degree",
    education_hiqual %in% c(4,5), "secondary_or_below",
    education_hiqual == 9, "no_qualification",
    default = NA_character_
  )]
  
  # Tenure: owner vs renter-ish (coarse)
  dt[, tenure_cat := fcase(
    tenure_hsownd %in% c(1,2,3), "owner",
    tenure_hsownd %in% c(4,5,97), "renter_or_other",
    default = NA_character_
  )]
  
  # Household size bands
  dt[, hh_size_band := fcase(
    is.na(hh_size), NA_character_,
    hh_size <= 1, "1",
    hh_size == 2, "2",
    hh_size == 3, "3",
    hh_size == 4, "4",
    hh_size >= 5, "5+",
    default = NA_character_
  )]
  
  # Income transforms (log1p, winsor-ish optional)
  for (v in c("net_hh_income","gross_hh_income")) {
    if (v %in% names(dt)) {
      dt[, (v) := suppressWarnings(as.numeric(get(v)))]
      dt[get(v) == 0, (v) := NA_real_]
      dt[, paste0(v, "_log1p") := fifelse(is.finite(get(v)) & get(v) >= 0, log1p(get(v)), NA_real_)]
    }
  }
  
  # Keep only MRP-relevant columns (plus geo + pidp)
  keep <- c(
    "pidp","lsoa21cd","wd24cd","lad24cd","msoa21cd",
    "age","age_band","sex","sex_cat",
    "ethnicity",
    "education_hiqual","edu_cat",
    "employment_status","fulltime_parttime",
    "tenure_hsownd","tenure_cat",
    "hh_size","hh_size_band",
    "net_hh_income","net_hh_income_log1p",
    "gross_hh_income","gross_hh_income_log1p"
  )
  keep <- intersect(keep, names(dt))
  dt <- dt[, ..keep]
  setkey(dt, pidp)
  dt
}

# Initialize person_features with geo backbone + MRP covariates (NEW)
person_features <- add_base_mrp_features(persons_geo)

# ---------------- STEP 7C. BLOCK HELPERS (UNCHANGED API, but now merge into existing person_features) -----

start_proxy_block <- function(domain, subdomain, measure, notes = NA_character_) {
  base_cols <- intersect(c("pidp","n_hidp","lsoa21cd","wd24cd","lad24cd","msoa21cd"), names(persons_geo))
  dt0 <- copy(persons_geo[, ..base_cols])
  
  # ha nincs msoa21cd a mappingben, legyen oszlop NA-val
  if (!"msoa21cd" %chin% names(dt0)) dt0[, msoa21cd := NA_character_]
  if (!"n_hidp"   %chin% names(dt0)) dt0[, n_hidp := NA_real_]
  
  list(
    meta = list(domain=domain, subdomain=subdomain, measure=measure, notes=notes),
    dt   = dt0,
    added_vars = character(0)
  )
}

register_vars <- function(B, vars) {
  stopifnot(is.list(B), "dt" %in% names(B))
  B$added_vars <- unique(c(B$added_vars, vars))
  B
}

add_var <- function(B, varname, transform = NULL, ...) {
  stopifnot(is.list(B), "dt" %in% names(B))
  if (!var_exists(varname)) { message(sprintf("Variable '%s' not found — skipped.", varname)); return(B) }
  VT <- pull_var_dt(varname)
  if (is.null(VT)) { message(sprintf("Variable '%s' not joinable — skipped.", varname)); return(B) }
  
  setkey(B$dt, pidp); setkey(VT, pidp)
  col_name <- varname
  if (col_name %in% names(B$dt)) col_name <- paste0(col_name, "_dup")
  
  B$dt <- merge(B$dt, VT, by = "pidp", all.x = TRUE, sort = FALSE)
  setnames(B$dt, "v", col_name, skip_absent = TRUE)
  
  vec <- suppressWarnings(as.numeric(B$dt[[col_name]]))
  B$dt[[col_name]] <- apply_transform(vec, transform = transform, ...)
  B$added_vars <- unique(c(B$added_vars, col_name))
  B
}

show_na_coverage <- function(B) {
  stopifnot(is.list(B), "dt" %in% names(B))
  if (!length(B$added_vars)) return(invisible(NULL))
  vars <- intersect(B$added_vars, names(B$dt))
  if (!length(vars)) return(invisible(NULL))
  cov <- sapply(vars, function(v) mean(is.finite(B$dt[[v]])))
  ord <- order(cov, decreasing = TRUE)
  print(round(cov[ord], 4))
  invisible(cov)
}

finalize_proxy_block <- function(
    B,
    coverage_min = MIN_COVERAGE,
    scale_before_averaging = c("0-10", "none", "0-1"),
    save_members = c("scaled10","raw"),
    member_prefix = "ATOM__"
) {
  stopifnot(is.list(B), "dt" %in% names(B))
  
  if (!exists(".coverage_log", envir = .GlobalEnv, inherits = FALSE)) {
    assign(".coverage_log", list(), envir = .GlobalEnv)
  }
  
  keep <- intersect(B$added_vars, names(B$dt))
  if (!length(keep)) { warning("No valid variables found in this block to finalize."); return(invisible(NULL)) }
  
  cov <- sapply(keep, function(v) mean(is.finite(B$dt[[v]])))
  cl <- get(".coverage_log", envir = .GlobalEnv)
  cl[[length(cl) + 1]] <- data.table(
    domain    = B$meta$domain,
    subdomain = B$meta$subdomain,
    measure   = B$meta$measure,
    var       = keep,
    coverage  = as.numeric(cov)
  )
  assign(".coverage_log", cl, envir = .GlobalEnv)
  
  keep2 <- keep[is.finite(cov) & !is.na(cov) & cov >= coverage_min]
  if (!length(keep2)) { warning("No variables passed the coverage threshold in this block."); return(invisible(NULL)) }
  
  scale_before_averaging <- match.arg(scale_before_averaging)
  scaler <- switch(
    scale_before_averaging,
    "none" = function(x) x,
    "0-1"  = minmax01,
    "0-10" = minmax0_10
  )
  
  dt <- data.table::copy(B$dt)
  
  scaled_mat <- lapply(keep2, function(v) scaler(suppressWarnings(as.numeric(dt[[v]]))))
  proxy_val <- rowMeans(as.data.frame(scaled_mat), na.rm = TRUE)
  proxy_val[is.nan(proxy_val)] <- NA_real_
  
  member_cols <- character(0)
  
  if ("scaled10" %in% save_members) {
    scaled10 <- lapply(keep2, function(v) minmax0_10(suppressWarnings(as.numeric(dt[[v]]))))
    for (i in seq_along(keep2)) {
      nm <- paste0(
        member_prefix,
        make.names(B$meta$domain), "__",
        make.names(B$meta$subdomain), "__",
        make.names(B$meta$measure), "__",
        make.names(keep2[i]), "__scaled10"
      )
      dt[, (nm) := scaled10[[i]]]
      member_cols <- c(member_cols, nm)
    }
  }
  
  if ("raw" %in% save_members) {
    for (v in keep2) {
      nm <- paste0(
        member_prefix,
        make.names(B$meta$domain), "__",
        make.names(B$meta$subdomain), "__",
        make.names(B$meta$measure), "__",
        make.names(v), "__raw"
      )
      dt[, (nm) := suppressWarnings(as.numeric(dt[[v]]))]
      member_cols <- c(member_cols, nm)
    }
  }
  
  proxy_name <- paste0("PROXY__",
                       make.names(B$meta$domain), "__",
                       make.names(B$meta$subdomain), "__",
                       make.names(B$meta$measure))
  
  id_keep <- c("pidp", "lsoa21cd", "wd24cd", "lad24cd", "msoa21cd")
  id_keep <- intersect(id_keep, names(dt))
  out <- dt[, id_keep, with = FALSE]
  out[, (proxy_name) := proxy_val]
  if (length(member_cols)) out <- cbind(out, dt[, ..member_cols])
  
  # MERGE INTO GLOBAL person_features (already initialized with MRP base)
  pf <- get("person_features", envir = .GlobalEnv)
  geo_cols <- intersect(c("lsoa21cd","wd24cd","lad24cd","msoa21cd"), names(out))
  out2 <- copy(out)
  if (length(geo_cols)) out2[, (geo_cols) := NULL]
  
  new_cols <- setdiff(names(out2), "pidp")
  dup_new  <- intersect(names(pf), new_cols)
  if (length(dup_new)) pf[, (dup_new) := NULL]
  
  setkey(pf, pidp); setkey(out2, pidp)
  pf <- merge(pf, out2, by = "pidp", all.x = TRUE, sort = FALSE)
  assign("person_features", pf, envir = .GlobalEnv)
  
  invisible(proxy_name)
}

# ======================================================================
# STEP 8. MANUAL BLOCKS (YOUR ORIGINAL BLOCKS)
# IMPORTANT: I did NOT change your mapping logic or your 0–10 normalization approach.
# NOTE: I am leaving the blocks as-is from your provided script segment.
#       Keep/extend them as you already do; finalize_proxy_block keeps 0–10 normalization.
# ======================================================================

# ======================================================================<<<<<<<<
# FOUNDATIONS OF PROSPERITY → Secure Livelihoods
# ======================================================================<<<<<<<<

# ----------------------- Secure income and good quality work ------------------

# - Pre-tax income
# - Real household disposable income
# - Proportion of Permanent Contracts
# - Commute time
# - Satisfactory leisure time
# - Overall job satisfaction
# - Satisfaction with opportunities for promotion
# - Satisfaction with quality of available jobs

B <- start_proxy_block(
  domain   = "Foundations of Prosperity",
  subdomain= "Secure Livelihoods",
  measure  = "Secure income and good quality work"
)

# >>> Gross household income (winsor + log1p; 0 → NA)
B <- add_var(
  B, "n_fihhmngrs_dv",
  transform = list(
    function(x) ifelse(is.finite(x) & x == 0, NA_real_, x),
    function(x) { q <- quantile(x, c(.01,.99), na.rm=TRUE); pmin(pmax(x, q[1]), q[2]) },
    "log1p"
  )
)

# >>> Equivalised net income (derived: 0→NA, winsor 1–99%, log1p)
B <- add_var(B, "n_fihhmnnet1_dv")   # raw net income
eq_dt <- pull_var_dt("n_ieqmoecd_dv")
if (!is.null(eq_dt)) {
  data.table::setnames(eq_dt, "v", "n_ieqmoecd_dv")
  data.table::setkey(B$dt, pidp); data.table::setkey(eq_dt, pidp)
  B$dt <- eq_dt[B$dt]
}
B$dt[, n_net_eq := {
  ok <- is.finite(n_fihhmnnet1_dv) & is.finite(n_ieqmoecd_dv) & n_ieqmoecd_dv > 0
  x  <- fifelse(ok, n_fihhmnnet1_dv / n_ieqmoecd_dv, NA_real_)
  x[x == 0] <- NA_real_
  if (sum(is.finite(x)) >= 10) {
    q <- stats::quantile(x, c(.01,.99), na.rm=TRUE)
    x <- pmin(pmax(x, q[1]), q[2])
  }
  log1p(x)
}]
B <- register_vars(B, "n_net_eq")
B$dt[, n_ieqmoecd_dv := NULL]

# >>> Proportion of permanent contracts (1=permanent, 0=temporary)
B <- add_var(
  B, "n_jbterm1",
  transform = list(
    function(x) dplyr::case_when(
      x == 1 ~ 1,       # permanent
      x == 2 ~ 0,       # temporary
      TRUE  ~ NA_real_
    )
  )
)

# >>> Satisfaction with amount of leisure time (1–7 → 0–1)
B <- add_var(
  B, "n_sclfsat7",
  transform = list(
    function(x) dplyr::case_when(
      x %in% c(-9, -8, -7, -2, -1) ~ NA_real_,
      TRUE ~ (x - 1) / 6
    )
  )
)

# >>> Job satisfaction (1–7 → 0–1)
B <- add_var(
  B, "n_jbsat",
  transform = list(
    function(x) dplyr::case_when(
      x %in% c(-9, -8, -7, -2, -1) ~ NA_real_,
      TRUE ~ (x - 1) / 6
    )
  )
)

# default: members → 0–10 saved, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# =========================================================================
# FOUNDATIONS OF PROSPERITY → Secure Livelihoods
# =========================================================================

# ----------------------- Genuinely affordable and secure housing --------------

# - Affordable housing
# - Size of House
# - Mortgage (i.e., whether they have a mortgage or not)
# - House ownership
# - Ability to keep up-to-date with bills

B <- start_proxy_block(
  domain   = "Foundations of Prosperity",
  subdomain= "Secure Livelihoods",
  measure  = "Genuinely affordable and secure housing"
)

# >>>  Housing cost for renters: inverse monthly cost (higher = better)
B <- add_var(
  B, "n_houscost2_dv",
  transform = list(
    function(x) ifelse(is.finite(x) & x <= 0, NA_real_, x),
    function(x) { 
      q <- quantile(x, c(.01, .99), na.rm = TRUE)
      pmin(pmax(x, q[1]), q[2])
    },
    "log1p",
    "flip"
  )
)

# >>> Size of dwelling: space_per_person (rooms per household member)
# Bring rooms
rooms <- pull_var_dt("n_hsrooms")
if (!is.null(rooms)) {
  data.table::setnames(rooms, "v", "n_hsrooms")
  data.table::setkey(rooms, pidp)
  data.table::setkey(B$dt, pidp)
  B$dt <- rooms[B$dt]
}

# Bring household size
hh <- pull_var_dt("n_hhsize")
if (!is.null(hh)) {
  data.table::setnames(hh, "v", "n_hhsize")
  data.table::setkey(hh, pidp)
  data.table::setkey(B$dt, pidp)
  B$dt <- hh[B$dt]
}

# Derive space_per_person
B$dt[, space_per_person := fifelse(
  is.finite(n_hsrooms) & is.finite(n_hhsize) & n_hhsize > 0,
  n_hsrooms / n_hhsize,
  NA_real_
)]
B$dt[, space_per_person := log1p(space_per_person)]

# Register only the derived indicator as a member
B <- register_vars(B, "space_per_person")

# Drop helper variables
B$dt[, c("n_hsrooms", "n_hhsize") := NULL]

# >>> No mortgage = 1, has mortgage = 0 (owners vs renters via costs)
# Bring raw housing costs for main residence
hc1 <- pull_var_dt("n_houscost1_dv")
if (!is.null(hc1)) {
  data.table::setnames(hc1, "v", "raw_houscost1_dv")
  data.table::setkey(hc1, pidp)
  data.table::setkey(B$dt, pidp)
  B$dt <- hc1[B$dt]
}

hc2 <- pull_var_dt("n_houscost2_dv")
if (!is.null(hc2)) {
  data.table::setnames(hc2, "v", "raw_houscost2_dv")
  data.table::setkey(hc2, pidp)
  data.table::setkey(B$dt, pidp)
  B$dt <- hc2[B$dt]
}

thr <- 10  # tolerance threshold in housing cost units

B$dt[, diff_ix := fifelse(
  is.finite(raw_houscost1_dv) & is.finite(raw_houscost2_dv),
  raw_houscost1_dv - raw_houscost2_dv,
  NA_real_
)]

B$dt[, no_mortgage := fcase(
  !is.finite(diff_ix)            , NA_real_,  # missing / invalid
  diff_ix < 0                    , NA_real_,  # defensive catch for anomalies
  diff_ix > thr                  , 0,         # clearly has a mortgage
  diff_ix >= 0 & diff_ix <= thr  , 1          # no mortgage (or negligible diff.)
)]

B <- register_vars(B, "no_mortgage")

# Drop helper housing-cost columns
B$dt[, c("raw_houscost1_dv", "raw_houscost2_dv", "diff_ix") := NULL]

# >>> Home ownership indicator: 1 = owner, 0 = non-owner, NA = missing
own <- pull_var_dt("n_hsownd")
if (!is.null(own)) {
  data.table::setnames(own, "v", "n_hsownd")
  data.table::setkey(own, pidp)
  data.table::setkey(B$dt, pidp)
  B$dt <- own[B$dt]
}

neg_codes <- c(-9, -8, -7, -2, -1)

B$dt[, own_ind := NA_real_]
B$dt[n_hsownd %in% c(1, 2, 3), own_ind := 1]   # owner (outright / mortgage / shared)
B$dt[n_hsownd %in% c(4, 5, 97), own_ind := 0]  # renter / rent-free / other
B$dt[n_hsownd %in% neg_codes,     own_ind := NA_real_]

B <- register_vars(B, "own_ind")

# Drop helper variable
B$dt[, n_hsownd := NULL]

# >>> Ability to keep up-to-date with bills
xb <- pull_var_dt("n_xphsdba")
if (!is.null(xb)) {
  data.table::setnames(xb, "v", "n_xphsdba")
  data.table::setkey(xb, pidp)
  data.table::setkey(B$dt, pidp)
  B$dt <- xb[B$dt]
}

xc <- pull_var_dt("n_xphsdct")
if (!is.null(xc)) {
  data.table::setnames(xc, "v", "n_xphsdct")
  data.table::setkey(xc, pidp)
  data.table::setkey(B$dt, pidp)
  B$dt <- xc[B$dt]
}

xh <- pull_var_dt("n_xphsdb")
if (!is.null(xh)) {
  data.table::setnames(xh, "v", "n_xphsdb")
  data.table::setkey(xh, pidp)
  data.table::setkey(B$dt, pidp)
  B$dt <- xh[B$dt]
}

B$dt[, bills_prob := fifelse(
  n_xphsdba %in% c(1, 2, 3),
  fifelse(n_xphsdba == 1, 0, 1), 
  NA_real_
)]

B$dt[, ct_prob := fifelse(
  n_xphsdct %in% c(1, 2),
  fifelse(n_xphsdct == 1, 1, 0),
  NA_real_
)]

B$dt[, hous_prob := fifelse(
  n_xphsdb %in% c(1, 2),
  fifelse(n_xphsdb == 1, 1, 0),
  NA_real_
)]

# Count how many of the three problem flags are available
B$dt[, n_available := rowSums(!is.na(.SD)), 
     .SDcols = c("bills_prob", "ct_prob", "hous_prob")]

# Any problem: 1 if any of the three is 1, 0 if all observed and none are 1, NA if no info
B$dt[, any_problem := fifelse(
  n_available == 0, 
  NA_real_,
  fifelse(rowSums(.SD, na.rm = TRUE) > 0, 1, 0)
), .SDcols = c("bills_prob", "ct_prob", "hous_prob")]

# Final indicator: 1 = problem-free, 0 = any difficulty, NA = all missing
B$dt[, finance_free_ind := fifelse(
  is.na(any_problem), 
  NA_real_,
  fifelse(any_problem == 1, 0, 1)
)]

B <- register_vars(B, "finance_free_ind")

# Drop helper columns
B$dt[, c("n_xphsdba", "n_xphsdct", "n_xphsdb",
         "bills_prob", "ct_prob", "hous_prob",
         "n_available", "any_problem") := NULL]

# >>> Satisfaction with: house/flat
B <- add_var(
  B, "n_sclfsat3",
  transform = list(
    function(x) dplyr::case_when(
      x %in% c(-9, -8, -7, -2, -1) ~ NA_real_,
      TRUE ~ (x - 1) / 6
    )
  )
)

# default: members → 0–10 saved, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# =========================================================================
# FOUNDATIONS OF PROSPERITY → Secure Livelihoods
# =========================================================================

# ----------------------- Freedom from financial stress ------------------------

# - Debt burden
# - Ability to save

B <- start_proxy_block(
  domain   = "Foundations of Prosperity",
  subdomain= "Secure Livelihoods",
  measure  = "Freedom from financial stress"
)

data.table::setDT(B$dt)

# >>> Subjective financial situation (current)
# Add the original UKHLS variable (raw scale)
B <- add_var(B, "n_finnow")

# Ensure we do NOT treat n_finnow as a member in finalize_proxy_block()
# (the derived indicator will be the actual member)
B$added_vars <- setdiff(B$added_vars, "n_finnow")
B$dt <- data.table::copy(B$dt)

# Create the derived indicator of financial comfort (0–1 scale)
B$dt[, current_financial_comfort_ind := fifelse(
  n_finnow %in% c(-9, -8, -7, -2, -1) | !is.finite(n_finnow), NA_real_,
  fifelse(
    n_finnow == 1, 1,
    fifelse(
      n_finnow == 2, 0.75,
      fifelse(
        n_finnow == 3, 0.5,
        fifelse(
          n_finnow == 4, 0.25,
          fifelse(n_finnow == 5, 0, NA_real_)
        )
      )
    )
  )
)]

# Drop the source variable (we no longer need the raw UKHLS item)
B$dt[, n_finnow := NULL]

# Register the derived indicator as the only member of this proxy block
B <- register_vars(B, "current_financial_comfort_ind")

# >>> Ability to save
sav <- pull_var_dt("n_matdepf")
if (!is.null(sav)) {
  data.table::setnames(sav, "v", "n_matdepf")
  data.table::setkey(sav, pidp)
  data.table::setkey(B$dt, pidp)
  B$dt <- sav[B$dt]
}

B$dt[, ability_to_save_ind := fifelse(
  n_matdepf %in% c(1, 3), 1,
  fifelse(n_matdepf == 2, 0, NA_real_)
)]

B$dt[, n_matdepf := NULL]

# Register as block member
B <- register_vars(B, "ability_to_save_ind")

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# =========================================================================
# FOUNDATIONS OF PROSPERITY → Secure Livelihoods
# =========================================================================

# ----------------------- Food and Energy Security -----------------------------

# - Eating less due to lack of money
# - Use of food banks
# - Ability to keep accommodation warm

B <- start_proxy_block(
  domain   = "Foundations of Prosperity",
  subdomain= "Secure Livelihoods",
  measure  = "Food and energy security"
)

data.table::setDT(B$dt)

# >>> Eating less / food insecurity (Wave 13 household items; pulled to pidp)
ranout <- pull_var_dt("m_ranoutfd")
fewtyp <- pull_var_dt("m_fewfdtyp")
lack   <- pull_var_dt("m_lacknutr")

# Join pidp-level pooled values
if (!is.null(ranout)) { setnames(ranout, "v", "m_ranoutfd"); setkey(ranout, pidp); setkey(B$dt, pidp); B$dt <- ranout[B$dt] }
if (!is.null(fewtyp)) { setnames(fewtyp, "v", "m_fewfdtyp"); setkey(fewtyp, pidp); setkey(B$dt, pidp); B$dt <- fewtyp[B$dt] }
if (!is.null(lack))   { setnames(lack,   "v", "m_lacknutr"); setkey(lack,   pidp); setkey(B$dt, pidp); B$dt <- lack[B$dt] }

data.table::setDT(B$dt)

# Recode each item to a 0–1 "food secure" direction:
# Yes (1) -> 0, No (2) -> 1, missing / special negatives -> NA
recode_food_item <- function(x) {
  data.table::fifelse(
    x %in% c(-9, -8, -7, -2, -1) | !is.finite(x), NA_real_,
    data.table::fifelse(
      x == 1, 0,                               # Yes -> food insecure
      data.table::fifelse(x == 2, 1, NA_real_) # No  -> food secure
    )
  )
}

B$dt[, ranout_food_ind         := recode_food_item(m_ranoutfd)]
B$dt[, few_types_food_ind      := recode_food_item(m_fewfdtyp)]
B$dt[, lack_nutrition_food_ind := recode_food_item(m_lacknutr)]

# Combine to a single PIDP-level indicator:
# Rule:
#  - if ANY item is 0 -> 0
#  - else if ANY item is 1 -> 1
#  - else -> NA
B$dt[, food_security_hh_ind := {
  vals_mat <- as.matrix(.SD)
  any_zero <- rowSums(vals_mat == 0, na.rm = TRUE) > 0
  any_one  <- rowSums(vals_mat == 1, na.rm = TRUE) > 0
  
  out <- rep(NA_real_, .N)
  out[any_zero] <- 0
  out[!any_zero & any_one] <- 1
  out
}, .SDcols = c("ranout_food_ind", "few_types_food_ind", "lack_nutrition_food_ind")]

# Register as member (PIDP-level; NO ward aggregation)
B <- register_vars(B, "food_security_hh_ind")

# Drop helper columns (keep only the derived member)
B$dt[, c("m_ranoutfd","m_fewfdtyp","m_lacknutr",
         "ranout_food_ind","few_types_food_ind","lack_nutrition_food_ind") := NULL]

# >>> Use of food banks
B <- add_var(B, "n_foodbank")
B <- add_var(B, "n_foodbankno")

used_any   <- B$dt$n_foodbank %in% c(2, 3, 4)
nonused    <- B$dt$n_foodbank == 1
usage_na   <- is.na(B$dt$n_foodbank) | !is.finite(B$dt$n_foodbank)
bad_reason <- B$dt$n_foodbankno %in% c(2, 3)   # did not want / could not access
B$dt <- data.table::copy(B$dt)

B$dt[, foodbank_nonuse_secure_ind := fifelse(
  usage_na, NA_real_,
  fifelse(
    used_any, 0,
    fifelse(
      nonused & bad_reason, 0,
      fifelse(nonused, 1, NA_real_)
    )
  )
)]

B <- register_vars(B, "foodbank_nonuse_secure_ind")
B$dt[, c("n_foodbank","n_foodbankno") := NULL]

# >>> Ability to keep accommodation warm
B <- add_var(B, "n_hheat")
B$dt <- data.table::copy(B$dt)

B$dt[, heat_ok := fifelse(
  n_hheat %in% c(1, 3), 1,
  fifelse(n_hheat == 2, 0, NA_real_)
)]
B <- register_vars(B, "heat_ok")
B$dt[, n_hheat := NULL]

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# =========================================================================
# FOUNDATIONS OF PROSPERITY → Secure Livelihoods
# =========================================================================

# ----------------------- Access to key basic services -------------------------

# - Affordable public transport
# - Satisfaction with public transport
# - Mode of transportation for work/education
# - Internet access

B <- start_proxy_block(
  domain   = "Foundations of Prosperity",
  subdomain= "Secure Livelihoods",
  measure  = "Access to key basic services"
)

# >>> Digital connectivity (composite internet access)
# Bring in relevant 0/1 or 1/2 variables
B <- add_var(B, "n_pcnet")     # 1=Yes, 2=No
B <- add_var(B, "n_homewifi")  # 1=Yes, 2=No
B <- add_var(B, "n_hhpc1")     # desktop (0/1)
B <- add_var(B, "n_hhpc2")     # laptop (0/1)
B <- add_var(B, "n_hhpc3")     # netbook (0/1)
B <- add_var(B, "n_hhpc4")     # tablet (0/1)
B <- add_var(B, "n_hhpc5")     # other (0/1)
B <- add_var(B, "n_hhpc96")    # none of the above (0/1)
B <- add_var(B, "n_nethow1")   # PC/laptop/netbook/tablet (0/1)
B <- add_var(B, "n_nethow2")   # Digital TV (0/1)
B <- add_var(B, "n_nethow3")   # Mobile phone (0/1)
B <- add_var(B, "n_nethow4")   # Games console (0/1)
B <- add_var(B, "n_nethow5")   # Other (0/1)

data.table::setDT(B$dt)

# 1) Basic internet access: 1 if either pcnet==1 or homewifi==1,
#    0 if both clearly “No”, otherwise NA
B$dt[, internet_ok := fifelse(
  n_pcnet == 1 | n_homewifi == 1, 1,
  fifelse(n_pcnet == 2 & n_homewifi == 2, 0, NA_real_)
)]

# 2) Wi-Fi quality: 1 if homewifi==1, 0 if ==2, NA otherwise
B$dt[, wifi_ok := fifelse(
  n_homewifi %in% c(1, 2),
  fifelse(n_homewifi == 1, 1, 0),
  NA_real_
)]

# 3) Device richness: sum of desktop/laptop/netbook/tablet/other
#    If “none of the above” ==1 → set device count to 0
#    Cap at 3, then divide by 3 to get 0–1 scale
dev_cols <- c("n_hhpc1","n_hhpc2","n_hhpc3","n_hhpc4","n_hhpc5")
B$dt[, devices_count := {
  r <- rowSums(.SD, na.rm = FALSE)
  r <- ifelse(n_hhpc96 == 1, 0, r)
  r
}, .SDcols = dev_cols]

B$dt[, devices_norm := fifelse(
  is.na(devices_count), NA_real_,
  pmin(pmax(devices_count, 0), 3) / 3
)]

# 4) Access method diversity: sum of nethow1–5, capped at 3, scaled to 0–1
mode_cols <- c("n_nethow1","n_nethow2","n_nethow3","n_nethow4","n_nethow5")
B$dt[, methods_count := rowSums(.SD, na.rm = FALSE), .SDcols = mode_cols]

B$dt[, methods_norm := fifelse(
  is.na(methods_count), NA_real_,
  pmin(pmax(methods_count, 0), 3) / 3
)]

# 5) Mobile-only penalty: if only one access method and it’s “mobile”, flag as 1
B$dt[, mobile_only := fifelse(
  methods_count == 1 & n_nethow3 == 1, 1, 0
)]

# 6) Composite raw index: mean of available components
# (internet, wifi, devices, methods)
B$dt[, digital_connectivity_raw := rowMeans(
  cbind(internet_ok, wifi_ok, devices_norm, methods_norm),
  na.rm = TRUE
)]

# 7) Apply mobile-only penalty (–0.25) and constrain to [0,1]
B$dt[, digital_connectivity_ind := pmax(
  0,
  fifelse(
    is.finite(digital_connectivity_raw),
    digital_connectivity_raw - 0.25 * mobile_only,
    NA_real_
  )
)]

# # 8) Binarize to "fully connected" (threshold can be tuned) --------- Disabled
# THRESH_DC <- 0.9
# B$dt[, digital_full_connectivity := fifelse(
#   is.finite(digital_connectivity_ind),
#   as.numeric(digital_connectivity_ind > THRESH_DC),
#   NA_real_
# )]

# Register only the composite indicator in the global registry
B <- register_vars(B, "digital_connectivity_ind")

# Drop helper variables, keep only the derived composite
B$dt[, c(
  "n_pcnet","n_homewifi",
  dev_cols, "n_hhpc96",
  mode_cols,
  "devices_count","devices_norm",
  "methods_count","methods_norm",
  "internet_ok","wifi_ok","mobile_only",
  "digital_connectivity_raw"
) := NULL]

# >>> Standard of public transport (Wave L)
# Load wave L geography with the raw public transport item
geo_l <- load_wave_geo(
  filename = "l_indresp",
  vars     = c("l_locserc")
)

# Recode l_locserc to a 0–1 indicator (person-level):
geo_l[, public_transport_quality_ind := fifelse(
  l_locserc %in% c(-9, -8, -7, -2, -1, 5) | !is.finite(l_locserc),
  NA_real_,
  1 - (l_locserc - 1) / 3
)]

# Attach PERSON-LEVEL indicator to the current proxy-block table (by pidp)
public_transport_person <- geo_l[, .(pidp, public_transport_quality_ind)]
B$dt <- merge(
  B$dt,
  public_transport_person,
  by   = "pidp",
  all.x = TRUE,
  sort  = FALSE
)

# Register the person-level indicator as an additional member of this proxy block
B <- register_vars(B, "public_transport_quality_ind")

B$added_vars <- c(
  "digital_connectivity_ind",
  "public_transport_quality_ind"
)

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# =========================================================================
# FOUNDATIONS OF PROSPERITY → Secure Livelihoods
# =========================================================================

# ----------------------- Feeling secure about the future ----------------------

# - Having social support when in need
# - Anticipation of moving out of the area

B <- start_proxy_block(
  domain    = "Foundations of Prosperity",
  subdomain = "Secure Livelihoods",
  measure   = "Feeling secure about the future"
)

# Inputs: loneliness items, expectation to move, future finances

B <- add_var(B, "n_sclackcom")
B <- add_var(B, "n_scleftout")
B <- add_var(B, "n_scisolate")
B <- add_var(B, "n_sclonely")
B <- add_var(B, "n_xpmove")
B <- add_var(B, "n_finfut")

data.table::setDT(B$dt)

# >>> Perceived social support index
loneliness_items <- c("n_sclackcom", "n_scleftout", "n_scisolate", "n_sclonely")

# Temporary recoded versions
B$dt[, c("sclack_rc", "scleft_rc", "scisolate_rc", "sclonely_rc") := {
  x1 <- n_sclackcom
  x2 <- n_scleftout
  x3 <- n_scisolate
  x4 <- n_sclonely
  
  bad <- c(-9, -8, -7, -2, -1)
  
  x1[x1 %in% bad] <- NA_real_
  x2[x2 %in% bad] <- NA_real_
  x3[x3 %in% bad] <- NA_real_
  x4[x4 %in% bad] <- NA_real_
  
  x1 <- (3 - x1) / 2  # 1→1, 2→0.5, 3→0
  x2 <- (3 - x2) / 2
  x3 <- (3 - x3) / 2
  x4 <- (3 - x4) / 2
  
  list(x1, x2, x3, x4)
}]

# Row-wise mean of the four recoded items
B$dt[, support_in_need_ind := rowMeans(
  cbind(sclack_rc, scleft_rc, scisolate_rc, sclonely_rc),
  na.rm = TRUE
)]

# If all four items are NA in a row, rowMeans with na.rm = TRUE returns NaN
B$dt[is.nan(support_in_need_ind), support_in_need_ind := NA_real_]

# Drop temporary recoded columns
B$dt[, c("sclack_rc", "scleft_rc", "scisolate_rc", "sclonely_rc") := NULL]

# >>> Anticipation of moving out of the area
B$dt[, xpmove_secure_ind := fifelse(
  n_xpmove %in% c(-9, -8, -7, -2, -1) | !is.finite(n_xpmove), NA_real_,
  fifelse(
    n_xpmove == 1, 0,
    fifelse(n_xpmove == 2, 1, NA_real_)
  )
)]

# >>> Expected future financial situation
B$dt[, finfuture_secure_ind := fifelse(
  n_finfut %in% c(-9, -8, -7, -2, -1) | !is.finite(n_finfut), NA_real_,
  fifelse(
    n_finfut == 1, 1,
    fifelse(
      n_finfut == 3, 0.5,
      fifelse(n_finfut == 2, 0, NA_real_)
    )
  )
)]

B$added_vars <- c(
  "support_in_need_ind",
  "xpmove_secure_ind",
  "finfuture_secure_ind"
)

# Drop raw UKHLS inputs
B$dt[, c(loneliness_items, "n_xpmove", "n_finfut") := NULL]

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# FOUNDATIONS OF PROSPERITY → An inclusive economy
# ======================================================================

# ----------------------- Fairness and equity ----------------------------------

# - Childcare spending
# - Part time or full time Job
# - Income Inequality

B <- start_proxy_block(
  domain   = "Foundations of Prosperity",
  subdomain= "An inclusive economy",
  measure  = "Fairness and equity"
)

data.table::setDT(B$dt)

# >>> Full-time vs part-time employment
B <- add_var(B, "n_jbft_dv")
B$dt <- data.table::copy(B$dt)

B$dt[, fulltime_ind := fifelse(
  n_jbft_dv %in% c(-9, -8, -7, -2, -1) | !is.finite(n_jbft_dv), NA_real_,
  fifelse(
    n_jbft_dv == 1, 1,      # full-time employee
    fifelse(
      n_jbft_dv == 2, 0,    # part-time employee
      NA_real_
    )
  )
)]

# Drop source variable if not needed later in this block
B$dt[, n_jbft_dv := NULL]

# >>> Satisfaction with income (1–7 → 0–1)
B <- add_var(
  B, "n_sclfsat2",
  transform = list(
    function(x) dplyr::case_when(
      x %in% c(-9, -8, -7, -2, -1) ~ NA_real_,
      TRUE ~ (x - 1) / 6
    )
  )
)

# Rename the transformed income satisfaction variable
B$dt <- data.table::copy(B$dt)
B$dt[, income_satisfaction_ind := n_sclfsat2]
B$dt[, n_sclfsat2 := NULL]

# >>> Ordinary people get their fair share of the nation’s wealth (Wave L)
# Load wave L geography with the raw fairness item
geo_l <- load_wave_geo(
  filename = "l_indresp",
  vars     = c("l_opsoca")
)

# Recode l_opsoca (1–5) to a 0–1 indicator (PERSON-LEVEL)
geo_l[, income_equality_ind := fifelse(
  l_opsoca %in% c(-9, -8, -7, -2, -1) | !is.finite(l_opsoca),
  NA_real_,
  1 - ((l_opsoca - 1) / 4)
)]

# Attach PERSON-LEVEL value by pidp
income_equality_person <- geo_l[, .(pidp, income_equality_ind)]

B$dt <- merge(
  B$dt,
  income_equality_person,
  by   = "pidp",
  all.x = TRUE,
  sort  = FALSE
)

B$added_vars <- c(
  "fulltime_ind",
  "income_satisfaction_ind",
  "income_equality_ind"
)

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# FOUNDATIONS OF PROSPERITY → A good start in life
# ======================================================================

# ----------------------- Childhood poverty ------------------------------------

# - Use of Childcare
# - Household Size
# - Children present in the household

# B <- start_proxy_block(
#   domain   = "Foundations of Prosperity",
#   subdomain= "A good start in life",
#   measure  = "Childhood poverty"
# )

# ======================================================================
# FOUNDATIONS OF PROSPERITY → A good start in life
# ======================================================================

# ----------------------- Adolescent transitions to work or study --------------

# - Students leaving key stage 4 and transitioning to any sustained educational destination
# - Unemployment
# - School attendance

# B <- start_proxy_block(
#   domain   = "Foundations of Prosperity",
#   subdomain= "A good start in life",
#   measure  = "Adolescent transitions to work or study"
# )

# ======================================================================
# OPPORTUNITIES AND ASPIRATIONS → Good quality basic education
# ======================================================================

# ---------------------- Access to good quality education ----------------------

# - Level of education attained
# - Local quality of primary and secondary schools

B <- start_proxy_block(
  domain   = "Opportunities and Aspirations",
  subdomain= "Good quality basic education",
  measure  = "Access to good quality education"
)

# Ensure B$dt is fully owned by data.table
B$dt <- data.table::copy(B$dt)
data.table::setDT(B$dt)
B$dt <- data.table::alloc.col(B$dt)

# >>> Level of education attained (adult-level, non-youth)
B <- add_var(B, "n_hiqual_dv")
B$dt <- data.table::copy(B$dt)

if ("n_hiqual_dv" %chin% names(B$dt)) {
  B$dt[, level_edu_ind := data.table::fcase(
    is.na(n_hiqual_dv),  NA_real_,
    n_hiqual_dv == 1,    1.00,  # Degree or higher
    n_hiqual_dv == 2,    0.90,
    n_hiqual_dv == 3,    0.75,
    n_hiqual_dv == 4,    0.60,
    n_hiqual_dv == 5,    0.40,
    n_hiqual_dv == 9,    0.00,  # No qualification
    default              = NA_real_
  )]
  
  # Drop the raw qualification variable once the indicator is created
  B$dt[, n_hiqual_dv := NULL]
}

B$added_vars <- "level_edu_ind"

# >>> Standard of primary and secondary schools (Wave L)
# Load wave L geography with the raw school quality items
geo_l <- load_wave_geo(
  filename = "l_indresp",
  vars     = c("l_locserap", "l_locseras")
)

geo_l[, primary_school_quality_ind := fifelse(
  l_locserap %in% c(-9, -8, -7, -2, -1) | !is.finite(l_locserap),
  NA_real_,
  1 - (l_locserap - 1) / 3
)]

geo_l[, secondary_school_quality_ind := fifelse(
  l_locseras %in% c(-9, -8, -7, -2, -1) | !is.finite(l_locseras),
  NA_real_,
  1 - (l_locseras - 1) / 3
)]

# Attach PERSON-LEVEL indicators to the current proxy-block table (by pidp)
school_quality_person <- geo_l[, .(pidp, primary_school_quality_ind, secondary_school_quality_ind)]

B$dt <- merge(
  B$dt,
  school_quality_person,
  by   = "pidp",
  all.x = TRUE,
  sort  = FALSE
)

# Register all members of this proxy block
B$added_vars <- c(
  "level_edu_ind",
  "primary_school_quality_ind",
  "secondary_school_quality_ind"
)

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# OPPORTUNITIES AND ASPIRATIONS → Lifelong learning                             
# ======================================================================

# ----------------------- Access to skills and training for work ---------------

# - Participation in professional training through work

B <- start_proxy_block(
  domain   = "Opportunities and Aspirations",
  subdomain= "Lifelong learning",
  measure  = "Access to skills and training for work"
)

# Ensure data.table with extra column capacity
B$dt <- data.table::as.data.table(B$dt)
data.table::setDT(B$dt)
B$dt <- data.table::alloc.col(B$dt)

# Add required variables from n_indresp (already cleaned, no negative codes)
for (v in c("n_jblkchb", "n_jbxpchb")) {
  B <- add_var(B, v)
}

dt <- data.table::as.data.table(B$dt)
data.table::setDT(dt)
dt <- data.table::alloc.col(dt)

# >>> Access to skills and training for work (everyone)
dt[, access_training_any :=
     fifelse(n_jblkchb == 1 | n_jbxpchb == 1, 1, 0)
]

# Drop source variables if not needed further
dt[, c("n_jblkchb", "n_jbxpchb") := NULL]

B$dt <- dt
B$added_vars <- "access_training_any"

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# OPPORTUNITIES AND ASPIRATIONS → Lifelong learning
# ======================================================================

# ---- Opportunities for self-improvement and personal development -------------

# - Participation in adult learning classes

B <- start_proxy_block(
  domain    = "Opportunities and Aspirations",
  subdomain = "Lifelong learning",
  measure   = "Opportunities for self-improvement and personal development"
)

# Ensure data.table
B$dt <- data.table::as.data.table(B$dt)
data.table::setDT(B$dt)
B$dt <- data.table::alloc.col(B$dt)

# Merge age
age_dt <- data.table::as.data.table(tables$n_indresp[, c("pidp", "n_dvage")])
data.table::setkey(age_dt, pidp)
data.table::setkey(B$dt,  pidp)

# Left join age onto B$dt
B$dt <- age_dt[B$dt]

# Safe copy
B$dt <- data.table::copy(B$dt)
data.table::setDT(B$dt)
B$dt <- data.table::alloc.col(B$dt)

# Add original variables
for (v in c(
  "n_fenow_cawi",  # FE leaving age / status (CAWI)
  "n_fenow",       # FE status
  "n_feend",       # FE leaving age
  "n_j1none",      # still in full-time education
  "n_trainany",    # any training
  "n_servuse7",    # adult education classes in last 12 months
  "n_jblkchb",     # would like work-related training
  "n_jbxpchb"      # expect work-related training
)) {
  B <- add_var(B, v)
}

dt <- data.table::copy(B$dt)
data.table::setDT(dt)
dt <- data.table::alloc.col(dt)

clean <- function(x) fifelse(
  x %in% c(-9, -8, -7, -2, -1),
  NA_real_,
  as.numeric(x)
)

dt[, `:=`(
  n_fenow_cawi_cl = clean(n_fenow_cawi),
  n_fenow_cl      = clean(n_fenow),
  n_feend_cl      = clean(n_feend),
  n_j1none_cl     = clean(n_j1none),
  n_trainany_cl   = clean(n_trainany),
  n_servuse7_cl   = clean(n_servuse7),
  n_jblkchb_cl    = clean(n_jblkchb),
  n_jbxpchb_cl    = clean(n_jbxpchb)
)]

# >>> 30+ self improvement indicator
dt[, self_improvement_30plus :=
     fcase(
       # Outside target population: under 30 → NA
       n_dvage < 30, NA_real_,
       
       # Any evidence of lifelong learning / self-improvement among 30+
       n_dvage >= 30 & (
         # Late FE / university involvement (40+)
         n_fenow_cawi_cl > 30 |
           n_fenow_cawi_cl == 3 |
           n_fenow_cl      == 3 |
           n_feend_cl      >  30 |
           n_j1none_cl     == 1 |
           
           # Any training
           n_trainany_cl   == 1 |
           
           # Adult education service use
           n_servuse7_cl   == 1 |
           
           # Wants or expects work-related training
           n_jblkchb_cl    == 1 |
           n_jbxpchb_cl    == 1
       ), 1,
       
       # Otherwise: in target age group (30+) but no evidence of learning → 0
       n_dvage >= 30, 0
     )
]

# Drop raw and helper variables but keep n_dvage for potential reuse.
dt[, c(
  "n_fenow_cawi", "n_fenow", "n_feend", "n_j1none",
  "n_trainany", "n_servuse7",
  "n_jblkchb", "n_jbxpchb",
  "n_fenow_cawi_cl", "n_fenow_cl", "n_feend_cl", "n_j1none_cl",
  "n_trainany_cl", "n_servuse7_cl",
  "n_jblkchb_cl", "n_jbxpchb_cl"
) := NULL]

B$dt <- dt
B$added_vars <- "self_improvement_30plus"

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# OPPORTUNITIES AND ASPIRATIONS → Freedom, choice and control
# ======================================================================

# ---- Freedom from discrimination ---------------------------------------------

# - Degree to which people with different backgrounds can live in harmony
# - Degree to which different cultures, beliefs and identities can flourish

B <- start_proxy_block(
  domain    = "Opportunities and Aspirations",
  subdomain = "Freedom, choice and control",
  measure   = "Freedom from discrimination"
)

data.table::setDT(B$dt)

# >>> Neighbourhood cohesion and mutual support (Wave L)
# Load wave L geography with the raw cohesion items
geo_l <- load_wave_geo(
  filename = "l_hhresp",
  vars     = c("l_nbrcoh2", "l_nbrcoh4")
)

# l_nbrcoh2: "People in this neighbourhood are willing to help their neighbours"
# 1 = Strongly agree (good), 5 = Strongly disagree (bad)
# Recode to 0–1 scale: 1 -> 1, 5 -> 0
geo_l[, helpful_neighbours_ind := fifelse(
  l_nbrcoh2 %in% c(-9, -8, -7, -2, -1) | !is.finite(l_nbrcoh2),
  NA_real_,
  1 - ((l_nbrcoh2 - 1) / 4)
)]

# l_nbrcoh4: "People in this neighbourhood don't get along with each other"
# 1 = Strongly agree (bad), 5 = Strongly disagree (good)
# Recode to 0–1 scale: 1 -> 0, 5 -> 1
geo_l[, neighbour_harmony_ind := fifelse(
  l_nbrcoh4 %in% c(-9, -8, -7, -2, -1) | !is.finite(l_nbrcoh4),
  NA_real_,
  (l_nbrcoh4 - 1) / 4
)]

cohesion_person <- geo_l[, .(pidp, helpful_neighbours_ind, neighbour_harmony_ind)]

B$dt <- merge(
  B$dt,
  cohesion_person,
  by   = "pidp",
  all.x = TRUE,
  sort  = FALSE
)

# Register the members of this proxy block
B$added_vars <- c(
  "helpful_neighbours_ind",
  "neighbour_harmony_ind"
)

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# OPPORTUNITIES AND ASPIRATIONS → Freedom, choice and control
# ----------------------------------------------------------------------

# - Feeling free to make decisions about one’s life
# - Degree to which people feel they can take steps to improve their life

B <- start_proxy_block(
  domain    = "Opportunities and Aspirations",
  subdomain = "Freedom, choice and control",
  measure   = "Having choices and control over one’s future"
)

agency_items <- c("n_scghqh", "n_scghqd", "n_scghqc")

for (v in agency_items) {
  B <- add_var(
    B, v,
    transform = function(x) {
      x <- suppressWarnings(as.numeric(x))
      # Keep only valid 1–4 responses; everything else → NA
      x[!(x %in% 1:4)] <- NA_real_
      # Rescale so that 1 = 1 (high agency), 4 = 0 (low agency)
      out <- (4 - x) / 3
      ifelse(is.finite(out), out, NA_real_)
    }
  )
}

# >>> Person-level mean agency score (0–1)
data.table::setDT(B$dt)
B$dt[, agency_mean := {
  r <- rowMeans(.SD, na.rm = TRUE)
  r[is.nan(r)] <- NA_real_
  r
}, .SDcols = agency_items]

# USE ONLY the derived mean as the block member
B$added_vars <- "agency_mean"

show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================<<<<<<<<
# Power, Voice and Influence → Political inclusion
# ======================================================================<<<<<<<<

# # ---- Political inclusion ---------------------------------------------------

# - Trust in the Local Authority / Council
# - Trust in political parties
# - Trust in the Parliament
# - Trust in the police
# - Trust in the British legal system
# - Trust in the Greater London Authority (GLA)
# - Trust in the National Government
# - Taking part in political party activities

B <- start_proxy_block(
  domain    = "Power, Voice and Influence",
  subdomain = "Political inclusion",
  measure   = "Political inclusion"
)

# Ensure B$dt is a proper data.table
B$dt <- data.table::copy(B$dt)
data.table::setDT(B$dt)
B$dt <- data.table::alloc.col(B$dt)

# >>> Importance of political beliefs to identity (Wave N, individual-level)
B <- add_var(B, "n_scwhorupol")
B$dt <- data.table::copy(B$dt)

if ("n_scwhorupol" %chin% names(B$dt)) {
  B$dt[, political_identity_importance_ind := fifelse(
    n_scwhorupol %in% 1:4 & is.finite(n_scwhorupol),
    1 - ((n_scwhorupol - 1) / 3),   # 1 → 1 (very important), 4 → 0 (not at all)
    NA_real_
  )]
  B$dt[, n_scwhorupol := NULL]
}

B$added_vars <- "political_identity_importance_ind"

# Ward-level political inclusion (Wave L)
# Load wave L file with the relevant political items
geo_l <- load_wave_geo(
  filename = "l_indresp",
  vars     = c("l_perbfts", "l_poleff1", "l_poleff2", "l_vote6")
)

# >>> Recode to 0–1 indicators at individual level in geo_l
geo_l[, satisfaction_voting_ind := fifelse(
  l_perbfts %in% 1:5 & is.finite(l_perbfts),
  1 - ((l_perbfts - 1) / 4),
  NA_real_
)]

geo_l[, political_efficacy_qualified_ind := fifelse(
  l_poleff1 %in% 1:5 & is.finite(l_poleff1),
  1 - ((l_poleff1 - 1) / 4),
  NA_real_
)]

geo_l[, political_efficacy_informed_ind := fifelse(
  l_poleff2 %in% 1:5 & is.finite(l_poleff2),
  1 - ((l_poleff2 - 1) / 4),
  NA_real_
)]

geo_l[, political_interest_ind := fifelse(
  l_vote6 %in% 1:4 & is.finite(l_vote6),
  1 - ((l_vote6 - 1) / 3),
  NA_real_
)]

# Keep only join keys + derived indicators (person-level)
politics_person <- geo_l[, .(
  pidp,
  satisfaction_voting_ind,
  political_efficacy_qualified_ind,
  political_efficacy_informed_ind,
  political_interest_ind
)]

# Merge PERSON-LEVEL indicators into the proxy-block table (by pidp)
B$dt <- merge(
  B$dt,
  politics_person,
  by   = "pidp",
  all.x = TRUE,
  sort  = FALSE
)

# Final member list (all 0–1 already; finalize_proxy_block will rescale to 0–10)
B$added_vars <- c(
  "political_identity_importance_ind",
  "satisfaction_voting_ind",
  "political_efficacy_qualified_ind",
  "political_efficacy_informed_ind",
  "political_interest_ind"
)

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# Power, Voice and Influence → Voice and influence
# ======================================================================

# ---- Feelings of influence --------------------------------------------------

# - Degree to which people feel they can influence decisions about their local area
# - Taking part in demonstrations
# - Boycott
# - Contacted a politician, local, non-local government official

B <- start_proxy_block(
  domain    = "Power, Voice and Influence",
  subdomain = "Voice and influence",
  measure   = "Feelings of influence"
)

data.table::setDT(B$dt)

# Load political influence variables from Wave L
geo_l <- load_wave_geo(
  filename = "l_indresp",
  vars     = c("l_perpolinf", "l_poleff4", "l_opsocm", "l_demorient")
)

# >>> Perceived influence of one's vote (0–10 → 0–1)
geo_l[, vote_influence := fifelse(
  l_perpolinf %in% c(-9, -8, -7, -2, -1, 11) | !is.finite(l_perpolinf),
  NA_real_,
  l_perpolinf / 10
)]

# >>> Feeling that one has a say in government decisions (1–5 → 0–1)
geo_l[, say_in_government := fifelse(
  l_poleff4 %in% c(-9, -8, -7, -2, -1) | !is.finite(l_poleff4),
  NA_real_,
  (l_poleff4 - 1) / 4
)]

# >>> Support for public protest rights (1–5 → 1–0)
geo_l[, protest_rights := fifelse(
  l_opsocm %in% c(-9, -8, -7, -2, -1) | !is.finite(l_opsocm),
  NA_real_,
  1 - ((l_opsocm - 1) / 4)
)]

# >>> Satisfaction with democracy (1–4 → 1–0)
geo_l[, democracy_satisfaction := fifelse(
  l_demorient %in% c(-9, -8, -7, -2, -1) | !is.finite(l_demorient),
  NA_real_,
  1 - ((l_demorient - 1) / 3)
)]

# >>> Organised civic participation (OR only)
geo_l_org <- load_wave_geo(
  filename = "l_indresp",
  vars     = c(
    "l_orga1",  # Political party
    "l_orga2",  # Trade union
    "l_orga3",  # Environmental group
    "l_orga5",  # Tenants / Residents group
    "l_orga7",  # Voluntary services group
    "l_orga10", # Professional organisation
    "l_orga11"  # Other community group
  )
)

data.table::setDT(geo_l_org)

org_vars <- c(
  "l_orga1",
  "l_orga2",
  "l_orga3",
  "l_orga5",
  "l_orga7",
  "l_orga10",
  "l_orga11"
)

# Clean 0/1 membership flags
for (v in org_vars) {
  if (v %chin% names(geo_l_org)) {
    geo_l_org[, (v) := fifelse(
      get(v) %in% 0:1 & is.finite(get(v)),
      as.numeric(get(v)),
      NA_real_
    )]
  }
}

# OR-style participation: active in at least one organisation
geo_l_org[, organised_voice_or := {
  any_obs <- rowSums(!is.na(.SD)) > 0
  any_yes <- rowSums(.SD == 1, na.rm = TRUE) > 0
  out <- rep(NA_real_, .N)
  out[any_obs] <- as.numeric(any_yes[any_obs])
  out
}, .SDcols = org_vars]

# Keep join key + OR indicator
org_person <- geo_l_org[, .(pidp, organised_voice_or)]
org_person <- unique(org_person, by = "pidp")

# Keep only join key + derived indicators (person-level)
voice_person <- geo_l[, .(
  pidp,
  vote_influence,
  say_in_government,
  protest_rights,
  democracy_satisfaction
)]

# safety: ensure 1 row per pidp
voice_person <- unique(voice_person, by = "pidp")

# Merge PERSON-LEVEL indicators into the proxy-block table (by pidp)
B$dt <- merge(
  B$dt,
  voice_person,
  by   = "pidp",
  all.x = TRUE,
  sort  = FALSE
)

# Merge OR indicator
B$dt <- merge(
  B$dt,
  org_person,
  by   = "pidp",
  all.x = TRUE,
  sort  = FALSE
)

B$added_vars <- c(
  "vote_influence",
  "say_in_government",
  "protest_rights",
  "democracy_satisfaction",
  "organised_voice_or"
)

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================<<<<<<<<
# Belonging, Connections and Leisure → Social relationships
# ======================================================================<<<<<<<<

# ---- Regular contact with family, friends, and neighbours --------------------

# - Having contact with family at least 2-3 times per week
# - Having contact with friends at least 2-3 times per week
# - Having contact with neighbours at least 2-3 times per week
# - Feelings of loneliness

B <- start_proxy_block(
  domain    = "Belonging, Connections and Leisure",
  subdomain = "Social relationships",
  measure   = "Regular contact with family, friends, and neighbours"
)

data.table::setDT(B$dt)

# >>> Ability to talk to family about worries (1–4 → 1–0)
B <- add_var(
  B, "n_scropenup",
  transform = function(x) {
    x <- suppressWarnings(as.numeric(x))
    x[!(x %in% 1:4)] <- NA_real_
    (4 - x) / 3
  }
)

# >>> Ability to talk to friends about worries (1–4 → 1–0)
B <- add_var(
  B, "n_scfopenup",
  transform = function(x) {
    x <- suppressWarnings(as.numeric(x))
    x[!(x %in% 1:4)] <- NA_real_
    (4 - x) / 3
  }
)

# >>> Loneliness (1–3 → 1–0)
B <- add_var(
  B, "n_sclonely",
  transform = function(x) {
    x <- suppressWarnings(as.numeric(x))
    x[!(x %in% 1:3)] <- NA_real_
    (3 - x) / 2
  }
)

# >>> Load Wave L neighbour-contact variable
geo_l <- load_wave_geo(
  filename = "l_indresp",
  vars     = c("l_scopngbhh")
)

# Recode l_scopngbhh 1–5 → 1–0
geo_l[, neighbour_contact_ind := fifelse(
  l_scopngbhh %in% 1:5 & is.finite(l_scopngbhh),
  1 - ((l_scopngbhh - 1) / 4),
  NA_real_
)]

# Keep only join key + derived indicator (person-level)
neighbour_person <- geo_l[, .(pidp, neighbour_contact_ind)]
neighbour_person <- unique(neighbour_person, by = "pidp")

# Merge PERSON-LEVEL indicator into B (by pidp)
B$dt <- merge(
  B$dt,
  neighbour_person,
  by   = "pidp",
  all.x = TRUE,
  sort  = FALSE
)

# Final 4 variables
B$added_vars <- c(
  "n_scropenup",
  "n_scfopenup",
  "n_sclonely",
  "neighbour_contact_ind"
)

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# Belonging, Connections and Leisure → Sense of community
# ======================================================================

# ---- Community cohesion ------------------------------------------------------

# - Feeling like they belong to the neighbourhood
# - Plans to remain in the neighbourhood for a number of years
# - Feeling like the friendships and associations in their neighbourhood mean a lot to them
# - Trusting people in their neighbourhood
# - Feeling like their neighbours will help them
# - Borrowing and exchanging favours with neighbours

B <- start_proxy_block(
  domain    = "Belonging, Connections and Leisure",
  subdomain = "Sense of community",
  measure   = "Community cohesion"
)

# Ensure B$dt is a data.table with its own storage
B$dt <- data.table::copy(B$dt)
data.table::setDT(B$dt)
B$dt <- data.table::alloc.col(B$dt)

# >>> PHDCN social cohesion (l_nbrcoh_dv) from household file l_hhresp
geo_l_hh <- load_wave_geo(
  filename = "l_hhresp",
  vars     = "l_nbrcoh_dv"
)

# Recode 4–20 → 0–1 (4 = lowest cohesion → 0, 20 = highest cohesion → 1)
geo_l_hh[, social_cohesion_phdcn := fifelse(
  is.finite(l_nbrcoh_dv) & l_nbrcoh_dv >= 4 & l_nbrcoh_dv <= 20,
  (l_nbrcoh_dv - 4) / 16,
  NA_real_
)]

# >>> Buckner cohesion (l_nbrsnci_dv) from individual file l_indresp
geo_l_ind <- load_wave_geo(
  filename = "l_indresp",
  vars     = "l_nbrsnci_dv"
)

# Recode 1–5 → 0–1 (1 = lowest cohesion → 0, 5 = highest cohesion → 1)
geo_l_ind[, neighbourhood_cohesion_buckner := fifelse(
  is.finite(l_nbrsnci_dv) & l_nbrsnci_dv >= 1 & l_nbrsnci_dv <= 5,
  (l_nbrsnci_dv - 1) / 4,
  NA_real_
)]

# Buckner: PERSON-LEVEL join by pidp
buckner_person <- geo_l_ind[, .(pidp, neighbourhood_cohesion_buckner)]
buckner_person <- unique(buckner_person, by = "pidp")

B$dt <- merge(
  B$dt,
  buckner_person,
  by   = "pidp",
  all.x = TRUE,
  sort  = FALSE
)

# PHDCN: HOUSEHOLD-LEVEL join by n_hidp
# Pull n_hidp into B$dt if not already there
if (!("n_hidp" %chin% names(B$dt))) {
  B <- add_var(B, "n_hidp")
  data.table::setDT(B$dt)
}

phdcn_hh <- geo_l_hh[, .(n_hidp, social_cohesion_phdcn)]
phdcn_hh <- unique(phdcn_hh, by = "n_hidp")

B$dt <- merge(
  B$dt,
  phdcn_hh,
  by   = "n_hidp",
  all.x = TRUE,
  sort  = FALSE
)

# Members of this proxy block
B$added_vars <- c(
  "social_cohesion_phdcn",
  "neighbourhood_cohesion_buckner"
)

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# Belonging, Connections and Leisure → Sense of community
# ======================================================================

# ---- Getting involved in community life --------------------------------------

# - Volunteer work
# - Membership in civic and voluntary organisations
# - Participation in local social activities

B <- start_proxy_block(
  domain    = "Belonging, Connections and Leisure",
  subdomain = "Sense of community",
  measure   = "Getting involved in community life"
)

# >>> Adult volunteering in the last 12 months (0/1)
B <- add_var(
  B, "n_volun",
  transform = function(x) {
    x <- suppressWarnings(as.numeric(x))
    # 1 = Yes → 1; 2 = No → 0; everything else (incl. negatives) → NA
    data.table::fifelse(
      x == 1, 1,
      data.table::fifelse(x == 2, 0, NA_real_)
    )
  }
)

# >>> Adult charitable giving (0/1)
B <- add_var(
  B, "n_chargv",
  transform = function(x) {
    x <- suppressWarnings(as.numeric(x))
    # 1 = Yes → 1; 2 = No → 0; everything else → NA
    data.table::fifelse(
      x == 1, 1,
      data.table::fifelse(x == 2, 0, NA_real_)
    )
  }
)

data.table::setDT(B$dt)
B$dt <- data.table::alloc.col(B$dt)

# >>> Load Wave L geography with neighbourhood engagement items
geo_l <- load_wave_geo(
  filename = "l_indresp",
  vars     = c("l_scopngbhe",  # willing to improve neighbourhood
               # "l_scopngbha",  # feel belong to neighbourhood ------- disabled
               "l_scopngbhc")  # can get advice locally
)

data.table::setDT(geo_l)

# Recode 1–5 items to 0–1 (person-level)
geo_l[, willing_improve_neighbourhood := data.table::fifelse(
  l_scopngbhe %in% 1:5 & is.finite(l_scopngbhe),
  1 - ((l_scopngbhe - 1) / 4),
  NA_real_
)]

geo_l[, local_advice_availability := data.table::fifelse(
  l_scopngbhc %in% 1:5 & is.finite(l_scopngbhc),
  1 - ((l_scopngbhc - 1) / 4),
  NA_real_
)]

# Keep only join key + derived indicators (person-level)
engagement_person <- geo_l[, .(
  pidp,
  willing_improve_neighbourhood,
  local_advice_availability
)]
engagement_person <- unique(engagement_person, by = "pidp")

# Merge person-level engagement indicators into the proxy-block table (by pidp)
B$dt <- merge(
  B$dt,
  engagement_person,
  by   = "pidp",
  all.x = TRUE,
  sort  = FALSE
)

# Final member list, diagnostics, finalize
B$added_vars <- c(
  # person-level
  "n_volun",
  "n_chargv",
  # ward-level
  "willing_improve_neighbourhood",
  # "neighbourhood_belonging",
  "local_advice_availability"
)

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# Belonging, Connections and Leisure → Arts, leisure and sports
# ======================================================================

# ---- Participation in arts, sport, and leisure activities --------------------

# - Participation in organised arts or cultural activities
# - Membership in club (e.g., sports club)

B <- start_proxy_block(
  domain    = "Belonging, Connections and Leisure",
  subdomain = "Arts, leisure and sports",
  measure   = "Participation in arts, sport, and leisure activities"
)

data.table::setDT(B$dt)
B$dt <- data.table::alloc.col(B$dt)

# >>> Organised participation (Wave L: selected orga items)  ---- OR only
geo_l <- load_wave_geo(
  filename = "l_indresp",
  vars     = c("l_orga9", "l_orga12", "l_orga13", "l_orga14", "l_orga15")
)

data.table::setDT(geo_l)

# Clean 0/1 membership flags
# UKHLS orga items are typically 0/1; treat anything else as missing
org_vars <- c("l_orga9", "l_orga12", "l_orga13", "l_orga14", "l_orga15")
for (v in org_vars) {
  if (v %chin% names(geo_l)) {
    geo_l[, (v) := data.table::fifelse(
      get(v) %in% 0:1 & is.finite(get(v)),
      as.numeric(get(v)),
      NA_real_
    )]
  }
}

# OR-style participation (0/1): active in at least one of selected organisations
# Note: if all selected items are missing -> NA (avoid treating as 0)
geo_l[, leisure_participation_or := {
  any_obs <- rowSums(!is.na(.SD)) > 0
  any_yes <- rowSums(.SD == 1, na.rm = TRUE) > 0
  out <- rep(NA_real_, .N)
  out[any_obs] <- as.numeric(any_yes[any_obs])
  out
}, .SDcols = org_vars]

# Keep only join key + derived indicator (person-level)
arts_sport_person <- geo_l[, .(pidp, leisure_participation_or)]
arts_sport_person <- unique(arts_sport_person, by = "pidp")

# Attach PERSON-LEVEL indicator to the proxy-block table (by pidp)
B$dt <- merge(
  B$dt,
  arts_sport_person,
  by    = "pidp",
  all.x = TRUE,
  sort  = FALSE
)

# >>> Physical activity index (Wave L: vigorous + moderate days)
geo_l <- load_wave_geo(
  filename = "l_indresp",
  vars     = c("l_vday", "l_mday")
)
data.table::setDT(geo_l)

# Clean vigorous and moderate days (0–7)
geo_l[, vigorous_days := data.table::fifelse(
  !is.na(l_vday) & is.finite(l_vday) & l_vday >= 0 & l_vday <= 7,
  l_vday,
  NA_real_
)]
geo_l[, moderate_days := data.table::fifelse(
  !is.na(l_mday) & is.finite(l_mday) & l_mday >= 0 & l_mday <= 7,
  l_mday,
  NA_real_
)]

# Convert to 0–1 scales
geo_l[, vigorous_index := vigorous_days / 7]
geo_l[, moderate_index := moderate_days / 7]

# Combined physical activity index (0–1), vigorous double weight
geo_l[, physical_activity_index := {
  tmp <- (2 * vigorous_index + moderate_index) / 3
  data.table::fifelse(is.finite(tmp), tmp, NA_real_)
}]

# Keep only join key + derived indicator (person-level)
physical_activity_person <- geo_l[, .(pidp, physical_activity_index)]
physical_activity_person <- unique(physical_activity_person, by = "pidp")

# Merge PERSON-LEVEL physical activity into block dt (by pidp)
B$dt <- merge(
  B$dt,
  physical_activity_person,
  by    = "pidp",
  all.x = TRUE,
  sort  = FALSE
)

# >>> Satisfaction with amount of leisure time (1–7 → 0–1)
B <- add_var(
  B, "n_sclfsat7",
  transform = list(
    function(x) dplyr::case_when(
      x %in% c(-9, -8, -7, -2, -1) ~ NA_real_,
      TRUE ~ (x - 1) / 6
    )
  )
)

data.table::setDT(B$dt)
B$dt[, leisure_satisfaction := n_sclfsat7]
B$dt[, n_sclfsat7 := NULL]

# Final member list
B$added_vars <- c(
  "physical_activity_index",
  "leisure_participation_or",
  "leisure_satisfaction"
)

# Finalize the block: member → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================<<<<<<<<
# Health and Healthy Environments → Healthy bodies and healthy minds 
# ======================================================================<<<<<<<<

# ---- Healthy bodies --------------------------------------------------

# - Subjective health
# - Health and disability status
# - Visited Nature Recently
# - Number of days where respondent walked more than 10 minutes in past 10 days

B <- start_proxy_block(
  domain    = "Health and Healthy Environments",
  subdomain = "Healthy bodies and healthy minds",
  measure   = "Healthy bodies"
)

data.table::setDT(B$dt)
B$dt <- data.table::alloc.col(B$dt)

# >>> Satisfaction with health (0–1, higher = more satisfied)
B <- add_var(
  B, "n_sclfsat1",
  transform = function(x) {
    x <- suppressWarnings(as.numeric(x))
    x[!(x %in% 1:7)] <- NA_real_
    out <- (x - 1) / 6
    ifelse(is.finite(out), out, NA_real_)
  }
)

# >>> Long-standing illness or disability (0/1)
B <- add_var(
  B, "n_health",
  transform = function(x) {
    x <- suppressWarnings(as.numeric(x))
    data.table::fifelse(
      x == 2, 1,   # no illness → good
      data.table::fifelse(
        x == 1, 0, # has illness → bad
        NA_real_
      )
    )
  }
)

# >>> Frequency of walking (0–1)
B <- add_var(
  B, "n_walkfreq",
  transform = function(x) {
    x <- suppressWarnings(as.numeric(x))
    x[!(x %in% 1:9)] <- NA_real_
    out <- (9 - x) / 8
    ifelse(is.finite(out), out, NA_real_)
  }
)

# >>> Physical activity index (Wave L: vigorous + moderate days)
geo_l <- load_wave_geo(
  filename = "l_indresp",
  vars     = c("l_vday", "l_mday")
)
data.table::setDT(geo_l)

# Clean vigorous and moderate days (0–7)
geo_l[, vigorous_days := data.table::fifelse(
  !is.na(l_vday) & is.finite(l_vday) & l_vday >= 0 & l_vday <= 7,
  l_vday,
  NA_real_
)]
geo_l[, moderate_days := data.table::fifelse(
  !is.na(l_mday) & is.finite(l_mday) & l_mday >= 0 & l_mday <= 7,
  l_mday,
  NA_real_
)]

# Convert to 0–1 scales
geo_l[, vigorous_index := vigorous_days / 7]
geo_l[, moderate_index := moderate_days / 7]

# Combined physical activity index (0–1), vigorous double weight
geo_l[, physical_activity_index := {
  tmp <- (2 * vigorous_index + moderate_index) / 3
  data.table::fifelse(is.finite(tmp), tmp, NA_real_)
}]

# Keep only join key + derived indicator (person-level)
physical_activity_person <- geo_l[, .(pidp, physical_activity_index)]
physical_activity_person <- unique(physical_activity_person, by = "pidp")

# Merge PERSON-LEVEL physical activity into block dt (by pidp)
B$dt <- merge(
  B$dt,
  physical_activity_person,
  by   = "pidp",
  all.x = TRUE,
  sort  = FALSE
)

# Register members (include physical activity)
B$added_vars <- c(
  "n_sclfsat1",
  "n_health",
  "n_walkfreq",
  "physical_activity_index"
)

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# Health and Healthy Environments → Healthy bodies and healthy minds 
# ======================================================================

# ---- Wellbeing -------------------------------------------------------------

# - Happiness
# - Life satisfaction
# - Feeling life is worthwhile
# - Anxiety

B <- start_proxy_block(
  domain    = "Health and Healthy Environments",
  subdomain = "Healthy bodies and healthy minds",
  measure   = "Wellbeing"
)

# >>> GHQ item: general happiness
B <- add_var(
  B, "n_scghql",
  transform = function(x) {
    x <- suppressWarnings(as.numeric(x))
    # Keep only valid 1–4; everything else → NA
    x[!(x %in% 1:4)] <- NA_real_
    # 1 → 1, 4 → 0
    out <- (4 - x) / 3
    ifelse(is.finite(out), out, NA_real_)
  }
)

# >>> GHQ item: feeling worthless
B <- add_var(
  B, "n_scghqk",
  transform = function(x) {
    x <- suppressWarnings(as.numeric(x))
    # Keep only valid 1–4; everything else → NA
    x[!(x %in% 1:4)] <- NA_real_
    # 1 → 1, 4 → 0
    out <- (4 - x) / 3
    ifelse(is.finite(out), out, NA_real_)
  }
)

# >>> Satisfaction with life overall
B <- add_var(
  B, "n_sclfsato",
  transform = function(x) {
    x <- suppressWarnings(as.numeric(x))
    # Keep only valid 1–7; everything else → NA
    x[!(x %in% 1:7)] <- NA_real_
    # 1 → 0, 7 → 1
    out <- (x - 1) / 6
    ifelse(is.finite(out), out, NA_real_)
  }
)

# >>> GHQ-12 composite score (0–36)
B <- add_var(
  B, "n_scghq1_dv",
  transform = function(x) {
    x <- suppressWarnings(as.numeric(x))
    # Drop out-of-range values (negatives already set to NA at load, but keep safe)
    x[x < 0]  <- NA_real_
    x[x > 36] <- NA_real_
    out <- (36 - x) / 36
    ifelse(is.finite(out), out, NA_real_)
  }
)

data.table::setDT(B$dt)

wellbeing_members <- c("n_scghql", "n_scghqk", "n_sclfsato", "n_scghq1_dv")

B$dt[, wellbeing_mean := {
  r <- rowMeans(.SD, na.rm = TRUE)
  r[is.nan(r)] <- NA_real_
  r
}, .SDcols = wellbeing_members]

B$added_vars <- "wellbeing_mean"
B$dt[, (wellbeing_members) := NULL]

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# Health and Healthy Environments → Healthy bodies and healthy minds 
# ======================================================================

# ---- Access to health and care services --------------------------------------

# - Access to mental healthcare
# - Access to physician for physical health problems in your local area
# - Satisfaction with quality of health services

B <- start_proxy_block(
  domain    = "Health and Healthy Environments",
  subdomain = "Healthy bodies and healthy minds",
  measure   = "Access to health and care services"
)

data.table::setDT(B$dt)
B$dt <- data.table::alloc.col(B$dt)
B$dt[, pidp := as.character(pidp)]

waves <- c("l","m","n")  # 12/13/14
per_wave <- vector("list", length(waves)); names(per_wave) <- waves

to_na <- function(x) { x <- suppressWarnings(
  as.numeric(x)); x[x < 0] <- NA_real_; x }

safe_get <- function(dt, candidates) {
  for (nm in candidates) if (nm %in% names(dt)) return(to_na(dt[[nm]]))
  rep(NA_real_, nrow(dt))
}

norm_pidp <- function(dt) {
  if ("pidp" %in% names(dt)) dt[, pidp := as.character(pidp)]; dt }

for (w in waves) {
  geo <- load_wave_geo(
    filename = paste0(w, "_indresp"),
    vars = c(
      paste0(w, "_health"), paste0(w, "_hl2gp"), paste0(w, "_hl2hop"),
      # fallbacks (harmless if absent)
      "health", "hl2gp", "hl2hop"
    )
  )
  data.table::setDT(geo)
  geo <- norm_pidp(geo)
  geo <- geo[!is.na(pidp)]  # drop missing join keys early
  
  v_health <- safe_get(geo, c(paste0(w, "_health"), "health"))  # 1 yes, 2 no
  v_hl2gp  <- safe_get(geo, c(paste0(w, "_hl2gp"),  "hl2gp"))   # 1 yes, 2 no
  v_hl2hop <- safe_get(geo, c(paste0(w, "_hl2hop"), "hl2hop"))  # 0..4
  
  # Need: long-standing illness/disability (clean + comparable)
  need_health <- data.table::fifelse(
    v_health %in% c(1, 2),
    as.numeric(v_health == 1),   # 1=yes -> 1, 2=no -> 0
    NA_real_
  )
  
  # Access (only meaningful if need==1)
  gp_visit       <- data.table::fifelse(!is.na(v_hl2gp),
                                        as.numeric(v_hl2gp == 1), NA_real_)
  outpatient_any <- data.table::fifelse(!is.na(v_hl2hop),
                                        as.numeric(v_hl2hop > 0), NA_real_)
  
  any_known   <- !is.na(gp_visit) | !is.na(outpatient_any)
  any_contact <- (gp_visit == 1) | (outpatient_any == 1)
  
  access_services_if_need <- data.table::fifelse(
    need_health == 1,
    data.table::fifelse(any_known, as.numeric(any_contact), NA_real_),
    NA_real_
  )
  
  unmet_need_gp <- data.table::fifelse(
    need_health == 1 & !is.na(v_hl2gp),
    as.numeric(v_hl2gp == 2),    # need==1 and did NOT visit GP
    NA_real_
  )
  
  per_wave[[w]] <- geo[, .(pidp, wave = w,
                  need_health, access_services_if_need, unmet_need_gp)]
}

PW <- data.table::rbindlist(per_wave, use.names = TRUE, fill = TRUE)
PW[, pidp := as.character(pidp)]
PW <- PW[!is.na(pidp)]  # safety

PW[, wave_rank := data.table::fcase(wave=="l",
      1, wave=="m", 2, wave=="n", 3, default=NA_real_)]
data.table::setorder(PW, pidp, -wave_rank)

PW_need <- PW[!is.na(need_health)]
data.table::setorder(PW_need, pidp, -wave_rank)
P_person <- PW_need[, .SD[1], by = pidp,
        .SDcols = c("need_health","access_services_if_need","unmet_need_gp")]

B$dt <- merge(B$dt, P_person, by = "pidp", all.x = TRUE, sort = FALSE)
B$dt[, unmet_need_gp_pos := data.table::fifelse(!is.na(unmet_need_gp),
                                                1 - unmet_need_gp, NA_real_)]

B$added_vars <- c("access_services_if_need", "unmet_need_gp_pos")

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# Health and Healthy Environments → Healthy, safe and clean neighbourhoods
# ======================================================================

# ---- Good quality housing ----------------------------------------------------

# - Satisfaction with local housing quality
# - Satisfaction with living conditions

B <- start_proxy_block(
  domain    = "Health and Healthy Environments",
  subdomain = "Healthy, safe and clean neighbourhoods",
  measure   = "Good quality housing"
)

# >>> Satisfaction with house/flat
B <- add_var(
  B, "n_sclfsat3",
  transform = function(x) {
    x <- suppressWarnings(as.numeric(x))
    # Keep only valid 1–7 responses; everything else → NA
    x[!(x %in% 1:7)] <- NA_real_
    out <- (x - 1) / 6
    ifelse(is.finite(out), out, NA_real_)
  }
)

# >>> Housing material deprivation
B <- add_var(
  B, "n_matdepd",
  transform = function(x) {
    x <- suppressWarnings(as.numeric(x))
    data.table::fifelse(
      x %in% c(1, 3), 1,                    # has it or does not need it
      data.table::fifelse(
        x == 2, 0,                          # cannot afford → deprivation
        NA_real_                            # code 4 (“does not apply”) or anything else
      )
    )
  }
)

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# Health and Healthy Environments → Healthy, safe and clean neighbourhoods
# ======================================================================

# ---- Safe and clean neighbourhoods -------------------------------------------

# - Safety at night
# - Safety in the day

B <- start_proxy_block(
  domain    = "Health and Healthy Environments",
  subdomain = "Healthy, safe and clean neighbourhoods",
  measure   = "Safe and clean neighbourhoods"
)

data.table::setDT(B$dt)
B$dt <- data.table::alloc.col(B$dt)

# >>> Load Wave L geography with crime/safety items
geo_l <- load_wave_geo(
  filename = "l_indresp",
  vars     = c("l_crdark", "l_crwora")
)

# Recode to 0–1 individual-level indicators
# Feel safe walking alone after dark (l_crdark)
geo_l[, safety_at_night := fifelse(
  l_crdark %in% 1:4,
  1 - ((l_crdark - 1) / 3),
  NA_real_
)]

# Worry about being a victim of crime (l_crwora)
geo_l[, low_crime_worry := fifelse(
  l_crwora %in% 1:2 & is.finite(l_crwora),
  ifelse(l_crwora == 2, 1, 0),
  NA_real_
)]

# Keep only join key + derived indicators (person-level)
safety_person <- geo_l[, .(
  pidp,
  safety_at_night,
  low_crime_worry
)]
safety_person <- unique(safety_person, by = "pidp")

# Merge PERSON-LEVEL indicators into the proxy-block table (by pidp)
B$dt <- merge(
  B$dt,
  safety_person,
  by   = "pidp",
  all.x = TRUE,
  sort  = FALSE
)

# Register the ward-level indicators as members of this proxy block
B$added_vars <- c(
  "safety_at_night",
  "low_crime_worry"
)

# Finalize the block: members → 0–10 scaling, composite = mean(0–10)
show_na_coverage(B)
finalize_proxy_block(B)

# ======================================================================
# Health and Healthy Environments → Healthy, safe and clean neighbourhoods
# ======================================================================

# ---- Access to green space ---------------------------------------------------

# - Satisfaction with green/open spaces

# B <- start_proxy_block(
#   domain    = "Health and Healthy Environments",
#   subdomain = "Healthy, safe and clean neighbourhoods",
#   measure   = "Access to green space"
# )

# ======================================================================
# Health and Healthy Environments → Sustainable and resilient communities
# ======================================================================

# ---- Access to green space ---------------------------------------------------

# - Satisfaction with local natural environment

# B <- start_proxy_block(
#   domain    = "Health and Healthy Environments",
#   subdomain = "Sustainable and resilient communities",
#   measure   = "Natural Environment"
# )

# ----------------------------- STEP 9. INDEPENDENT POOLING QC (WARD N) -----------------------------
# This prints by default (not a patch) and uses the actual mapping backbone.

pooling_waves <- c("n","m","l")   # Wave 14, 13, 12
ward_by_wave <- rbindlist(lapply(pooling_waves, function(wv) {
  fn <- paste0(wv, "_indresp")
  if (!file.exists(file.path(data_dir, paste0(fn, ".tab")))) return(NULL)
  if (!file.exists(file.path(id_dir,   paste0(wv, "_lsoa21_protect.tab")))) return(NULL)
  
  g <- load_wave_geo(filename = fn)  # returns pidp + wd24cd + lad24cd + msoa21cd + etc.
  g[, wave := wv]
  g[, .(pidp, wd24cd, lad24cd, msoa21cd, lsoa21cd, wave)]
}), use.names = TRUE, fill = TRUE)

if (is.null(ward_by_wave) || nrow(ward_by_wave) == 0) {
  stop("ward_by_wave is empty: no usable waves found (need .tab + protect per wave).")
}

cat("pooling_waves:", paste(pooling_waves, collapse=", "), "\n")
cat("unique(ward_by_wave$wave):", paste(sort(unique(ward_by_wave$wave)), collapse=", "), "\n")
cat("nrows ward_by_wave:", nrow(ward_by_wave), "\n")

if (pooling_mode == "independent") {
  waves_3 <- intersect(pooling_waves, unique(ward_by_wave$wave))
  
  if (length(waves_3) < 2) {
    stop(
      "Independent pooling requires >=2 waves in ward_by_wave.\n",
      "Requested pooling_waves: ", paste(pooling_waves, collapse=", "), "\n",
      "Found in ward_by_wave$wave: ", paste(sort(unique(ward_by_wave$wave)), collapse=", "), "\n",
      "Check that both (wv_indresp.tab) and (wv_lsoa21_protect.tab) exist for each wave."
    )
  }
  
  # ensure newest-first ordering (n > m > l > ...)
  canonical <- c("n","m","l","k","j","i","h","g","f","e","d","c","b","a")
  waves_3 <- waves_3[order(match(waves_3, canonical))]
  
  latest <- if ("n" %in% waves_3) "n" else waves_3[1]
  wave_priority <- data.table(wave = waves_3, pri = seq_along(waves_3))
  
  w <- copy(ward_by_wave)
  w <- w[wave %chin% waves_3]
  w <- merge(w, wave_priority, by = "wave", all.x = TRUE)
  
  setorder(w, pidp, pri)
  pooled_indep <- w[, .SD[1], by = pidp]   # 1 row/person (latest wave)
  pooled_indep <- pooled_indep[!is.na(wd24cd) & wd24cd != ""]
  
  # Drop missing ward codes (otherwise NA becomes a mega-"ward")
  w <- w[!is.na(wd24cd) & wd24cd != ""]
  
  ward_n_indep  <- pooled_indep[, .(N_indep = .N), by = wd24cd]
  ward_n_latest <- w[wave == latest, .(N_latest = .N), by = wd24cd]
  
  cmp_indep <- merge(ward_n_indep, ward_n_latest, by = "wd24cd", all = TRUE)
  setDT(cmp_indep)
  cmp_indep[is.na(N_indep),  N_indep  := 0L]
  cmp_indep[is.na(N_latest), N_latest := 0L]
  
  cmp_indep[, delta_N := N_indep - N_latest]
  cmp_indep[, pct_increase := fifelse(N_latest > 0, 100 * delta_N / N_latest, NA_real_)]
  
  summary_indep <- data.table(
    metric   = c("mean", "median"),
    N_latest = c(mean(cmp_indep$N_latest),  median(cmp_indep$N_latest)),
    N_indep  = c(mean(cmp_indep$N_indep),   median(cmp_indep$N_indep)),
    delta_N  = c(mean(cmp_indep$delta_N),   median(cmp_indep$delta_N))
  )
  summary_indep[, pct_gain := fifelse(N_latest > 0, 100 * delta_N / N_latest, NA_real_)]
  
  cat("\n=== INDEPENDENT pooling (unique pidp) — latest vs pooled ===\n")
  print(summary_indep)
  
  cat("\n=== Top 20 wards by absolute N gain (delta_N) [INDEPENDENT] ===\n")
  print(cmp_indep[order(-delta_N)][1:min(20, .N)])
  
  # Histogram of N_indep (as in your screenshot workflow)
  png(file.path(out_base_dir, "ward_N_independent_hist.png"), width = 1200, height = 900, res = 150)
  hist(cmp_indep[order(-delta_N)]$N_indep,
       main = "Histogram of ward N under INDEPENDENT pooling (unique pidp)",
       xlab = "N_indep", col = "grey85", border = "grey40", breaks = 100)
  dev.off()
}

# =====================================================================
# STEP 10–11: AGGREGATION (person → geography), SUBDOMAIN/DOMAIN/CPI, EXPORTS + MAPS
# IMPORTANT: composites are already 0–10 at person-level (proxy columns).
# Here we ONLY aggregate (mean) to the chosen geography.
# =====================================================================

write_individual_outputs <- function(out_base_dir) {
  if (!exists("person_features", envir = .GlobalEnv, inherits = FALSE)) {
    stop("person_features not found.")
  }
  pf <- as.data.table(get("person_features", envir = .GlobalEnv))
  
  out_dir <- file.path(out_base_dir, "individual")
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  
  id_cols <- intersect(c("pidp","lsoa21cd","wd24cd","lad24cd","msoa21cd"), names(pf))
  proxy_cols <- grep("^PROXY__", names(pf), value = TRUE)
  atom_cols  <- grep("^ATOM__",  names(pf), value = TRUE)
  
  # --- NEW: also export MRP covariates in the backbone ------------------------
  mrp_cols <- intersect(c(
    "age","age_band","sex","sex_cat",
    "ethnicity",
    "education_hiqual","edu_cat",
    "employment_status","fulltime_parttime",
    "tenure_hsownd","tenure_cat",
    "hh_size","hh_size_band",
    "net_hh_income","net_hh_income_log1p",
    "gross_hh_income","gross_hh_income_log1p"
  ), names(pf))
  # ---------------------------------------------------------------------------
  
  # 1) backbone (geo + MRP covariates)
  fwrite(pf[, c(id_cols, mrp_cols), with = FALSE],
         file.path(out_dir, "person_geo_backbone.csv.gz"))
  
  # 2) proxy scores
  if (length(proxy_cols)) {
    fwrite(pf[, c(id_cols, proxy_cols), with = FALSE],
           file.path(out_dir, "person_proxy_scores.csv.gz"))
  }
  
  # 3) subdomain/domain/CPI at person-level (mirror your geo logic)
  proxy_to_sd <- data.frame(
    proxy = proxy_cols,
    domain = sub("^(PROXY__)([^_]+).*", "\\2", proxy_cols),
    subdomain = sub("^PROXY__[^_]+__([^_]+).*", "\\1", proxy_cols),
    stringsAsFactors = FALSE
  )
  proxy_to_sd$domain <- gsub("\\.", " ", proxy_to_sd$domain)
  proxy_to_sd$subdomain <- gsub("\\.", " ", proxy_to_sd$subdomain)
  
  person_sub <- pf[, ..id_cols]
  
  for (sd in unique(paste(proxy_to_sd$domain, proxy_to_sd$subdomain, sep="|||"))) {
    parts <- strsplit(sd, "\\|\\|\\|")[[1]]
    dom <- parts[1]; subd <- parts[2]
    cols <- proxy_to_sd$proxy[proxy_to_sd$domain == dom & proxy_to_sd$subdomain == subd]
    cols <- intersect(cols, names(pf))
    if (!length(cols)) next
    nm <- paste0("SUBD__", make.names(dom), "__", make.names(subd))
    person_sub[, (nm) := rowMeans(as.matrix(pf[, .SD, .SDcols = cols]), na.rm = TRUE)]
  }
  
  fwrite(person_sub, file.path(out_dir, "person_subdomain_scores.csv.gz"))
  
  sub_cols <- setdiff(names(person_sub), id_cols)
  person_dom <- pf[, ..id_cols]
  if (length(sub_cols)) {
    domain_token  <- sapply(sub_cols, function(nm) strsplit(nm, "__", fixed = TRUE)[[1]][2])
    domain_groups <- split(sub_cols, domain_token)
    for (dom in names(domain_groups)) {
      cols_for_dom <- domain_groups[[dom]]
      dname <- paste0("DOM__", make.names(dom))
      person_dom[, (dname) := rowMeans(as.matrix(person_sub[, .SD, .SDcols = cols_for_dom]), na.rm = TRUE)]
    }
  }
  fwrite(person_dom, file.path(out_dir, "person_domain_scores.csv.gz"))
  
  domcols <- grep("^DOM__", names(person_dom), value = TRUE)
  person_cpi <- person_dom[, ..id_cols]
  person_cpi[, CPI := if (!length(domcols)) NA_real_
             else rowMeans(as.matrix(person_dom[, .SD, .SDcols = domcols]), na.rm = TRUE)]
  fwrite(person_cpi, file.path(out_dir, "person_cpi.csv.gz"))
  
  # 4) atoms (optional, big)
  if (length(atom_cols)) {
    fwrite(pf[, c(id_cols, atom_cols), with = FALSE],
           file.path(out_dir, "person_atoms.csv.gz"))
  }
  
  message("Individual-level outputs written under: ", normalizePath(out_dir, winslash = "/"))
}

aggregate_and_output <- function(level = c("LAD","WARD","MSOA")) {
  level <- toupper(level)[1]
  stopifnot(level %in% c("LAD","WARD","MSOA"))
  
  if (!exists("person_features", envir = .GlobalEnv, inherits = FALSE)) {
    stop("person_features not found. You must run at least one proxy block + finalize_proxy_block() first.")
  }
  person_features <- get("person_features", envir = .GlobalEnv)
  
  id_col <- switch(level, "LAD"="lad24cd", "WARD"="wd24cd", "MSOA"="msoa21cd")
  if (!id_col %in% names(person_features)) {
    message(sprintf("[%s] ID column '%s' not found. Skipping.", level, id_col))
    return(invisible(NULL))
  }
  if (level == "MSOA" && all(is.na(person_features[[id_col]]))) {
    message("[MSOA] No MSOA mapping available. Provide combined LU and retry.")
    return(invisible(NULL))
  }
  
  out_dir <- file.path(out_base_dir, paste0("results_", tolower(level)))
  img_dir <- file.path(out_dir, "maps")
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  dir.create(img_dir, showWarnings = FALSE, recursive = TRUE)
  
  var_cols <- grep("^PROXY__", names(person_features), value = TRUE)
  if (!length(var_cols)) {
    message(sprintf("[%s] No PROXY columns found. Nothing to aggregate.", level))
    return(invisible(NULL))
  }
  
  # Aggregation: mean only (as in your "TEST" setting)
  geo_features <- as.data.table(person_features)[, {
    out <- lapply(.SD, function(v) mean(v, na.rm = TRUE))
    out
  }, by = id_col, .SDcols = var_cols]
  setnames(geo_features, id_col, "GEO_ID")
  
  # Parse proxy → domain/subdomain tokens from column names (kept as your original approach)
  proxy_to_sd <- data.frame(
    proxy = var_cols,
    domain = sub("^(PROXY__)([^_]+).*", "\\2", var_cols),
    subdomain = sub("^PROXY__[^_]+__([^_]+).*", "\\1", var_cols),
    stringsAsFactors = FALSE
  )
  proxy_to_sd$domain <- gsub("\\.", " ", proxy_to_sd$domain)
  proxy_to_sd$subdomain <- gsub("\\.", " ", proxy_to_sd$subdomain)
  
  # Subdomain scores = mean of proxies within subdomain
  geo_subdomains <- data.table(GEO_ID = geo_features$GEO_ID)
  for (sd in unique(paste(proxy_to_sd$domain, proxy_to_sd$subdomain, sep="|||"))) {
    parts <- strsplit(sd, "\\|\\|\\|")[[1]]
    dom <- parts[1]; subd <- parts[2]
    proxy_cols <- proxy_to_sd$proxy[proxy_to_sd$domain == dom & proxy_to_sd$subdomain == subd]
    proxy_cols <- intersect(proxy_cols, names(geo_features))
    if (!length(proxy_cols)) next
    colname <- paste0("SUBD__", make.names(dom), "__", make.names(subd))
    geo_subdomains[, (colname) := rowMeans(as.matrix(geo_features[, proxy_cols, with = FALSE]), na.rm = TRUE)]
  }
  
  # Domain scores = mean of subdomains within domain
  sub_cols <- setdiff(names(geo_subdomains), "GEO_ID")
  geo_domains <- data.table(GEO_ID = geo_features$GEO_ID)
  
  if (length(sub_cols)) {
    domain_token  <- vapply(sub_cols, function(nm) strsplit(nm, "__", fixed = TRUE)[[1]][2], character(1))
    domain_groups <- split(sub_cols, domain_token)
    
    for (dom in names(domain_groups)) {
      cols_for_dom <- domain_groups[[dom]]
      cols_for_dom <- intersect(cols_for_dom, names(geo_subdomains))
      if (!length(cols_for_dom)) next
      
      dname <- paste0("DOM__", make.names(dom))
      
      mat <- as.matrix(geo_subdomains[, cols_for_dom, with = FALSE])
      vals <- rowMeans(mat, na.rm = TRUE)
      vals[is.nan(vals)] <- NA_real_
      
      geo_domains[, (dname) := vals]
    }
  }
  
  domcols <- grep("^DOM__", names(geo_domains), value = TRUE)
  
  # CPI = mean of domain scores
  geo_cpi        <- data.table(GEO_ID = geo_features$GEO_ID)
  geo_cpi[, CPI := if (length(domcols))
    rowMeans(as.matrix(geo_domains[, domcols, with = FALSE]), na.rm = TRUE)
    else NA_real_]
  
  fwrite(geo_features,   file.path(out_dir, paste0(tolower(level), "_features_PROXIES.csv")))
  fwrite(geo_subdomains, file.path(out_dir, paste0(tolower(level), "_subdomain_scores_0_10.csv")))
  fwrite(geo_domains,    file.path(out_dir, paste0(tolower(level), "_domain_scores_0_10.csv")))
  fwrite(geo_cpi,        file.path(out_dir, paste0(tolower(level), "_cpi_0_10.csv")))
  
  # ---------------- MAPS (CPI + DOMAINS) with TRUE min/max ----------------
  
  message(sprintf("[%s] Building geometry for maps…", level))
  
  lsoa_g <- st_read(lsoa_shp_path, quiet = TRUE)
  lsoa_g <- lsoa_g[!st_is_empty(lsoa_g), ] |> st_make_valid()
  names(lsoa_g) <- clean_header(names(lsoa_g))
  col_lsoa <- names(lsoa_g)[grepl("(^|_)lsoa(_)?(20)?21(_)?(cd|code)", names(lsoa_g))]
  stopifnot(length(col_lsoa)>=1)
  col_lsoa <- col_lsoa[1]
  
  lsoa_sf_base <- lsoa_g |>
    dplyr::transmute(lsoa21cd = toupper(trimws(.data[[col_lsoa]]))) |>
    dplyr::select(lsoa21cd, geometry)
  
  bg_lsoa <- lsoa_sf_base |>
    dplyr::left_join(lk_wd_lad |> dplyr::select(lsoa21cd, lad24cd), by = "lsoa21cd") |>
    dplyr::filter(!is.na(lad24cd) & lad24cd != "")
  if (isTRUE(only_london)) bg_lsoa <- dplyr::filter(bg_lsoa, grepl("^E090", lad24cd))
  bg_lsoa <- st_make_valid(bg_lsoa)
  
  silhouette_bg <- bg_lsoa |>
    dplyr::summarise(geometry = st_union(geometry), .groups = "drop") |>
    st_make_valid()
  outer_outline <- st_boundary(silhouette_bg)
  
  lad_polys <- bg_lsoa |>
    dplyr::group_by(lad24cd) |>
    dplyr::summarise(.groups = "drop") |>
    st_make_valid()
  lad_outline <- st_boundary(lad_polys)
  
  if (level %in% c("LAD","WARD")) {
    lk_full <- lk_wd_lad |>
      dplyr::transmute(
        lsoa21cd,
        target = if (level == "LAD") lad24cd else wd24cd,
        lad24cd
      )
  } else {
    if (is.null(lk_lsoa_msoa)) {
      message("[MSOA] Combined LSOA→MSOA lookup not provided. Skipping maps.")
      return(invisible(list(features=geo_features, sub=geo_subdomains, dom=geo_domains, cpi=geo_cpi)))
    }
    lk_full <- lk_lsoa_msoa |>
      dplyr::left_join(lk_wd_lad |> dplyr::select(lsoa21cd, lad24cd), by = "lsoa21cd",
                       suffix = c("", ".lk2")) |>
      dplyr::mutate(lad24cd = dplyr::coalesce(.data$lad24cd, .data$lad24cd.lk2)) |>
      dplyr::transmute(lsoa21cd, target = msoa21cd, lad24cd)
  }
  
  lk_full <- lk_full |>
    dplyr::filter(!is.na(target) & target != "")
  if (isTRUE(only_london)) lk_full <- lk_full |> dplyr::filter(grepl("^E090", lad24cd))
  if (!nrow(lk_full)) {
    message(sprintf("[%s] No areas after scope filtering; skipping maps.", level))
    return(invisible(list(features=geo_features, sub=geo_subdomains, dom=geo_domains, cpi=geo_cpi)))
  }
  
  lsoa_sf <- lsoa_sf_base |>
    dplyr::inner_join(lk_full, by = "lsoa21cd") |>
    st_make_valid()
  
  geo_geom <- lsoa_sf |>
    dplyr::group_by(.data[["target"]]) |>
    dplyr::summarise(geometry = st_union(geometry), .groups = "drop") |>
    st_make_valid()
  names(geo_geom)[1] <- "GEO_ID"
  
  # CPI map
  geo_map <- geo_geom |>
    dplyr::left_join(as.data.frame(geo_cpi), by = "GEO_ID")
  geo_map$CPI <- suppressWarnings(as.numeric(geo_map$CPI))
  
  cpi_lim <- range(geo_map$CPI, na.rm = TRUE)
  if (!all(is.finite(cpi_lim))) cpi_lim <- c(0, 10)
  if (diff(cpi_lim) == 0) cpi_lim <- cpi_lim + c(-1e-6, 1e-6)
  
  bb <- st_bbox(silhouette_bg)
  
  p_cpi <- ggplot() +
    geom_sf(data = silhouette_bg, fill = "grey92", color = NA) +
    geom_sf(data = lad_outline, color = "grey65", linewidth = 0.25) +
    geom_sf(data = geo_map, aes(fill = CPI), color = "white", linewidth = 0.15) +
    geom_sf(data = outer_outline, color = "grey25", linewidth = 0.7) +
    viridis::scale_fill_viridis(option = "C", direction = 1, limits = cpi_lim, na.value = "grey90", name = "CPI (0–10)") +
    labs(title = sprintf("Citizen Prosperity Index (CPI) — %s level", level),
         caption = "Sources: UKHLS; ONS LSOA 2021 base; best-fit lookups") +
    theme_void(base_size = 10) +
    theme(legend.position = "right",
          plot.title = element_text(face = "bold", size = 12)) +
    coord_sf(xlim = c(bb["xmin"], bb["xmax"]), ylim = c(bb["ymin"], bb["ymax"]), expand = FALSE)
  
  ggsave(file.path(img_dir, paste0(tolower(level), "_cpi_0_10.png")),
         p_cpi, width = 9, height = 7.5, dpi = 300, bg = "white")
  
  # Domain maps
  if (length(domcols)) {
    for (dcol in domcols) {
      domain_name  <- gsub("^DOM__", "", dcol)
      domain_label <- gsub("_", " ", domain_name)
      
      dom_dt <- geo_domains[, .(GEO_ID, value = get(dcol))]
      geo_map_dom <- merge(geo_geom, dom_dt, by = "GEO_ID", all.x = TRUE, sort = FALSE)
      
      dom_lim <- range(geo_map_dom$value, na.rm = TRUE)
      if (!all(is.finite(dom_lim))) dom_lim <- c(0, 10)
      if (diff(dom_lim) == 0) dom_lim <- dom_lim + c(-1e-6, 1e-6)
      
      p_dom <- ggplot() +
        geom_sf(data = silhouette_bg, fill = "grey92", color = NA) +
        geom_sf(data = lad_outline, color = "grey65", linewidth = 0.25) +
        geom_sf(data = geo_map_dom, aes(fill = value), color = "white", linewidth = 0.15) +
        geom_sf(data = outer_outline, color = "grey25", linewidth = 0.7) +
        viridis::scale_fill_viridis(option = "C", direction = 1, limits = dom_lim, na.value = "grey90",
                                    name = paste0(domain_label, " (0–10)")) +
        labs(title = sprintf("CPI Domain — %s (%s level)", domain_label, level),
             caption = "Sources: UKHLS; ONS LSOA 2021 base; best-fit lookups") +
        theme_void(base_size = 10) +
        theme(legend.position = "right",
              plot.title = element_text(face = "bold", size = 12)) +
        coord_sf(xlim = c(bb["xmin"], bb["xmax"]), ylim = c(bb["ymin"], bb["ymax"]), expand = FALSE)
      
      ggsave(file.path(img_dir, paste0(tolower(level), "_domain_", domain_name, "_0_10.png")),
             p_dom, width = 9, height = 7.5, dpi = 300, bg = "white")
    }
  }
  
  invisible(list(features=geo_features, sub=geo_subdomains, dom=geo_domains, cpi=geo_cpi))
}

# ------------------ RUN AGGREGATION FOR SELECTED LEVELS -------------------------

aggregation_levels <- unique(toupper(aggregation_levels))
aggregation_levels <- intersect(aggregation_levels, c("MSOA","WARD","LAD"))
if (!length(aggregation_levels)) aggregation_levels <- c("LAD")

results <- list()
for (lvl in aggregation_levels) {
  message(sprintf("=== Aggregating at %s level ===", lvl))
  results[[lvl]] <- try(aggregate_and_output(lvl), silent = TRUE)
  if (inherits(results[[lvl]], "try-error")) {
    message(sprintf("[%s] Aggregation failed: %s", lvl, as.character(results[[lvl]])))
  }
}

write_individual_outputs(out_base_dir)

write_individual_master <- function(out_base_dir, include_atoms = FALSE) {
  out_dir <- file.path(out_base_dir, "individual")
  
  geo   <- data.table::fread(file.path(out_dir, "person_geo_backbone.csv.gz"))
  proxy <- data.table::fread(file.path(out_dir, "person_proxy_scores.csv.gz"))
  subd  <- data.table::fread(file.path(out_dir, "person_subdomain_scores.csv.gz"))
  dom   <- data.table::fread(file.path(out_dir, "person_domain_scores.csv.gz"))
  cpi   <- data.table::fread(file.path(out_dir, "person_cpi.csv.gz"))
  
  # keep only non-id columns from each layer (avoid duplicating geo columns)
  id_cols <- intersect(c("pidp","lsoa21cd","wd24cd","lad24cd","msoa21cd"), names(geo))
  
  proxy2 <- proxy[, setdiff(names(proxy), id_cols), with = FALSE]
  subd2  <- subd [, setdiff(names(subd ), id_cols), with = FALSE]
  dom2   <- dom [, setdiff(names(dom  ), id_cols), with = FALSE]
  cpi2   <- cpi [, setdiff(names(cpi  ), id_cols), with = FALSE]
  
  master <- data.table::copy(geo)
  master <- cbind(master, proxy2, subd2, dom2, cpi2)
  
  if (isTRUE(include_atoms) && file.exists(file.path(out_dir, "person_atoms.csv.gz"))) {
    atoms <- data.table::fread(file.path(out_dir, "person_atoms.csv.gz"))
    atoms2 <- atoms[, setdiff(names(atoms), id_cols), with = FALSE]
    master <- cbind(master, atoms2)
  }
  
  data.table::fwrite(master, file.path(out_dir, "person_master_all.csv.gz"))
  message("Wrote: ", normalizePath(file.path(out_dir, "person_master_all.csv.gz"), winslash="/"))
}

# usage:
write_individual_master(out_base_dir, include_atoms = FALSE)  # recommended
# write_individual_master(out_base_dir, include_atoms = TRUE) # if you want ATOM__ too

# =====================================================================
# STEP 12. DIAGNOSTICS / QC
# Purpose: Validate that CPI construction ran as expected and detect common failure modes.
# Outputs: ./results/diagnostics (csv + optional png)
# =====================================================================

run_cpi_diagnostics <- function(
    out_base_dir,
    person_features_obj        = "person_features",
    results_obj                = "results",
    corr_warn_below            = 0.80,
    proxy_atom_diff_warn       = 1e-6,
    min_nonmissing_for_checks  = 200,
    save_plots                 = TRUE
) {
  suppressWarnings({
    if (!requireNamespace("data.table", quietly = TRUE)) stop("Package 'data.table' is required.")
    if (!requireNamespace("ggplot2", quietly = TRUE)) stop("Package 'ggplot2' is required.")
  })
  library(data.table)
  library(ggplot2)
  
  diag_dir <- file.path(out_base_dir, "diagnostics")
  dir.create(diag_dir, showWarnings = FALSE, recursive = TRUE)
  
  ts_tag <- format(Sys.time(), "%Y%m%d_%H%M%S")
  say <- function(...) cat(sprintf(...), "\n")
  
  # ------------------ 0) Load core objects ------------------
  
  if (!exists(person_features_obj, envir = .GlobalEnv, inherits = FALSE)) {
    stop(sprintf("DIAG: '%s' not found in .GlobalEnv.", person_features_obj))
  }
  pf <- as.data.table(get(person_features_obj, envir = .GlobalEnv))
  
  say("=== DIAG: person_features loaded: %d rows, %d cols ===", nrow(pf), ncol(pf))
  
  if (!("pidp" %in% names(pf))) stop("DIAG: 'pidp' is missing from person_features.")
  if (anyNA(pf$pidp)) stop("DIAG: 'pidp' contains NA values.")
  
  # ------------------ 1) Key integrity: pidp uniqueness ------------------
  
  dup_pidp <- pf[, .N, by = pidp][N > 1]
  if (nrow(dup_pidp)) {
    fwrite(dup_pidp, file.path(diag_dir, paste0("pidp_duplicates_", ts_tag, ".csv")))
    warning(sprintf("DIAG: Found %d duplicate pidp values (saved csv).", nrow(dup_pidp)))
  } else {
    say("DIAG: pidp uniqueness OK (1 row per pidp).")
  }
  
  # ------------------ 2) Geography completeness ------------------
  
  geo_cols <- intersect(c("lsoa21cd","wd24cd","lad24cd","msoa21cd"), names(pf))
  geo_missing <- rbindlist(lapply(geo_cols, function(gc) {
    v <- pf[[gc]]
    data.table(
      geo_col = gc,
      missing_share = mean(is.na(v) | v == "", na.rm = TRUE)
    )
  }), use.names = TRUE, fill = TRUE)
  
  fwrite(geo_missing, file.path(diag_dir, paste0("geo_missing_", ts_tag, ".csv")))
  say("DIAG: Geography missingness saved: geo_missing_%s.csv", ts_tag)
  
  # ------------------ 3) PROXY columns: existence, range, coverage ------------------
  
  proxy_cols <- grep("^PROXY__", names(pf), value = TRUE)
  if (!length(proxy_cols)) stop("DIAG: No PROXY__ columns found in person_features.")
  
  proxy_qc <- rbindlist(lapply(proxy_cols, function(pc) {
    x <- suppressWarnings(as.numeric(pf[[pc]]))
    data.table(
      proxy = pc,
      n_nonmissing = sum(is.finite(x)),
      coverage = mean(is.finite(x)),
      min = suppressWarnings(min(x, na.rm = TRUE)),
      max = suppressWarnings(max(x, na.rm = TRUE)),
      mean = suppressWarnings(mean(x, na.rm = TRUE)),
      sd = suppressWarnings(sd(x, na.rm = TRUE))
    )
  }), use.names = TRUE, fill = TRUE)
  
  # Flag out-of-range values (tolerate tiny floating noise)
  proxy_qc[, out_of_range := (is.finite(min) & min < -1e-6) | (is.finite(max) & max > 10 + 1e-6)]
  
  fwrite(proxy_qc, file.path(diag_dir, paste0("proxy_qc_", ts_tag, ".csv")))
  say("DIAG: Proxy QC saved: proxy_qc_%s.csv", ts_tag)
  
  if (any(proxy_qc$out_of_range, na.rm = TRUE)) {
    bad <- proxy_qc[out_of_range == TRUE]
    fwrite(bad, file.path(diag_dir, paste0("proxy_out_of_range_", ts_tag, ".csv")))
    warning(sprintf("DIAG: %d PROXY columns have values outside [0,10] (saved csv).", nrow(bad)))
  } else {
    say("DIAG: PROXY ranges OK (within ~[0,10]).")
  }
  
  # Optional: proxy coverage plot
  if (isTRUE(save_plots)) {
    p_cov <- ggplot(proxy_qc, aes(x = reorder(proxy, coverage), y = coverage)) +
      geom_col() +
      coord_flip() +
      labs(title = "Proxy coverage (share non-missing)", x = NULL, y = "Coverage") +
      theme_minimal(base_size = 10)
    ggsave(file.path(diag_dir, paste0("proxy_coverage_", ts_tag, ".png")),
           p_cov, width = 10, height = max(4, 0.18 * nrow(proxy_qc)), dpi = 200, bg = "white")
  }
  
  # ------------------ 4) ATOM sanity: raw vs scaled10 monotonicity ------------------
  # Expectation: for each ATOM variable, __scaled10 should be (monotonic) min-max of __raw
  # over the whole dataset. Spearman should usually be strongly positive.
  
  atom_scaled_cols <- grep("^ATOM__.*__scaled10$", names(pf), value = TRUE)
  atom_raw_cols    <- sub("__scaled10$", "__raw", atom_scaled_cols)
  
  atom_pairs <- data.table(
    atom_scaled = atom_scaled_cols,
    atom_raw    = atom_raw_cols
  )[atom_raw %in% names(pf)]
  
  atom_corr <- rbindlist(lapply(seq_len(nrow(atom_pairs)), function(i) {
    sc <- atom_pairs$atom_scaled[i]
    rw <- atom_pairs$atom_raw[i]
    xs <- suppressWarnings(as.numeric(pf[[sc]]))
    xr <- suppressWarnings(as.numeric(pf[[rw]]))
    
    ok <- is.finite(xs) & is.finite(xr)
    n_ok <- sum(ok)
    
    if (n_ok < min_nonmissing_for_checks) {
      return(data.table(
        atom_scaled = sc, atom_raw = rw, n = n_ok,
        spearman = NA_real_, pearson = NA_real_,
        flagged = FALSE, note = "too_few_nonmissing"
      ))
    }
    
    sp <- suppressWarnings(cor(xr[ok], xs[ok], method = "spearman"))
    pr <- suppressWarnings(cor(xr[ok], xs[ok], method = "pearson"))
    
    data.table(
      atom_scaled = sc, atom_raw = rw, n = n_ok,
      spearman = sp, pearson = pr,
      flagged = is.finite(sp) & sp < corr_warn_below,
      note = ""
    )
  }), use.names = TRUE, fill = TRUE)
  
  fwrite(atom_corr, file.path(diag_dir, paste0("atom_raw_vs_scaled_corr_", ts_tag, ".csv")))
  say("DIAG: Atom raw vs scaled correlations saved: atom_raw_vs_scaled_corr_%s.csv", ts_tag)
  
  if (any(atom_corr$flagged, na.rm = TRUE)) {
    bad_atoms <- atom_corr[flagged == TRUE][order(spearman)]
    fwrite(bad_atoms, file.path(diag_dir, paste0("atom_corr_flagged_", ts_tag, ".csv")))
    warning(sprintf(
      "DIAG: %d ATOM raw-vs-scaled pairs have Spearman < %.2f (saved csv).",
      nrow(bad_atoms), corr_warn_below
    ))
  } else {
    say("DIAG: Atom raw-vs-scaled monotonicity looks OK (Spearman mostly >= %.2f).", corr_warn_below)
  }
  
  # ------------------ 5) PROXY reconstruction from ATOM__*__scaled10 ------------------
  # finalize_proxy_block() defines PROXY as mean of scaled members (0–10 scaling depends on chosen scaler).
  # Here we validate internal consistency using the saved member scaled10 columns:
  # PROXY should match rowMeans(ATOM__...__scaled10 members), up to tiny numeric tolerance.
  
  parse_proxy_key <- function(x) sub("^PROXY__", "", x)
  parse_atom_key  <- function(x) {
    y <- sub("^ATOM__", "", x)
    y <- sub("__[^_]+__scaled10$", "", y)  # drop variable name + suffix
    y
  }
  
  atom_keys <- parse_atom_key(atom_scaled_cols)
  proxy_keys <- parse_proxy_key(proxy_cols)
  
  # Map: proxy_key -> atom members (same domain/subdomain/measure signature)
  proxy_atom_map <- lapply(seq_along(proxy_cols), function(j) {
    pk <- proxy_keys[j]
    members <- atom_scaled_cols[atom_keys == pk]
    list(proxy = proxy_cols[j], key = pk, members = members)
  })
  
  proxy_consistency <- rbindlist(lapply(proxy_atom_map, function(m) {
    pc <- m$proxy
    members <- m$members
    
    xp <- suppressWarnings(as.numeric(pf[[pc]]))
    if (!length(members)) {
      return(data.table(proxy = pc, n_members = 0L, n = sum(is.finite(xp)),
                        max_abs_diff = NA_real_, mean_abs_diff = NA_real_,
                        flagged = FALSE, note = "no_atom_members_found"))
    }
    
    mat <- as.matrix(pf[, ..members])
    mat <- apply(mat, 2, function(z) suppressWarnings(as.numeric(z)))
    rec <- rowMeans(mat, na.rm = TRUE)
    rec[is.nan(rec)] <- NA_real_
    
    ok <- is.finite(xp) & is.finite(rec)
    n_ok <- sum(ok)
    
    if (n_ok < min_nonmissing_for_checks) {
      return(data.table(proxy = pc, n_members = length(members), n = n_ok,
                        max_abs_diff = NA_real_, mean_abs_diff = NA_real_,
                        flagged = FALSE, note = "too_few_nonmissing"))
    }
    
    dif <- abs(xp[ok] - rec[ok])
    maxd <- max(dif, na.rm = TRUE)
    meand <- mean(dif, na.rm = TRUE)
    
    data.table(
      proxy = pc,
      n_members = length(members),
      n = n_ok,
      max_abs_diff = maxd,
      mean_abs_diff = meand,
      flagged = is.finite(maxd) & maxd > proxy_atom_diff_warn,
      note = ""
    )
  }), use.names = TRUE, fill = TRUE)
  
  fwrite(proxy_consistency, file.path(diag_dir, paste0("proxy_vs_atom_reconstruction_", ts_tag, ".csv")))
  say("DIAG: Proxy reconstruction check saved: proxy_vs_atom_reconstruction_%s.csv", ts_tag)
  
  if (any(proxy_consistency$flagged, na.rm = TRUE)) {
    bad_proxy <- proxy_consistency[flagged == TRUE][order(-max_abs_diff)]
    fwrite(bad_proxy, file.path(diag_dir, paste0("proxy_reconstruction_flagged_", ts_tag, ".csv")))
    warning(sprintf(
      "DIAG: %d PROXY columns differ from mean(ATOM__*__scaled10) by > %.2e (saved csv).",
      nrow(bad_proxy), proxy_atom_diff_warn
    ))
  } else {
    say("DIAG: PROXY internal reconstruction from ATOM members looks consistent.")
  }
  
  # ------------------ 6) Aggregation outputs sanity checks (if available) ------------------
  
  if (exists(results_obj, envir = .GlobalEnv, inherits = FALSE)) {
    res <- get(results_obj, envir = .GlobalEnv)
    if (is.list(res) && length(res)) {
      geo_qc <- rbindlist(lapply(names(res), function(lvl) {
        x <- res[[lvl]]
        if (inherits(x, "try-error") || is.null(x)) {
          return(data.table(level = lvl, status = "missing_or_error"))
        }
        if (!("cpi" %in% names(x)) || is.null(x$cpi)) {
          return(data.table(level = lvl, status = "no_cpi_table"))
        }
        cpi_dt <- as.data.table(x$cpi)
        if (!("CPI" %in% names(cpi_dt))) {
          return(data.table(level = lvl, status = "no_CPI_column"))
        }
        v <- suppressWarnings(as.numeric(cpi_dt$CPI))
        data.table(
          level = lvl,
          status = "ok",
          n_geo = nrow(cpi_dt),
          cpi_nonmissing = sum(is.finite(v)),
          cpi_coverage = mean(is.finite(v)),
          cpi_min = suppressWarnings(min(v, na.rm = TRUE)),
          cpi_max = suppressWarnings(max(v, na.rm = TRUE)),
          cpi_mean = suppressWarnings(mean(v, na.rm = TRUE))
        )
      }), use.names = TRUE, fill = TRUE)
      
      fwrite(geo_qc, file.path(diag_dir, paste0("geo_results_qc_", ts_tag, ".csv")))
      say("DIAG: Aggregation results QC saved: geo_results_qc_%s.csv", ts_tag)
    }
  }
  
  # ------------------ 7) Coverage log (if present) ------------------
  
  if (exists(".coverage_log", envir = .GlobalEnv, inherits = FALSE)) {
    cl <- get(".coverage_log", envir = .GlobalEnv)
    if (is.list(cl) && length(cl)) {
      cov_dt <- rbindlist(cl, use.names = TRUE, fill = TRUE)
      fwrite(cov_dt, file.path(diag_dir, paste0("coverage_log_", ts_tag, ".csv")))
      say("DIAG: Coverage log saved: coverage_log_%s.csv", ts_tag)
      
      # Quick summary table
      cov_sum <- cov_dt[, .(
        mean_coverage = mean(coverage, na.rm = TRUE),
        min_coverage  = min(coverage, na.rm = TRUE),
        max_coverage  = max(coverage, na.rm = TRUE),
        n_vars = .N
      ), by = .(domain, subdomain, measure)]
      
      fwrite(cov_sum, file.path(diag_dir, paste0("coverage_log_summary_", ts_tag, ".csv")))
    }
  }
  
  # ------------------ 8) Compact console summary ------------------
  
  say("\n=== DIAG SUMMARY ===")
  say("Rows: %d | Cols: %d | Proxies: %d | AtomPairs: %d",
      nrow(pf), ncol(pf), length(proxy_cols), nrow(atom_corr))
  
  n_low_atom <- sum(atom_corr$flagged, na.rm = TRUE)
  n_bad_recon <- sum(proxy_consistency$flagged, na.rm = TRUE)
  n_out_rng <- sum(proxy_qc$out_of_range, na.rm = TRUE)
  
  say("Proxy out-of-range flags: %d", n_out_rng)
  say("Atom raw-vs-scaled (Spearman < %.2f) flags: %d", corr_warn_below, n_low_atom)
  say("Proxy reconstruction diff (> %.2e) flags: %d", proxy_atom_diff_warn, n_bad_recon)
  
  invisible(list(
    proxy_qc = proxy_qc,
    atom_corr = atom_corr,
    proxy_consistency = proxy_consistency,
    geo_missing = geo_missing
  ))
}

# Run diagnostics
diag_results <- run_cpi_diagnostics(out_base_dir)

message("Done. Outputs written under: ", normalizePath(out_base_dir, winslash = "/"))