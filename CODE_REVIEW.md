# Code Review: Citizen Prosperity Index Pipeline

## Summary

Review of the three-stage pipeline (data preparation, MRP, post-MRP aggregation)
for creating a Citizen Prosperity Index from Understanding Society (UKHLS) data.

Two code bugs were identified, along with several methodological concerns.

Updated 2026-01-30 with findings from the MRP output data
(`poststrat_14_01_2026/`), including fit diagnostics, MCSE stability,
microdata coverage gaps, and credible interval analysis.

---

## BUG 1 (High Impact): Wellbeing block double-counts its members

**File:** `Base R Codes/1_mapping_v43.R`, line 2703

The wellbeing proxy block adds four GHQ/satisfaction items via `add_var()`,
which populates `B$added_vars` with `[n_scghql, n_scghqk, n_sclfsato, n_scghq1_dv]`.
Then line 2703 does:

```r
B$added_vars <- unique(c(B$added_vars, "wellbeing_mean"))
```

This **appends** `wellbeing_mean` to the existing 4 items, giving 5 members.
`finalize_proxy_block()` creates a proxy that is the mean of all 5 (each
scaled to 0-10). But `wellbeing_mean` is itself the rowMean of those same 4
items -- so every item is double-counted.

Additionally, `n_scghq1_dv` is the GHQ-12 Likert composite (0-36), which
already contains `n_scghql` and `n_scghqk` as components. Those two individual
items are therefore **triple-counted**: once directly, once via `wellbeing_mean`,
and once again through the GHQ-12 composite.

Compare with the analogous "agency" block at line 1987, which correctly
**replaces** rather than appends:

```r
B$added_vars <- "agency_mean"   # correct -- replaces
```

**Fix:** Line 2703 should be:

```r
B$added_vars <- "wellbeing_mean"
```

Even after this fix, consider whether the GHQ-12 composite should appear
alongside its own sub-items inside `wellbeing_mean`.

---

## BUG 2 (Medium Impact): Health service access conflates utilization with accessibility

**File:** `Base R Codes/1_mapping_v43.R`, lines 2738-2741

```r
geo_l[, health_service_access := fifelse(
  l_hlsv96 %in% c(0, 1) & is.finite(l_hlsv96),
  ifelse(l_hlsv96 == 0, 1, 0),
  NA_real_
)]
```

`l_hlsv96` is a "None of these" flag for health/welfare services used.
The code treats "used some service" as good access (1) and "used none"
as bad access (0). However, a healthy person who does not need services
is coded as having poor access, while a chronically ill person who
frequently uses services is coded as having good access. This measures
**utilization**, not **accessibility** -- and for a prosperity index
the two can point in opposite directions.

---

## METHODOLOGICAL CONCERN 1: Age band mismatch for 16-17 year olds in MRP

**File:** `Base R Codes/2_MRP_v4.R`, lines 424-426

The microdata "18-24" band is relabeled to "16-24" for Census alignment,
but it only contains 18-24 year olds (under-18s are excluded from the
survey response set). The Census marginals for "16-24" include 16-17 year
olds. The IPF grid therefore allocates Census-counted 16-17 year olds to
the "16-24" cell, but the MRP model's estimate for that cell is trained
exclusively on 18-24 respondents. The model implicitly assumes 16-17 year
olds behave identically to 18-24 year olds on all outcomes.

---

## METHODOLOGICAL CONCERN 2: Heavy reliance on Wave L variables (not pooled)

**File:** `Base R Codes/1_mapping_v43.R` (multiple blocks)

Many important variables are pulled exclusively from Wave L using
`load_wave_geo("l_indresp", ...)` rather than the multi-wave pooling
mechanism (`pull_var_dt()`). Affected variables include:

- Public transport quality (`l_locserc`)
- School quality (`l_locserap`, `l_locseras`)
- Neighbourhood cohesion items (`l_nbrcoh2`, `l_nbrcoh4`, `l_nbrsnci_dv`)
- Political participation (`l_perbfts`, `l_poleff1`, `l_poleff2`, `l_vote6`)
- Safety and crime worry (`l_crdark`, `l_crwora`)
- Physical activity (`l_vday`, `l_mday`)
- Organisation participation (`l_orga96`)
- Health service access (`l_hlsv96`)

Consequences:
1. Respondents present in Waves M/N but absent from Wave L have NA for all
   of these, reducing the effective sample.
2. These represent older data (Wave 12 vs. Wave 14 for the core variables).

---

## METHODOLOGICAL CONCERN 3: Arts/leisure subdomain rests on a single binary indicator

**File:** `Base R Codes/1_mapping_v43.R`, lines 2488-2512

The "Arts, leisure and sports" subdomain is measured solely by `l_orga96`
("None of these" for organisation membership), converted to a 0/1 indicator.
This is a thin operationalization for an entire CPI subdomain. The output
data shows MRP estimates for this subdomain ranging from approximately 1 to
3.5 on a 0-10 scale, reflecting low base rates and limited discriminatory
power.

---

## METHODOLOGICAL CONCERN 4: Unbalanced subdomain structure inflates certain measures

The CPI chain is: Proxies -> Subdomains -> Domains -> CPI, with equal
weighting at each level. This creates implicit unequal weighting of the
underlying measures. For example, within **Foundations of Prosperity**:

- "Secure Livelihoods" = mean of 5 proxy blocks (~15+ indicators)
- "An inclusive economy" = mean of 1 proxy block (3 indicators)

The domain score gives 50/50 weight to these two subdomains, so the
3 fairness/equity indicators receive as much domain-level influence as
the ~15+ Secure Livelihoods indicators combined.

---

## METHODOLOGICAL CONCERN 5: self_improvement_30plus creates age-dependent missingness

**File:** `Base R Codes/1_mapping_v43.R`, lines 1840-1868

This indicator is NA for everyone under 30. When it enters the "Lifelong
learning" subdomain (alongside "Access to skills and training"), wards with
many young people have the subdomain driven disproportionately by the other
measure. The MRP model adjusts for age composition in predictions, but the
outcome variable itself already contains this structural missingness pattern,
which the model cannot fully recover from.

---

## MRP OUTPUT FINDINGS (from poststrat_14_01_2026/)

### FINDING 1 (High Impact): Three subdomains lack meaningful ward-level discrimination

Credible interval analysis reveals that for three subdomains, the 95%
credible intervals are wider than the full range of ward-level means.
This means the model **cannot reliably distinguish or rank wards** for
these outcomes -- the uncertainty swamps the signal.

| Subdomain | Avg CI Width | Range of Ward Means | CI/Range Ratio | Assessment |
|---|---|---|---|---|
| Voice and influence | 1.05 | 0.87 | **1.21** | Cannot rank wards |
| Social relationships | 1.44 | 1.33 | **1.08** | Cannot rank wards |
| Lifelong learning | 2.06 | 2.15 | 0.96 | Borderline |
| Arts, leisure & sports | 4.49 | 5.22 | 0.86 | Wide CIs, but range is wider |

For Arts/leisure/sports, the average CI (4.49) is 1.56 times the average
mean (3.04), meaning individual ward estimates are extremely imprecise even
though there is enough range to see broad patterns.

These subdomains contribute equally to the CPI alongside much more
precisely estimated subdomains, diluting the index with noise.

### FINDING 2 (High Impact): 81 wards (11.9%) have no microdata across ALL subdomains

81 wards are entirely model-extrapolated for every single subdomain.
Their CPI scores depend solely on the MRP model's structural assumptions
and the correctness of the IPF-weighted Census covariates. For these wards,
the CPI cannot be validated against any direct survey evidence.

Two subdomains have substantially worse coverage:

| Subdomain | Wards without microdata | % of 679 |
|---|---|---|
| Arts, leisure & sports | **144** | **21.2%** |
| Voice and influence | **129** | **19.0%** |
| All other subdomains | 81-84 | 11.9-12.4% |

The 63 extra missing wards in Arts/leisure/sports (beyond the core 81)
correspond to wards where Wave L `l_orga96` had no respondents. This
is consistent with Methodological Concern 3 (single binary indicator
from a single wave).

### FINDING 3 (Medium Impact): Two models have low effective sample sizes

Two MRP models have `min_neff_ratio` below the 0.10 threshold, meaning
at least one parameter in each model has fewer than 10% effectively
independent posterior draws:

| Subdomain | min_neff_ratio | n_rows | Flag |
|---|---|---|---|
| Arts, leisure & sports | **0.055** | 2,083 | Below 0.10 |
| Voice and influence | **0.059** | 2,383 | Below 0.10 |
| Lifelong learning | 0.102 | 3,319 | Marginal |
| All others | 0.13-0.26 | 3,400-3,600 | OK |

These are the same two subdomains with the worst microdata coverage.
The low n_eff could produce noisy posterior summaries for some parameters.
All other convergence indicators are clean: zero divergent transitions
across all 12 models, and all max Rhat values below 1.01.

### FINDING 4 (Medium Impact): Extrapolated wards have substantially wider CIs

Wards without microdata have credible intervals 5-48% wider than those
with data, depending on subdomain:

| Subdomain | CI width ratio (no data / has data) |
|---|---|
| Secure Livelihoods | **1.48x** |
| Healthy neighbourhoods | 1.29x |
| Good basic education | 1.28x |
| Healthy bodies/minds | 1.22x |
| All others | 1.05-1.17x |

The Secure Livelihoods penalty is notable because this subdomain feeds
into "Foundations of Prosperity" alongside the much thinner "An inclusive
economy" subdomain (Methodological Concern 4). Extrapolated wards are
disproportionately imprecise in the richer subdomain.

### FINDING 5 (Low Impact): MCSE values are universally acceptable

All relative MCSE values (MCSE/posterior SD) are below 0.03 across all
679 wards and all 12 subdomains, well under the 0.10 concern threshold.
The MCMC sampling itself is numerically stable.

### FINDING 6 (Informational): Ward count discrepancy (679 vs 680)

The MRP outputs contain 679 wards, while the final summary CSV contains
680 rows. This is likely the City of London (E09000001), which is excluded
in Script 1 (line 421) but may be re-included by the LSOA-to-ward lookup
in Script 3. Worth verifying that all 680 summary rows have valid estimates.

---

## MINOR NOTES

1. **City of London excluded without documentation** (line 421):
   `map <- map[lad24cd != "E09000001"]` silently drops the City of London.
   Reasonable (tiny residential population) but should be documented.

2. **Ward-level random effects prior is strongly regularizing** (2_MRP_v4.R:730):
   `prior(exponential(4), ...)` for ward-level SD has mean 0.25, strongly
   smoothing ward differences. The LAD-level uses `exponential(1)` (mean 1.0).
   Sensitivity analysis recommended.

3. **na.rm = TRUE in CPI z-score averaging** (3_after_MRP_v3.R:146):
   If a ward is missing a domain z-score, its CPI is computed from fewer
   domains. Non-random missingness could introduce bias.

4. **GHQ-12 overlap in wellbeing block**: Even after fixing Bug 1,
   `n_scghq1_dv` (GHQ-12 composite) overlaps with `n_scghql` and
   `n_scghqk` (individual GHQ items). Consider using either the
   composite OR the individual items, not both.

5. **Clustering hardcoded for 5 domains** (3_after_MRP_v3.R:299):
   `stopifnot(length(dom_value_cols) == 5)` will break if domains change.
