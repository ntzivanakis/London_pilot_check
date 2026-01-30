# Code Review: Citizen Prosperity Index Pipeline

## Summary

Review of the three-stage pipeline (data preparation, MRP, post-MRP aggregation)
for creating a Citizen Prosperity Index from Understanding Society (UKHLS) data.

Two code bugs were identified, along with several methodological concerns.

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
