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

---

## VARIABLE ENRICHMENT: UKHLS catalogue search for thin subdomains

A systematic search of the Understanding Society data catalogue identified
variables that could strengthen the subdomains with the weakest precision
(Arts/leisure/sports, Voice and influence, Social relationships) and address
the health service access measurement problem. For each subdomain, variables
are listed with their UKHLS names, wave availability, and integration notes.

**Key constraint:** Many enrichment candidates are in **rotating modules** that
may not be present in every wave. The Leisure, Culture and Sport module
(78 detailed activity items) was collected at waves 2 and 5 only -- **not**
at waves L(12), M(13), or N(14). Enrichment must therefore focus on variables
available in recent waves.

### 1. Arts, Leisure and Sports (currently: single binary `l_orga96`)

**Current state:** One binary indicator from Wave L. CI/Range ratio = 0.86 but
average CI is 1.56x the mean. 144 wards (21.2%) have no microdata.

**Additional variables available in recent waves:**

| Variable | Description | Waves | Type | Notes |
|---|---|---|---|---|
| `orga12` | Active in social club / working men's club | L(12) | Binary | From Groups & Organisations module |
| `orga13` | Active in sports club | L(12) | Binary | From same module |
| `club` | Sports club member | Multiple | Binary | May have broader wave availability |
| `vday` | Vigorous physical activity days (last 7 days) | L(12), M(13) | Count 0-7 | IPAQ short form; Exercise module |
| `mday` | Moderate physical activity days (last 7 days) | L(12), M(13) | Count 0-7 | IPAQ short form |
| `wday` | Walking days (last 7 days) | L(12), M(13) | Count 0-7 | IPAQ short form |
| `volun` | Volunteered in last 12 months | Even waves (2,4,6,8,10,14?) | Binary | Rotating module; may NOT be at wave 12 |
| `volfreq` | Frequency of volunteering | Same as `volun` | Ordinal | Only if `volun=1` |

**Variables NOT available in recent waves (waves 2 and 5 only):**

The detailed Leisure, Culture and Sport module (museum visits, arts events,
theatre, gallery attendance, library use, 78 activity items) was collected at
waves 2 and 5 only. These cannot be used for the current L/M/N-based analysis.

**Recommendation:**
- Add `orga12` (social club) and `orga13` (sports club) as separate binary
  indicators -- these disaggregate the `orga96` "none" flag and add information.
- Add IPAQ measures (`vday`, `mday`) from the Exercise module. These are
  available at waves 12 and 13, so they can be pooled via `pull_var_dt()`,
  increasing sample size and coverage. Physical activity is a natural component
  of "leisure and sports."
- If `volun`/`volfreq` are confirmed at wave 12 or 14, add volunteering as a
  further indicator of active community/leisure engagement.
- Even with these additions, this subdomain will remain thinner than most
  others. Consider whether the subdomain name should be narrowed to match what
  is measurable (e.g., "Active participation and physical activity").

### 2. Voice and Influence (currently: 4 Wave-L items)

**Current state:** `l_perpolinf`, `l_poleff4`, `l_opsocm`, `l_demorient` from
Wave L only. CI/Range ratio = 1.21 (worst of all subdomains). 129 wards (19.0%)
have no microdata.

**Additional variables available in UKHLS:**

| Variable | Description | Waves | Type | Notes |
|---|---|---|---|---|
| `poleff1` | "I feel qualified to participate in politics" | Rotating (wave 3, L?) | 1-5 Likert | Political Efficacy module; check wave L availability |
| `poleff2` | "Better informed about politics than most" | Same | 1-5 Likert | |
| `poleff3` | "Public officials don't care what people like me think" | Same | 1-5 Likert | Reverse-coded for positive voice |
| `vote6` | Level of interest in politics | Multiple | Ordinal | Already used in the code (`l_vote6`) |
| `vote1` | Supports a particular political party | Multiple | Binary | Party identification |
| `orga1` | Active in political party | L(12) | Binary | Groups & Organisations module |
| `perbfts` | Perceptions of benefits | L(12) | Ordinal | Already used in the code |
| `contmp` | Has contacted MP | Rotating? | Binary | Check wave availability |

**Key issue:** The code already uses `l_vote6` and `l_perbfts` elsewhere (these
are loaded from Wave L). The four items currently in the Voice subdomain
(`perpolinf`, `poleff4`, `opsocm`, `demorient`) may already be the full set
available at Wave L.

**Recommendation:**
- Check whether `poleff1`, `poleff2`, `poleff3` are available at Wave L. If so,
  adding the full political efficacy battery (4 items instead of just `poleff4`)
  would strengthen measurement.
- The primary precision lever for this subdomain is **wave pooling**: if
  `poleff` and related items are available at other waves, using `pull_var_dt()`
  instead of Wave-L-only loading would increase sample size substantially.
- Add `orga1` (political party activity) from the Groups & Organisations module
  as an additional behavioral indicator alongside the attitudinal items.

### 3. Social Relationships (currently: 4 items, narrow scales)

**Current state:** `n_scropenup`, `n_scfopenup`, `n_sclonely`, `l_scopngbhh`.
CI/Range ratio = 1.08 (cannot rank wards).

**Additional variables available in recent waves:**

| Variable | Description | Waves | Type | Notes |
|---|---|---|---|---|
| `scleftout` | How often feels left out | 9-13 | 1-3 scale | UCLA loneliness; **poolable** |
| `scisolate` | How often feels isolated from others | 9-13 | 1-3 scale | UCLA loneliness; **poolable** |
| `sclackcom` | How often feels lack of companionship | 9-13 | 1-3 scale | UCLA loneliness; **poolable** |
| `scopngbha` | "I feel like I belong to this neighbourhood" | Multiple | 1-5 Likert | Buckner's scale item |
| `scopngbhb` | Another Buckner item (psychological community) | Multiple | 1-5 Likert | |
| `scopngbhc` | Buckner: neighbouring item | Multiple | 1-5 Likert | |
| `scopngbhd` | Buckner: neighbouring item | Multiple | 1-5 Likert | |
| `scopngbhe` | Buckner: psychological community | Multiple | 1-5 Likert | |
| `scopngbhf` | Buckner: attraction to neighbourhood | Multiple | 1-5 Likert | |
| `scopngbhg` | Buckner: psychological community | Multiple | 1-5 Likert | |
| `nbrcoh_dv` | Neighbourhood Social Cohesion (PHDCN, alpha=0.8) | Multiple | 4-20 scale | Already used in "Sense of community" |
| `nbrsnci_dv` | Buckner's Neighbourhood Cohesion (alpha=0.88) | Multiple | 1-5 scale | Already used in "Sense of community" |
| `sclfsat6` | Satisfaction with social life | Multiple | 1-7 Likert | Self-completion module |
| `lfsat6` | Satisfaction with social life (BHPS version) | BHPS waves | 1-7 Likert | Use `sclfsat6` for UKHLS |

**Important note:** The `nbrcoh_dv` and `nbrsnci_dv` variables are already
used, but in the **separate** "Sense of community" subdomain (lines 2269-2356),
not in "Social relationships." The `scopngbh*` items are inputs to `nbrsnci_dv`.

**Recommendation:**
- **Highest priority:** Add `scleftout`, `scisolate`, `sclackcom` alongside
  the existing `sclonely`. These three variables form the UCLA 3-item loneliness
  scale and are available at waves 9-13, making them **poolable across waves**.
  This both enriches measurement and increases effective sample size.
- Add `sclfsat6` (satisfaction with social life) as a broader relational
  wellbeing indicator. Check wave availability -- if present in M/N, it can
  be pooled.
- Adding more `scopngbh*` items beyond `scopngbhh` could help, but these
  overlap with the "Sense of community" subdomain. Consider whether the two
  subdomains should remain separate or be merged to avoid redundancy.

### 4. Lifelong Learning (currently: borderline precision, CI/Range = 0.96)

**Current state:** Two proxy blocks: "Access to skills and training" uses
`n_jblkchb` and `n_jbxpchb`; "Self-improvement" uses `n_fenow_cawi`,
`n_fenow`, `n_feend`, `n_j1none`, `n_trainany`, `n_servuse7`, `n_jblkchb`,
`n_jbxpchb`. The self-improvement indicator is NA for under-30s.

**Additional variables:**

| Variable | Description | Waves | Type | Notes |
|---|---|---|---|---|
| `hiqual_dv` | Highest qualification ever reported | All waves | Derived ordinal | Updated each wave; could be used to detect qualification gain |
| `trainn` | Number of training periods since last interview | Multiple | Count | Only if `trainany=1` |
| `sclfsat4` | Satisfaction with: job (if employed) | Multiple | 1-7 Likert | Proxy for skill-job match |

**Key issue:** The thinness here is less about missing variables and more about
structural missingness: `self_improvement_30plus` excludes everyone under 30.
The code already uses a substantial number of education and training variables.

**Recommendation:**
- The primary fix is addressing the **age-dependent missingness** rather than
  adding more variables. Options: (a) create an all-ages version of the
  self-improvement indicator that includes young people's current education
  status; (b) use `fenow` (still in further education) and `trainany` as
  all-ages indicators in a separate proxy block.
- Consider using wave-over-wave change in `hiqual_dv` to detect active
  qualification attainment, though this requires longitudinal linkage.
- `trainn` (number of training periods) adds granularity beyond the binary
  `trainany`, but only for those who trained.

### 5. Health Service Access (currently: `l_hlsv96` -- utilization not access)

**Current state:** Uses "None of these" flag from health service use list,
inverted to create a binary access measure. This conflates utilization with
accessibility (Bug 2).

**Variables to consider:**

| Variable | Description | Waves | Type | Notes |
|---|---|---|---|---|
| `locserd` | Satisfaction with local medical/health services | L(12), possibly others | 1-4 scale | Part of `locser` battery (neighbourhood services) |
| `opserv` (numbered) | Satisfaction with specific services used | Wave-dependent | 1-7 Likert | Only for those who used the service |
| GP visit count | Number of GP visits in last 12 months | L(12) | Count | From healthserviceuselong_w12 module |
| `hlsv10` | Used hospital consultant/outpatients | L(12) | Binary | Specific service type |
| `hlsvd` | Used social worker | L(12) | Binary | Welfare service type |

**The `locser` battery:** This is a set of questions about satisfaction with
local area services. The variables are:
- `locserc` = satisfaction with public transport (confirmed; already used for
  another subdomain)
- `locserd` = satisfaction with local medical/health services (based on BHPS
  harmonisation documentation)
- `locserb`, `locsere` = other local services (shopping, schools, etc.)

**Recommendation:**
- **Replace** `hlsv96`-based measure with `locserd` (satisfaction with local
  medical/health services). This directly measures perceived accessibility
  rather than utilization, and is conceptually aligned with what the subdomain
  intends to capture.
- If `locserd` is not available or has insufficient coverage, consider the
  GP visit count as an alternative, though this still measures utilization.
  It could be conditioned on self-reported health status to better approximate
  access (e.g., GP visits relative to health need).
- The `opserv` satisfaction variables would add a quality dimension but are
  conditional on service use, creating selection bias.

---

### Summary of enrichment priorities (ranked by expected impact)

| Priority | Subdomain | Action | Expected effect |
|---|---|---|---|
| **1** | Social relationships | Add `scleftout`, `scisolate`, `sclackcom` (poolable waves 9-13) | More items + wave pooling → larger n + richer measurement |
| **2** | Arts/leisure/sports | Add `orga12`, `orga13`, `vday`, `mday` from Exercise module | 4 more indicators; IPAQ poolable across waves 12-13 |
| **3** | Voice and influence | Pool existing items across waves; add `poleff1-3` if available at wave L | Wave pooling is the main lever for precision |
| **4** | Health service access | Replace `hlsv96` with `locserd` | Fixes utilization-vs-access conflation |
| **5** | Lifelong learning | Fix age-dependent missingness in self_improvement_30plus | Removes structural NA for under-30s |

**General principle:** For all Wave-L-only variables, the single most effective
precision improvement is **wave pooling** via `pull_var_dt()`. If a variable
(or its equivalent) exists at waves M or N, pooling across waves roughly
doubles the sample size per ward, which directly reduces CI widths. This should
be investigated systematically for every Wave-L-only variable listed in
Methodological Concern 2.
