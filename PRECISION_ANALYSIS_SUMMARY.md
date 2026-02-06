# Citizen Prosperity Index: Precision Analysis Summary

**Date:** February 2026
**Purpose:** Explain the ability to rank geographic areas using our survey-based estimates

---

## The Core Question

**Can we reliably rank areas from "best" to "worst" on each subdomain of the Prosperity Index?**

Short answer: **It depends on the geographic level.**

---

## Why Ranking is Challenging

Our estimates come from survey data (Understanding Society), not a census. This means:

1. **We don't have data from everyone** - only a sample of residents
2. **Each estimate has uncertainty** - a "margin of error" around it
3. **When margins of error overlap**, we can't confidently say one area is better than another

**Simple analogy:** Imagine measuring the height of two people, but your ruler is blurry. Person A measures "170-180 cm" and Person B measures "172-182 cm". You can't confidently say who is taller because the ranges overlap.

---

## How We Measured Ranking Ability

We calculated: **"What percentage of area pairs can we confidently rank?"**

- If Area A's range is 6.0-7.0 and Area B's range is 7.5-8.5, we CAN rank them (no overlap)
- If Area A's range is 6.0-7.5 and Area B's range is 7.0-8.0, we CANNOT confidently rank them (they overlap)

We used **80% confidence** as our threshold - meaning we're 80% sure Area A is truly higher/lower than Area B.

---

## Results: Three Scenarios Tested

### Scenario 1: Ward-Level Ranking (Current Approach)

**~680 wards in Greater London**

| Subdomain | Can We Rank? | % of Ward Pairs Rankable |
|-----------|--------------|--------------------------|
| Secure Livelihoods | Yes | 42% |
| Good Quality Basic Education | Yes | 40% |
| Voice and Influence | Yes | 25% |
| Healthy Neighbourhoods | Yes | 27% |
| Arts, Leisure and Sports | Yes | 25% |
| Political Inclusion | Yes | 25% |
| Lifelong Learning | Yes | 30% |
| Sense of Community | Yes | 21% |
| Inclusive Economy | Yes | 16% |
| Freedom, Choice and Control | Yes | 15% |
| Healthy Bodies and Minds | Yes | 15% |
| **Social Relationships** | **NO** | **8%** |

**Bottom line:**
- 11 out of 12 subdomains CAN be ranked at ward level, but with limited precision
- On average, only ~24% of ward pairs can be confidently ranked
- Social Relationships cannot reliably distinguish between wards

---

### Scenario 2: Domain-Level Ranking (Aggregating Subdomains)

**Still ~680 wards, but using 5 broader domains instead of 12 subdomains**

| Domain | % of Ward Pairs Rankable |
|--------|--------------------------|
| Opportunities and Aspirations | 44% |
| Foundations of Prosperity | 41% |
| Power, Voice and Influence | 31% |
| Health and Healthy Environments | 30% |
| Belonging, Connections and Leisure | 26% |

**Bottom line:**
- Modest improvement (~34% pairs rankable on average vs 24%)
- All domains can be ranked, but precision is still limited
- Not a major fix for the underlying problem

---

### Scenario 3: Local Authority (Borough) Level Ranking

**33 London boroughs instead of 680 wards**

| Subdomain | Ward Rankable | LA Rankable | Improvement |
|-----------|---------------|-------------|-------------|
| Secure Livelihoods | 42% | 78% | 1.9x better |
| Good Quality Basic Education | 40% | 74% | 1.9x better |
| Social Relationships | 8% | 55% | 7x better |
| *Average across all* | *24%* | *71%* | *3x better* |

**Bottom line:**
- **ALL 12 subdomains become reliably rankable**
- On average, 71% of borough pairs can be confidently ranked
- Even Social Relationships works at this level

---

## Why Does Borough-Level Work Better?

| Factor | Ward Level | Borough Level |
|--------|------------|---------------|
| Number of areas | ~680 | 33 |
| Survey respondents per area | Few | Many more (~20x) |
| Margin of error | Large | Small |
| Ability to distinguish areas | Poor | Good |

**Analogy:** It's like comparing the average height of 680 small groups (5 people each) vs 33 large groups (100 people each). With larger groups, the averages are more stable and easier to compare.

---

## The Trade-Off

| Approach | Pros | Cons |
|----------|------|------|
| **Ward-level** | Fine-grained detail | Can only rank ~24% of pairs; Social Relationships doesn't work |
| **Borough-level** | Reliable ranking (71% of pairs) | Loses detail within boroughs |

---

## Recommendations

### Option A: Report at Borough Level Only
- Use borough-level rankings as the primary output
- All subdomains work reliably
- Clear, defensible comparisons

### Option B: Hybrid Approach (Recommended)
1. **Borough rankings** for formal comparisons ("Camden ranks higher than Hackney")
2. **Ward-level maps** for visual exploration, with clear caveats about uncertainty
3. **Grouped ward bands** (e.g., top 20%, middle 60%, bottom 20%) instead of exact ranks

### Option C: Improve Ward-Level Precision
This would require:
- Adding more survey variables to weak subdomains (especially Social Relationships)
- Statistical adjustments to allow more variation between wards
- Longer implementation time

---

## Key Messages for Stakeholders

1. **The estimates themselves are valid** - this is about our ability to *rank* them, not whether they're correct

2. **Uncertainty is normal** in survey-based small-area estimation - we're being transparent about it

3. **Borough-level comparisons are reliable** - we can confidently compare most borough pairs

4. **Ward-level patterns are indicative** - useful for identifying potential hotspots, but not for definitive rankings

5. **Social Relationships subdomain** needs improvement before ward-level use - it currently cannot distinguish between wards

---

## Technical Note

For those interested in the methodology:

- **Metric used:** Pairwise ranking probability at 80% confidence
- **Method:** For each pair of areas, we calculate the probability that one truly exceeds the other, accounting for uncertainty in both estimates
- **Threshold:** We consider a pair "rankable" if this probability exceeds 80% (or is below 20%)
- **Model:** Multilevel Regression and Poststratification (MRP) with Bayesian estimation

---

*For questions about this analysis, please contact [your name/team].*
