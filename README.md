# Retail Loyalty Purchasing Behavior Pipeline

A Bronze → Silver → Gold pipeline on Databricks Lakeflow, turning raw retail transaction and loyalty customer data into a business-ready table.

**Business question:** How does purchasing behavior differ by customer age group, retailer, product brand, and month?

```
Source (Excel: Loyalty cardholders + Transaction Details)
   → Bronze  (raw, preserved as-is)
   → Silver  (cleaned, deduplicated, flagged — nothing deleted unless it fails an ID/invariant check)
   → Gold    (joined, filtered, aggregated for the business question)
   → Dashboard
```

## Bronze

Two streaming tables, both reading from the same uploaded file in `/Volumes/week4grouphw/bronze/raw/`:

- `bronze.loyalty_cardholders1` — Loyalty cardholders sheet
- `bronze.transaction_details1` — Transaction Details sheet

**Streaming table**, not a materialized view, because the source is meant to grow over time. Auto Loader keeps track of which files it has already ingested, so when a new file lands in that volume path and the pipeline is re-run, only the new file gets picked up — old data isn't reprocessed. (A materialized view, by contrast, fully recomputes from scratch every refresh — that's why Gold and the DQ monitoring tables below use materialized views instead: they need to look across *all* the data at once for joins and aggregates, not just append new rows.)

**Trigger:** Triggered (manual), not continuous. The pipeline only updates when it's run again — nothing streams in automatically in the background. That's a normal setup for a batch-style workload like this one.

## Silver

Both tables follow the same shape: **Deduplicate → Clean/Cast → Validate → Flag.** (Loyalty cardholders adds an Age Calculation step in between.)

**Core philosophy: flag almost everything, drop almost nothing.** A flagged row still exists in Silver with all its original context intact — Gold decides later what's actually disqualifying for the business question at hand, not Silver.

Rows are only dropped in Silver when there's no other way to identify or trust the record:

| Table | Dropped when | Why |
|---|---|---|
| `loyalty_cardholders` | `user_id` is NULL | Birthday and registered date are meaningless without knowing who they belong to — no other unique identifier exists in this table. |
| `transaction_details` | `customer_id`, `transaction_id`, `receipt_date`, and `transaction_date` are **all** missing at once | Nothing left to trace the row back to a real transaction. |
| `transaction_details` | `total_unit_price` **and** `quantity` are **both** negative at the same time | A real return only reverses one side of a sale, never both — this combination has no legitimate explanation. |

*(This is a `DROP ROW` constraint, not `FAIL UPDATE` — the offending row is excluded and the pipeline keeps running, rather than halting the whole update. Chosen deliberately over a hard stop: a double-negative row still gets caught and removed every time it occurs, without a single occurrence taking down the entire refresh.)*

Everything else becomes a warning flag. Full reference:

**`loyalty_cardholders1`**

| Column | Flag | Condition | Value |
|---|---|---|---|
| birthday | `dq_birthday_flag` | NULL | `invalid_or_missing` |
| birthday | `dq_birthday_flag` | after today | `future_date` |
| birthday | `dq_birthday_flag` | before 1920-01-01 | `implausible_year` |
| registered_date | `dq_registered_date_flag` | NULL | `invalid_or_missing` |
| registered_date | `dq_registered_date_flag` | = 1970-01-01 | `suspicious_epoch_default_date` |
| registered_date | `dq_registered_date_flag` | after now | `future_date` |
| age | `dq_age_flag` | NULL | `invalid_or_missing` |
| age | `dq_age_flag` | > 100 | `over_100` |
| age | `dq_age_flag` | < 18 | `under_18` |

**`transaction_details1`**

| Column | Flag | Condition | Value |
|---|---|---|---|
| customer_id | `dq_customer_flag` | NULL | `possible_non_loyalty_member` |
| transaction_id | `dq_transaction_id_flag` | missing | — |
| receipt_number | `dq_receipt_number_flag` | missing or non-numeric | — |
| product_sku | `dq_product_sku_flag` | NULL | `missing_sku` |
| product_brand | `dq_product_brand_flag` | missing | `missing_product_brand` |
| product_brand | `dq_product_brand_flag` | SKU classifier didn't recognize a brand at all | not flagged — nothing to compare against yet, so it isn't treated as a mismatch |
| product_brand | `dq_product_brand_flag` | original brand ≠ SKU-derived brand | `brand_mismatch` |
| product_brand | `dq_product_brand_flag` | UFC vs. its own sub-brand (Golden/Hapi/Super Fiesta) | `brand_generalized` (not treated as a real conflict) |
| receipt/transaction date | `dq_date_flags` (array — can hold more than one) | missing receipt date, missing transaction date, `1970-01-01` epoch default, future receipt/transaction date, transaction before receipt | one entry per issue found |
| total_unit_price | `dq_price_flag` | missing, negative, or zero | — |
| quantity | `dq_quantity_flag` | missing, non-integer, ≤ 0, or suspiciously high | — |
| unit_price (derived) | `dq_price_outlier_flag` | more than ±3 SD from this **specific product at this specific retailer's** own average | `suspicious_high_price` / `suspicious_low_price` |

*Price outliers are compared within `(product_sku, retailer)`, not across all retailers — different stores legitimately price the same product differently, and comparing against everyone else's price made a store's genuinely correct price look wrong just because another store sold more of it.*

## DQ Monitoring

Separate from what Gold filters — these exist so flag volume and drop rate are visible over time, regardless of what any one Gold table decides to act on.

| Table | What it tracks |
|---|---|
| `dq_drop_rate_monitor` | Bronze vs. Silver row counts and % dropped, per table |
| `dq_health_check` | Fails the pipeline once the 10% drop-rate threshold is hit |
| `dq_warnings_loyalty` | Summary counts of warning flags in `loyalty_cardholders` |
| `dq_warnings_transactions` | Summary counts of warning flags in `transaction_details` |
| `dq_silver_to_gold_exclusions_rows` | The actual rows excluded when building Gold |
| `dq_silver_to_gold_exclusions` | Summary of how many rows were excluded, and why |

**Stop conditions:**
- **< 5% of rows dropped** → normal, no action
- **5–10% dropped** → warning
- **≥ 10% dropped** → pipeline fails via `dq_health_check`, requires investigation before trusting the run

## Gold

`gold` is scoped to **loyalty members only** — `age_group` can't exist without a loyalty profile, so a guest transaction can't meaningfully answer the "by age group" part of the business question. This means `total_revenue` in Gold represents loyalty-segment revenue, not company-wide revenue.

Rows are excluded from Gold when:

- `customer_id` is NULL (non-loyalty)
- `quantity` is missing, negative, or zero
- `total_unit_price` is missing or negative
- `dq_date_flags` contains `missing_transaction_date`, `suspicious_epoch_default_date`, `future_receipt_date`, or `future_transaction_date` — these would put the row in the wrong month bucket, which directly corrupts a required dimension

Rows are **kept** in Gold even when flagged, when the flag doesn't threaten anything Gold actually reports:

- `transaction_before_receipt` — internal inconsistency, doesn't affect which month the row belongs to
- `missing_receipt_date` alone — Gold groups by `transaction_date`, not `receipt_date`, so this doesn't affect month bucketing
- Brand issues, missing `transaction_id`, receipt number formatting — don't touch any of Gold's dimensions or metrics

**Still under review:** whether `dq_price_outlier_flag` rows should eventually be excluded too. For now they're kept and just monitored, since the detection method needs manual verification against a small dataset before trusting it to remove data automatically.

## Tech Stack

Databricks Lakeflow Declarative Pipelines (`STREAMING TABLE`, `MATERIALIZED VIEW`, `CONSTRAINT ... EXPECT`), Databricks SQL, Unity Catalog. Source ingested via `read_files(..., format => 'excel')`.

## Open Items

- [ ] Expand SKU regex patterns as new, unrecognized SKUs show up — the classifier won't catch every brand/type/weight pattern on day one by design; check `WHERE brand_from_sku IS NULL OR product_type IS NULL` periodically to find candidates for new rules
- [ ] Finish reviewing flagged price outliers; decide on detection method (mean/SD vs. median-ratio) and whether to exclude from Gold
- [ ] Pick a final cutoff for implausibly young ages in the loyalty roster


