# Retail Loyalty Purchasing Behavior Pipeline

A Bronze → Silver → Gold data quality pipeline on Databricks Lakeflow that transforms raw retail transaction and loyalty customer data into business-ready analytics tables.

**Business question:** How does purchasing behavior differ by customer age group, retailer, product brand, and month?

Source (Excel: Loyalty cardholders + Transaction Details) → Bronze (raw ingestion, preserved as-is) → Silver (cleaned, deduplicated, validated with DQ flags) → Gold (two tables: detail-level + pre-aggregated) → Dashboard


---

## Bronze Layer

Two streaming tables, each reading from its own subfolder in `/Volumes/week4grouphw/bronze/raw/`:

- **`bronze.loyalty_cardholders1`** — Customer profiles from Loyalty cardholders sheet  
  Source: `/Volumes/week4grouphw/bronze/raw/loyalty_cardholders/`
  
- **`bronze.transaction_details1`** — Purchase records from Transaction Details sheet  
  Source: `/Volumes/week4grouphw/bronze/raw/transaction_details/`

**Why Streaming Tables?** Auto Loader tracks which files have been ingested. When new files arrive, only new data is processed — no full recomputation. (Unlike materialized views, which recompute from scratch each refresh.)

**Trigger:** Manual (triggered mode). Pipeline updates only when explicitly run.

---

## Silver Layer

**Philosophy: Flag everything, drop only critical issues.**

Both tables follow: **Deduplicate → Clean/Cast → Validate → Flag**

Rows are flagged for data quality issues but **kept in Silver** with full context intact. Gold decides what's actually disqualifying for business analysis — Silver preserves everything possible.

### Drop Conditions (Constraint-Based)

Rows are **only dropped** when there's no way to identify or trust the record:

| Table | Dropped when | Rationale |
|---|---|---|
| `loyalty_cardholders` | `user_id` IS NULL | No way to identify who this profile belongs to |
| `transaction_details` | `customer_id`, `transaction_id`, and **both dates** are ALL NULL | Insufficient identity — can't trace back to a real transaction |
| `transaction_details` | `quantity` < 0 **AND** `total_unit_price` < 0 (both negative) | Possible data corruption — returns/refunds only reverse one of these, never both |

### Data Quality Flags

Everything else becomes a **warning flag** for downstream inspection:

**`loyalty_cardholders1` Flags:**

| Column | Flag Column | Condition | Flag Value |
|---|---|---|---|
| birthday | `dq_birthday_flag` | NULL | `invalid_or_missing` |
| birthday | `dq_birthday_flag` | Future date | `future_date` |
| birthday | `dq_birthday_flag` | Before 1920 | `implausible_year` |
| registered_date | `dq_registered_date_flag` | NULL | `invalid_or_missing` |
| registered_date | `dq_registered_date_flag` | Epoch default (1970-01-01) | `suspicious_epoch_default_date` |
| registered_date | `dq_registered_date_flag` | Future date | `future_date` |
| age | `dq_age_flag` | NULL | `invalid_or_missing` |
| age | `dq_age_flag` | > 100 | `over_100` |
| age | `dq_age_flag` | < 18 | `under_18` |

**`transaction_details1` Flags:**

| Column | Flag Column | Condition | Flag Value |
|---|---|---|---|
| customer_id | `dq_customer_flag` | NULL | `possible_non_loyalty_member` |
| transaction_id | `dq_transaction_id_flag` | NULL | `missing_transaction_id` |
| receipt_number | `dq_receipt_number_flag` | Missing or non-numeric |`missing_receipt_number`, `non_numeric_receipt_number`  |
| product_sku | `dq_product_sku_flag` | NULL | `missing_sku` |
| product_brand | `dq_product_brand_flag` | NULL | `missing_product_brand` |
| product_brand | `dq_product_brand_flag` | Mismatch with SKU-derived brand | `brand_mismatch` |
| product_brand | `dq_product_brand_flag` | UFC sub-brand conflict | `brand_generalized` |
| dates | `dq_date_flags` (array) | Missing dates, epoch defaults, future dates, transaction before receipt | (multiple flags per row possible) |
| total_unit_price | `dq_price_flag` | Missing, negative, or zero | (flagged) |
| quantity | `dq_quantity_flag` | Missing, non-integer, ≤ 0, suspiciously high | (flagged) |
| unit_price | `dq_price_outlier_flag` | ±3 SD from this product's average **at this retailer** | `suspicious_high_price` / `suspicious_low_price` |

*Price outliers are detected within `(product_sku, retailer)` groups — different retailers legitimately price the same product differently.*

---

## Data Quality Monitoring

**6 consolidated monitoring tables** track constraint violations and analytical exclusions across the pipeline:

| Table | Purpose |
|---|---|
| **`dq_pipeline_funnel`** | Complete Bronze → Silver → Gold funnel with row counts, duplicates, constraint drops (dual negatives, insufficient identity), and analytical exclusions |
| **`dq_health_check`** | **FAILS pipeline** if Bronze → Silver drop rate ≥ 10% (5–10% warns) |
| **`dq_warnings_loyalty`** | Warning flag counts for `loyalty_cardholders` |
| **`dq_warnings_transactions`** | Warning flag counts for `transaction_details` |
| **`dq_excluded_rows_all_stages`** | **Row-level inspection**: Every dropped/excluded row across Bronze → Silver → Gold with reason flags |

### Pipeline Health Thresholds

| Drop Rate | Status | Action |
|---|---|---|
| < 5% | OK | Normal operation |
| 5–10% | WARNING | Monitor closely |
| ≥ 10% | **STOP** | Pipeline fails via `dq_health_check` constraint — investigate before trusting results |

---

## Gold Layer

**Two-table strategy** for the business question:

### 1. `gold.loyalty_purchases` (Detail-Level)
* **Granularity:** One row per product line item (transaction_id + product_sku)
* **Purpose:** Flexible dashboard exploration, drill-downs, custom aggregations
* **Use for:** Ad-hoc analysis, filtering on any dimension, transaction-level detail

### 2. `gold.purchasing_behavior` (Pre-Aggregated)
* **Granularity:** One row per `(age_group, retailer, product_brand, transaction_month)` combination
* **Purpose:** Fast dashboard performance for the specific business question
* **Pre-computed metrics:**
  - `transaction_count` — Unique transactions
  - `product_line_items` — Total line items
  - `unique_customers` — Distinct customers
  - `total_quantity_sold` — Total items sold
  - `total_revenue` — Total revenue
  - `avg_revenue_per_transaction` — Average transaction value
  - `avg_items_per_transaction` — Average basket size

### Scope & Exclusions

**Gold is scoped to loyalty members only** — guest transactions (NULL `customer_id`) are excluded because age group analysis requires a loyalty profile. Revenue in Gold represents loyalty-segment revenue, not company-wide.

**Rows are excluded from Gold when:**

* `customer_id` IS NULL (non-loyalty transaction)
* `quantity` is NULL, ≤ 0
* `total_unit_price` is NULL or < 0
* `product_sku` is NULL
* `dq_date_flags` contains any of:
  - `missing_transaction_date`
  - `suspicious_epoch_default_date`
  - `future_receipt_date`
  - `future_transaction_date`
  
  *(These would put the row in the wrong month bucket, corrupting a required dimension)*
* `dq_price_outlier_flag` is set (under review — currently monitored but not excluded)

**Rows are KEPT even when flagged if:**

* `transaction_before_receipt` — internal inconsistency but doesn't affect month bucketing
* `missing_receipt_date` alone — Gold groups by `transaction_date`, not `receipt_date`
* Brand issues, missing `transaction_id`, receipt formatting — don't affect Gold dimensions or metrics
---

## Tech Stack

* **Platform:** Databricks Lakeflow Spark Declarative Pipelines
* **Storage:** Unity Catalog (Delta tables)
* **Ingestion:** Auto Loader (`read_files(..., format => 'excel')`)
* **Monitoring:** Constraint-based data quality (`CONSTRAINT ... EXPECT`)
* **Languages:** Databricks SQL

---

## Open Items

- [ ] Expand SKU regex patterns as new unrecognized SKUs appear — check `WHERE brand_from_sku IS NULL` periodically to find candidates for new rules
- [ ] Finalize price outlier detection method (mean/SD vs. median-ratio) and decide whether to exclude from Gold
- [ ] Establish final cutoff for implausibly young ages in loyalty roster
- [ ] Validate that `purchasing_behavior` aggregations match dashboard requirements before finalizing pre-computation grain

---

## Repository Structure
/Week4HW/ ├── transformations/ │ ├── Bronze.sql # Raw ingestion (streaming tables) │ ├── Silver.sql # Cleaning, deduplication, validation │ ├── gold.sql # Business analytics tables │ └── dq_monitoring.sql # Data quality monitoring views └── /Volumes/week4grouphw/bronze/raw/ └── [source Excel files]
