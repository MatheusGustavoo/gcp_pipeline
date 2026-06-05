# ELT Data Pipeline on GCP

A production-grade ELT pipeline built on **Google Cloud Platform**, processing transaction data through automated quality checks with dbt transformations, and data governance using Dataplex.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Pipeline Flow](#pipeline-flow)
  - [Step 1 — Ingest: GCS → BigQuery](#step-1--ingest-gcs--bigquery)
  - [Step 2 — Test Raw Data (Cloud Run Job 1)](#step-2--test-raw-data-cloud-run-job-1)
  - [Step 3 — Transform Data (Cloud Run Job 2)](#step-3--transform-data-cloud-run-job-2)
  - [Step 4 — Test Transformed Data (Cloud Run Job 3)](#step-4--test-transformed-data-cloud-run-job-3)
  - [Step 5 — Data Quality & Profiling (Dataplex)](#step-5--data-quality--profiling-dataplex)
- [Data Model](#data-model)
  - [Source Schema (raw_data)](#source-schema-raw_data)
  - [Transformed Schema (transformed_data)](#transformed-schema-transformed_data)
- [dbt Configuration](#dbt-configuration)
- [Orchestration — Apache Airflow (Cloud Composer)](#orchestration--apache-airflow-cloud-composer)
  - [ELT Pipeline DAG](#elt-pipeline-dag)
  - [Data Quality DAG](#data-quality-dag)
- [Data Governance with Dataplex](#data-governance-with-dataplex)
- [Event-Driven Trigger — Cloud Run Function](#event-driven-trigger--cloud-run-function)
- [CI/CD — Cloud Build](#cicd--cloud-build)
- [Setup & Deployment](#setup--deployment)
- [Prerequisites](#prerequisites)

---

## Architecture Overview

```
GCS Bucket (raw CSV files)
        │
        ▼  [Cloud Run Function — event-driven trigger]
        │
        ▼
Cloud Composer (Airflow DAG: elt_bank_data_pipeline)
        │
        ├── Load to BigQuery (GCS → staging.raw_data)
        │
        ├── Move files to /proceed folder
        │
        ├── [Task Group 1] Test Raw Data
        │       └── Cloud Run Job → dbt test (source:data_raw)
        │
        ├── [Task Group 2] Transform Data
        │       └── Cloud Run Job → dbt run (data-raw model)
        │
        ├── [Task Group 3] Test Transformed Data
        │       └── Cloud Run Job → dbt test (data-transformed)
        │
        └── Trigger Dataplex DAG
                ├── Data Quality Scan → results exported to BigQuery
                └── Data Profile Scan
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud Platform | Google Cloud Platform (GCP) |
| Orchestration | Cloud Composer (Apache Airflow) |
| Transformation | dbt Core + dbt-bigquery |
| Data Warehouse | BigQuery |
| Containerization | Docker + Cloud Run Jobs |
| CI/CD | Cloud Build |
| Data Governance | Dataplex (Quality Scans, Profile Scans, Custom Aspects) |
| Event Trigger | Cloud Run Functions |
| Storage | Google Cloud Storage (GCS) |
| Logging | Google Cloud Logging |
| Notifications | Email (SMTP via Airflow) |

---

## Project Structure

```
.
├── dag-elt-pipeline.py                          # Main Airflow DAG (ELT pipeline)
├── data-quality-dag.py                          # Airflow DAG (Dataplex quality & profile scans)
│
├── bigquery-dbt/
│   ├── cloud-run-job-1-test-raw-data/           # dbt tests on raw data
│   │   ├── Dockerfile.test
│   │   ├── cloudbuild.test.raw.yml
│   │   ├── dbt_project.yml
│   │   ├── packages.yml
│   │   ├── profiles.yml
│   │   ├── requirements.txt
│   │   └── models/
│   │       └── sources.yml                      # Source schema + raw data tests
│   │
│   ├── cloud-run-job-2-transform-data/          # dbt transformation models
│   │   ├── Dockerfile.transform
│   │   ├── cloudbuild.transform.yml
│   │   ├── dbt_project.yml
│   │   ├── packages.yml
│   │   ├── profiles.yml
│   │   ├── requirements.txt
│   │   └── models/
│   │       └── data-raw/
│   │           ├── raw_data.sql                 # Main transformation model
│   │           └── schema.yml
│   │
│   └── cloud-run-job-3-test-transformed-data/   # dbt tests on transformed data
│       ├── Dockerfile.test.transformed
│       ├── cloudbuild.test.transformed.yml
│       ├── dbt_project.yml
│       ├── packages.yml
│       ├── profiles.yml
│       ├── requirements.txt
│       ├── models/
│       │   └── data-transformed/
│       │       └── schema.yml                   # Transformed data tests
│       └── tests/
│           └── transactions_id_not_null_transformed.sql
│
├── cloud-run-function/
│   ├── function-composer.py                     # Cloud Function: GCS event → Airflow DAG trigger
│   └── requirements.txt
│
└── dataplex/
    ├── data-classification.sh                   # Custom aspect: sensitivity classification
    ├── data-freshness.sh                        # Custom aspect: update frequency metadata
    ├── data-sla-performance.sh                  # Custom aspect: SLA & performance metrics
    └── data-stewardship-info.sh                 # Custom aspect: ownership & stewardship
```

---

## Pipeline Flow

### Step 1 — Ingest: GCS → BigQuery

A CSV file named with the prefix `bank_data` is uploaded to the GCS bucket `elt-dev`. This event automatically triggers a **Cloud Run Function**, which matches the filename against a set of regex rules and triggers the Airflow DAG `elt_bank_data_pipeline`.

The DAG begins by:
1. Listing all matching objects in the bucket using `GCSListObjectsOperator`
2. Loading them into `your-project-id.staging.raw_data` via `GCSToBigQueryOperator` (schema autodetect, `WRITE_TRUNCATE`)
3. Moving the processed files to a `proceed/` folder to avoid reprocessing

---

### Step 2 — Test Raw Data (Cloud Run Job 1)

A containerized dbt project runs `dbt test --select source:data_raw` against the freshly loaded `staging.raw_data` table.

Tests validated on raw data:

| Column | Tests |
|---|---|
| ID1 (transaction_id) | `not_null` |
| ID2 (merchant_id) | `not_null` |
| ID3 (customer_id) | `not_null` |
| String1 (business_category) | `not_null`, `accepted_values`: Retail, Wholesale, Online, Subscription, B2B, Marketplace |
| String2 (currency_code) | `not_null`, `accepted_values`: EUR, USD, GBP, PLN, SEK, NOK |
| String3 (reference_token) | `not_null`, `unique` |
| Float1 (gross_amount) | `not_null` |
| Float2 (fee_amount) | `not_null` |
| Float3 (net_amount) | `not_null` |
| Status | `not_null`, `accepted_values`: Settled, Pending, Failed, Reversed, Cancelled, In Review |

---

### Step 3 — Transform Data (Cloud Run Job 2)

A containerized dbt project runs `dbt run --select data-raw`, executing the `raw_data.sql` model which materializes as a table `bank_data_dev.transformed_data`.

**Transformations applied:**

- **Field renaming:** Generic names (ID1, String1, Float1…) are renamed to semantic names (`transaction_id`, `business_category`, `gross_amount`…)
- **Normalization:** String fields are trimmed and uppercased
- **Derived fields:**
  - `tax_amount` — 10% of gross amount
  - `calculated_net` — gross minus fee (for validation)
  - `final_status` — simplified status mapping (Settled → Completed, Pending/In Review → Pending, etc.)
  - `fee_percentage` — fee as a percentage of gross amount
  - `currency_region` — geographic grouping by currency (Europe, North America, Nordic/Eastern Europe)
  - `commerce_type` — high-level grouping (Physical Commerce, Digital Commerce, Business Services)
- **Data quality flags:**
  - `net_amount_mismatch_flag` — net amount deviates from gross − fee by more than 0.01
  - `fee_exceeds_amount_flag` — fee amount exceeds gross amount
- **Metadata:** `dbt_loaded_at` timestamp

---

### Step 4 — Test Transformed Data (Cloud Run Job 3)

A containerized dbt project runs `dbt test --select data-transformed` against `bank_data_dev.transformed_data`.

Tests include column-level validations (not_null, accepted_values, uniqueness) as well as table-level expression tests using `dbt_utils`:

| Test | Severity |
|---|---|
| `fee_percentage <= 5.0 OR gross_amount = 0` | warn |
| `gross_amount >= 0 AND fee_amount >= 0` | error |
| `net_amount <= gross_amount` | error |
| `gross_amount` between 0 and 1,000,000 | warn |
| `fee_percentage` between 0 and 5 | warn |

Custom singular test: `transactions_id_not_null_transformed.sql` directly queries the source table for null `reference_token` values.

---

### Step 5 — Data Quality & Profiling (Dataplex)

After all dbt tests pass, the ELT DAG triggers `dataplex_etl_with_quality_checks_and_profile_scan`. This secondary DAG runs two parallel scan groups:

**Data Quality Scan** — rules defined via the Dataplex API:

| Rule | Dimension | Column | Threshold |
|---|---|---|---|
| transaction-id-not-null | COMPLETENESS | transaction_id | 100% |
| unique-transaction-id | UNIQUENESS | transaction_id | 100% |
| reference-token-not-null | COMPLETENESS | reference_token | 100% |
| unique-reference-token | UNIQUENESS | reference_token | 100% |
| customer-id-not-null | COMPLETENESS | customer_id | 100% |
| fee-percentage-in-range | VALIDITY | fee_percentage | 0–100 |

Results are exported to `bank_data_dev.dq_results` in BigQuery and sent via email.

**Data Profile Scan** — a default profile scan is run to generate column-level statistics (min, max, nullability, distinct counts, etc.) for the transformed table.

---

## Data Model

### Source Schema (`raw_data`)

| Column | Type | Description |
|---|---|---|
| ID1 | STRING | Transaction identifier |
| ID2 | STRING | Merchant identifier |
| ID3 | STRING | Customer identifier |
| String1 | STRING | Business category |
| String2 | STRING | Currency code |
| String3 | STRING | Reference token (unique) |
| Float1 | NUMERIC | Gross transaction amount |
| Float2 | NUMERIC | Fee amount |
| Float3 | NUMERIC | Net amount |
| Status | STRING | Transaction status |

### Transformed Schema (`transformed_data`)

| Column | Type | Description |
|---|---|---|
| transaction_id | STRING | Cleaned transaction ID |
| merchant_id | STRING | Cleaned merchant ID |
| customer_id | STRING | Cleaned customer ID |
| business_category | STRING | Uppercase category |
| currency_code | STRING | Uppercase ISO currency |
| reference_token | STRING | Unique reference |
| gross_amount | NUMERIC | Gross amount |
| fee_amount | NUMERIC | Fee amount |
| net_amount | NUMERIC | Net amount |
| transaction_status | STRING | Cleaned status |
| tax_amount | NUMERIC | 10% of gross amount |
| calculated_net | NUMERIC | gross − fee |
| final_status | STRING | Simplified status |
| fee_percentage | NUMERIC | Fee % of gross |
| currency_region | STRING | Geographic grouping |
| commerce_type | STRING | Commerce category |
| net_amount_mismatch_flag | BOOLEAN | Net amount discrepancy flag |
| fee_exceeds_amount_flag | BOOLEAN | Fee > gross flag |
| dbt_loaded_at | TIMESTAMP | Load timestamp |

---

## dbt Configuration

Each Cloud Run Job contains its own isolated dbt project with the following structure:

**`dbt_project.yml`** — project name `dbt_core`, profile `bigquery-dbt`

```yaml
models:
  dbt_core:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
```

**`profiles.yml`** — connects to BigQuery via `oauth` using the Cloud Run service account. Target dataset: `bank_data_dev`, region: `EU`.

**`packages.yml`** — third-party dbt packages used:

```yaml
- dbt-labs/dbt_utils: 1.1.1
- calogica/dbt_expectations: 0.10.1
```

---

## Orchestration — Apache Airflow (Cloud Composer)

### ELT Pipeline DAG

**DAG ID:** `elt_bank_data_pipeline`  
**Schedule:** Event-driven (triggered by Cloud Run Function)  
**Catchup:** Disabled

```
list_gcs_objects
        │
        ▼
load_to_bigquery
        │
        ▼
move_gcs_files_to_proceed
        │
        ▼
[test_raw_data_group]
 ├── execute_test_raw_data_job
 ├── log_test_raw_success / log_test_raw_failure
 └── join_raw
        │
        ▼
[transform_data_group]
 ├── execute_transform_job
 ├── log_transform_success / log_transform_failure
 └── join_transform
        │
        ▼
[test_transformed_data_group]
 ├── execute_transformed_data_test_job
 ├── log_test_success / log_test_failure
 └── join_test
        │
        ├── log_pipeline_success → send_success_email → trigger_data_quality_dag
        └── send_failure_email
```

Each Cloud Run Job operator (`CloudRunExecuteJobOperator`) is wrapped in a Task Group with success/failure logging branches using `trigger_rule`. The final step uses `TriggerDagRunOperator` in deferrable mode to trigger the Dataplex DAG without blocking a worker slot.

### Data Quality DAG

**DAG ID:** `dataplex_etl_with_quality_checks_and_profile_scan`  
**Schedule:** None (triggered by ELT DAG)

Runs two independent Task Groups in sequence:
- `data_quality_scan_group` — creates/updates scan definition, runs the scan, fetches results, sends email report
- `data_profile_scan_group` — creates/updates profile scan definition, runs the scan, fetches results

---

## Data Governance with Dataplex

Four custom **Aspect Types** are defined via shell scripts using the Dataplex REST API, enabling metadata tagging on any BigQuery table in the data catalog:

| Aspect | Fields |
|---|---|
| **Data Classification** (`data-classification`) | sensitivity_level (required), business_unit, cost_center |
| **Data Freshness** (`data-freshness`) | last_updated_timestamp, update_frequency, expected_next_update |
| **SLA & Performance** (`sla-and-performance`) | sla_uptime_percent, max_latency_minutes, support_contact |
| **Data Stewardship** (`data-stewardship-info`) | data_owner_email (required), steward_team, last_reviewed_date |

To create the aspect types, replace `your-project-id` and run:

```bash
bash dataplex/data-classification.sh
bash dataplex/data-freshness.sh
bash dataplex/data-sla-performance.sh
bash dataplex/data-stewardship-info.sh
```

---

## Event-Driven Trigger — Cloud Run Function

**Entry point:** `trigger_dag`

The function listens to GCS object creation events. When a file is uploaded to the `elt-dev` bucket, it:

1. Extracts the object name from the event payload
2. Skips files already in the `proceed/` folder
3. Matches the filename against regex rules (e.g. `bank_data` → `elt_bank_data_pipeline`)
4. Calls the Airflow REST API (`POST /api/v1/dags/{dag_id}/dagRuns`) with the file metadata as DAG config

Authentication is handled via `google.auth.default` with the `AuthorizedSession`, using the function's service account identity.

---

## CI/CD — Cloud Build

Each Cloud Run Job has a dedicated `cloudbuild.yml` file that automates build and deployment:

```
Build Docker image → Push to GCR → Deploy to Cloud Run Jobs
```

| Job | Image | Cloud Run Job Name |
|---|---|---|
| Test Raw Data | `gcr.io/$PROJECT_ID/dbt-test` | `dbt-test-job` |
| Transform Data | `gcr.io/$PROJECT_ID/dbt-transform` | `dbt-transform-job` |
| Test Transformed | `gcr.io/$PROJECT_ID/dbt-test-transformed` | `dbt-test-transformed-job` |

All jobs are deployed to region `europe-west1` with 2Gi memory and a 30-minute task timeout.

---

## Setup & Deployment

**1. Configure your project ID**

Replace `your-project-id` in all `profiles.yml`, `schema.yml`, `dag-elt-pipeline.py`, `data-quality-dag.py`, and Dataplex scripts.

**2. Deploy Cloud Run Jobs**

```bash
# From each cloud-run-job-* directory:
gcloud builds submit --config cloudbuild.<name>.yml .
```

**3. Deploy the Cloud Run Function**

```bash
cd cloud-run-function
gcloud functions deploy trigger_dag \
  --runtime python311 \
  --trigger-event google.storage.object.finalize \
  --trigger-resource elt-dev \
  --entry-point trigger_dag \
  --region europe-west1
```

**4. Upload DAGs to Cloud Composer**

```bash
gcloud composer environments storage dags import \
  --environment <your-composer-env> \
  --location europe-west1 \
  --source dag-elt-pipeline.py

gcloud composer environments storage dags import \
  --environment <your-composer-env> \
  --location europe-west1 \
  --source data-quality-dag.py
```

**5. Create Dataplex Aspect Types**

```bash
bash dataplex/data-classification.sh
bash dataplex/data-freshness.sh
bash dataplex/data-sla-performance.sh
bash dataplex/data-stewardship-info.sh
```

**6. Trigger the pipeline**

Upload a CSV file named with prefix `bank_data` to the `elt-dev` GCS bucket. The Cloud Run Function will automatically trigger the Airflow DAG.

---

## Prerequisites

- GCP Project with billing enabled
- APIs enabled: BigQuery, Cloud Run, Cloud Build, Cloud Composer, Cloud Functions, Dataplex, Cloud Storage, Cloud Logging
- Cloud Composer environment (Airflow 2.x) in `europe-west1`
- Service accounts with appropriate IAM roles:
  - Cloud Run Jobs: BigQuery Data Editor, BigQuery Job User
  - Cloud Composer: Cloud Run Admin, BigQuery Admin, GCS Object Admin
  - Cloud Run Function: Composer User, Cloud Logging Writer
- SMTP connection configured in Airflow (`smtp_default`) for email notifications
- BigQuery dataset `staging` with table `raw_data` accessible to the dbt service account
