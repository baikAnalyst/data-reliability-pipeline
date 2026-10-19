**[ 🌐 English ]** | [ 🇰🇷 한국어 (Korean) ](README_ko.md)

---

# 🛡️ Production PostgreSQL Data Reliability Pipeline & DQ Audit Engine

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-336791.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![PostGIS](https://img.shields.io/badge/PostGIS-Spatial-006400.svg?logo=postgis&logoColor=white)](https://postgis.net/)
[![Selenium](https://img.shields.io/badge/Selenium-Automation-43B02A.svg?logo=selenium&logoColor=white)](https://www.selenium.dev/)
[![Data Quality](https://img.shields.io/badge/DQ_Audit-16_Rules-success.svg)](https://github.com/baikAnalyst/data-reliability-pipeline)
[![Unexplained Loss](https://img.shields.io/badge/Unexplained_Loss-0_Rows-brightgreen.svg)](https://github.com/baikAnalyst/data-reliability-pipeline)
[![Competition Entry](https://img.shields.io/badge/2026_KSPO_Competition-Official_Entry-8A2BE2.svg?logo=target&logoColor=white)](https://www.kspo.or.kr/)

> **"Pipeline SUCCESS ≠ Data Integrity PASS: Verifying that data truly survived transformations as intended."**  
> 🎯 **Production Data Reliability & DQ Audit System for 'Woosimwoonkka' (2026 KSPO Competition Entry)**  
> 💡 **Woosimwoonkka ("Shall we work out, since we're bored?")** — Sports facility recommendation service across 4 disciplines (Running, Cycling, Fitness, CrossFit), fusing multi-agency public datasets (KMA, AirKorea, data.go.kr)  
> 👤 **Data Engineering & Pipeline Lead**: **Sunyoung Baik ([@baikAnalyst](https://github.com/baikAnalyst))**

## 🏗️ 1. Architecture & Multi-Schema Isolation

To guarantee uncompromising data integrity, the pipeline maintains strict logical and physical isolation across dedicated PostgreSQL schemas.

```text
 [Public Data APIs (11 Datasets)]            [Culture Big Data Platform (8 Datasets)]
               │                                            │
      (Open API Collector)                         (Selenium Headless)
               │                                            │
   Fake 200 Detection · Backoff Retry           Dynamic Modal / Session · CSV Streaming
 Full Reconciliation & JSONL Checkpoint-Resume              │
               │                                            │
  [JSON/XML Parse ➔ Staging Ingestion]         [PostgreSQL COPY ➔ Staging Ingestion]
               │                                            │
               └─────────────────────┬──────────────────────┘
                                     │
                 (Atomic Staging Isolation & Validation ➔ RAW)
                                     │
                                     ▼


 ┌────────────────────────────────────────────────────────────────────────────┐
 │                               PostgreSQL DB                                │
 │                                                                            │
 │  ┌───────────────────────────────┐      ┌───────────────────────────────┐  │
 │  │          schema: raw          │      │       schema: processed       │  │
 │  │ - Immutable Raw Source (4.08M)│      │ - 16-Rule DQ Curated (4.00M)  │  │
 │  │ - Auditing & Replay Baseline  │      │ - Service-Serving Optimized   │  │
 │  └───────────────┬───────────────┘      └───────────────▲───────────────┘  │
 │                  │                                      │                  │
 │                  ▼                                      │                  │
 │         [ Cleaning & Normalization ]                    │                  │
 │                  │                                      │                  │
 │                  ▼                                      │                  │
 │         [ 16-Rule DQ Audit & Reconciliation ] ──────────┘                  │
 │            (Full Audit Across All 19 Datasets)                             │
 │                                                                            │
 │  ┌───────────────────────────────┐      ┌───────────────────────────────┐  │
 │  │       schema: monitoring      │      │        schema: derived        │  │
 │  │ - pipeline_run_history        │      │ - PostGIS Spatial Marts       │  │
 │  │ - Immutable JSONB DQ Logs     │      │ - High-Dimension Aggregations │  │
 │  └───────────────────────────────┘      └───────────────────────────────┘  │
 └────────────────────────────────────────────────────────────────────────────┘
```

| Schema | Isolation Role & Governance Policy |
| :--- | :--- |
| **`raw`** | Retains external source records in pristine, unmodified state. Acts as the immutable baseline for replays, debugging, and regulatory auditing. |
| **`processed`** | Curated production tier holding records that have passed all 16 DQ audit rules and business constraints. Serves application queries directly. |
| **`monitoring`** | Permanent ledger of pipeline execution telemetry, stage latency profiles, incident traces, and structured JSONB DQ audit certificates. |
| **`derived`** | Specialized analytical tier holding PostGIS spatial indices (`ST_Distance`, `ST_DWithin`) and cross-domain feature aggregations. |
| **`interaction`** | Dedicated partition recording real-time user discovery paths, feedback, and engagement metrics. |

---

## ⚡ 2. High-Performance Ingestion & Streaming Bulk Load

- **Strict API Response Isolation**: Rejects blind reliance on HTTP 200 status codes. Decodes domain-level payload response codes (Result Codes) and validates structured XML/JSON envelopes to abort corrupted payloads prior to ingestion.
- **Zero Intermediate Disk Footprint**: Eliminates disk I/O bottlenecks inherent in conventional `API → CSV on Disk → DB` architectures. Payloads are buffered in memory and directly streamed into PostgreSQL.
- **PostgreSQL `COPY` Bulk Load Engine**: Replaces iterative, single-row `INSERT` statements with PostgreSQL binary streaming `COPY` protocols across all 19 datasets, eliminating transaction overhead by over 90%.
- **Atomic Transition to 16-Rule DQ Audit**: Upon completion of `raw` bulk loading, datasets immediately trigger independent cleansing, normalization, and **full reconciliation auditing** before elevation to `processed`.

#### 🎥 Live Pipeline & Scraping Demonstrations

| Target Domain | Video Link | Engineering Verification Highlights |
| :--- | :--- | :--- |
| **Public API Pipeline** | 📽️ [**API End-to-End Ingestion**](https://github.com/user-attachments/assets/b345c8a7-cd16-4115-8c8a-c8b18d2a45c9) | Payload validation ➔ PostgreSQL RAW streaming bulk load ➔ Cleansing & 16-rule DQ audit |
| **Culture Big Data Crawler** | 📽️ [**Selenium Crawling & COPY Ingestion**](https://github.com/user-attachments/assets/47a238cb-ff08-4085-8ecc-5b4f55d1c0ce) | Authenticated session maintenance ➔ Dynamic modal bypass ➔ Delta CSV resolution ➔ COPY bulk load ➔ Full DQ audit |

---

## 🛡️ 3. Fault-Tolerance & Safe Recovery

Every ingestion and transformation phase is equipped with robust failure containment mechanisms:

```text
 Error Detected ➔ Categorize Failure ➔ Exponential Backoff / Timeout ➔ Checkpoint-Resume
                                                                           │
                                                                           ▼
                                                   [ Staging Load & Row Count Verification ]
                                                                           │
                                        ┌──────────────────────────────────┴──────────────────────────────────┐
                                        ▼ (FAILED)                                                            ▼ (PASSED)
                         Discard Staging & Preserve RAW                                        Atomic Table Swap
                         (Prevents Incomplete Contamination)                                   (Zero-Downtime Live Promotion)
```

1. **Intelligent Retry & Fail-Fast Policies**:
   - Transient network timeouts and socket disconnects trigger exponential backoff retries.
   - Irrecoverable errors (e.g., daily API quota depletion, upstream schema deprecation) trigger immediate fail-fast termination to conserve computing resources.
2. **Unit-Fingerprinted Checkpoints (Resume Capability)**:
   - Stateful scrapers write transaction tokens (partition offsets) to resilient JSONL checkpoint registries, enabling instant resumption precisely at the point of failure.
3. **Atomic Staging-to-Production Swap**:
   - New batches land in staging tables. Only after row count parity, domain constraints, and primary key integrity checks pass is the table atomically swapped into production. Failed runs drop staging tables, leaving live datasets completely unharmed.

---

## 🔍 4. 16-Rule Data Quality (DQ) Audit & Reconciliation

A core architectural tenet of this pipeline is the **complete decoupling of Pipeline Success (`Exit Code 0`) from Data Integrity (`DQ PASS`)**.  
Every execution executes an automated, mathematical audit across **16 formal validation rules grouped into 4 functional layers**:

#### 📋 16-Rule Data Quality Audit Matrix

| Verification Layer | Rule Count | Audit Rules | Core Engineering Objective |
| :--- | :---: | :--- | :--- |
| **① Lineage & Tracking** | **4 Rules** | `row_tracking`<br>`id_null_check`<br>`id_duplicate_check`<br>`id_unknown_check` | Injects an immutable `_dq_row_id` into every record to enable end-to-end trace auditing from origin to storage. |
| **② Reconciliation** | **3 Rules** | `row_count_reconciliation`<br>`unexplained_loss_zero`<br>`removal_reason_audit` | Enforces mathematical conservation: $\text{RAW} = \text{PROCESSED} + \sum \text{Excluded}$. Guarantees exactly 0 unexplained row drops. |
| **③ Transition Safety** | **4 Rules** | `new_nulls_detection`<br>`null_transition_tracking`<br>`datatype_safe_casting`<br>`value_loss_prevention` | Monitors column-level null drift ($\text{RAW} \neq \text{NULL} \land \text{PROCESSED} == \text{NULL}$) to block silent data evaporation during casting. |
| **④ Domain & Spatial** | **5 Rules** | `coordinate_korea_boundary`<br>`coordinate_null_filter`<br>`semantic_text_preservation`<br>`composite_key_dedup`<br>`needs_review_flag` | Enforces ROK spatial bounds ($33^\circ \sim 39^\circ\text{N}, 124^\circ \sim 132^\circ\text{E}$), dedupes composite keys, and preserves critical non-numeric text. |

### 4.1 Row-Level Reconciliation Equation
Comparing bulk row counts alone allows dropped rows and duplicate additions to mask errors. The pipeline tracks every individual row:
- **Zero Unexplained Loss Principle**:
  $$\text{RAW Records } (4,088,065) - \text{Excluded Anomalies } (78,468) = \text{PROCESSED Records } (4,009,597)$$
- All 78,468 filtered records (1.92%) carry explicit, verifiable justification codes (e.g., coordinates outside territory, out-of-bounds telemetry, missing mandatory business keys).

---

### 4.2 Production Incident Case Studies (Data Loss Prevention)

By actively validating the pipeline against upstream domain specifications, the DQ engine intercepted three major silent data corruption hazards:

| Incident Context & Dataset | Upstream Defect / Root Cause | Mitigation via Advanced DQ Engine |
| :--- | :--- | :--- |
| **① Intercepted 25,070 Silent NULLs**<br>[National Sports Prescription Data](https://www.bigdata-culture.kr/bigdata/user/data_market/detail.do?id=2b1c565d-5f37-4152-966d-5f8094f8cf33) *(KSPO)* | Upstream column `MESURE_TME` (Session Round) was erroneously parsed as a datetime string, silently collapsing valid integers (`1, 2, 67, 1591`) into NULLs. | **`new_nulls_detection`**: Flagged unexpected null drift across the transformation boundary, halting promotion and recovering 100% of valid sessions. |
| **② Preserved Semantic Domain Text**<br>[KMA Ultra-Short-Term Weather API](https://www.data.go.kr/data/15084084/openapi.do) `getUltraSrtNcst` *(KMA)* | Precipitaton metric `RN1` includes critical non-numeric strings (`"강수없음"` / No Rain, `"< 1.0mm"`). Naive numeric casting wiped these into NULL values. | **`semantic_text_preservation`**: Segregated metric text representations and numeric values, preserving domain semantics without corrupting analytical queries. |
| **③ Intercepted 1,206 Corrupted Court Records**<br>[Open School Sports Facility Data](https://www.bigdata-culture.kr/bigdata/user/data_market/detail.do?id=ddd74830-f977-11eb-8e60-2bcdc8456bfb) *(KSOC)* | Field `ARBY_COT_CO_VALUE` contained complex dimensional text (`"570㎡ / 1 Court"`). Aggressive integer casting discarded dimension attributes. | **`value_loss_prevention`**: Detected intra-field information evaporation, isolating unstructured court strings into structured feature attributes. |

---

## 📊 5. Audit Evidence: Production Database Trail

To ensure compliance and total operational accountability, every pipeline execution writes an immutable audit record directly into `monitoring.pipeline_run_history`.

<p align="center">
  <img src="docs/images/dq_audit.png" alt="DQ Audit Execution Report" width="900">
</p>

```text
 [Pipeline Complete] ➔ [16-Rule DQ Audit & Reconciliation] ➔ [JSONB Serialization]
                                                                     │
                                                                     ▼
 ┌─────────────────────────────────────────────────────────────────────────────────┐
 │                     monitoring.pipeline_run_history (Audit DB)                  │
 │  - run_id (Unique Execution UUID)                                               │
 │  - run_status (Execution Lifecycle State) vs dq_status (Data Integrity State)   │
 │  - stage_durations (Granular Latency Profile Across All Stages)                 │
 │  - dq_results (Detailed Reconciliation Ledger, Exclusion Reasons, Rule Metrics) │
 └─────────────────────────────────────────────────────────────────────────────────┘
```

### 5.1 Audit History Schema (`monitoring.pipeline_run_history`)

| Field | Type | Description & Operational Value |
| :--- | :--- | :--- |
| **`run_id`** | `UUID` | Immutable execution key linking operational logs, trace telemetry, and webhook alerts. |
| **`dataset`** | `VARCHAR(64)` | Target dataset identifier (e.g., `weather`, `culture_bigdata`, `aed`). |
| **`run_status`** | `VARCHAR(16)` | Process lifecycle status (`SUCCESS` / `FAILED`). |
| **`dq_status`** | `VARCHAR(16)` | **Data integrity status (`CHECK_PASSED` / `CHECK_FAILED` / `NEEDS_REVIEW`).** |
| **`stage_durations`**| `JSONB` | Wall-clock latency benchmarks broken down by fetch, buffer, raw load, clean, and audit. |
| **`dq_results`** | `JSONB` | Full reconciliation breakdown including row counts, categorized exclusion counts, and rule results. |
| **`started_at` / `ended_at`** | `TIMESTAMPTZ` | Microsecond-accurate operational timestamps for SLA and latency monitoring. |

---

### 5.2 Production Audit Query

Auditors and engineers can verify both execution health and data integrity across recent runs with a single query:

```sql
-- Retrieve the latest execution state and comprehensive DQ audit payload
SELECT 
    run_id,
    dataset,
    run_status,
    dq_status,
    stage_durations,
    jsonb_pretty(dq_results) AS formatted_audit_report
FROM monitoring.pipeline_run_history 
ORDER BY started_at DESC 
LIMIT 1;
```

---

### 5.3 Live Audit Record Payload (Weather Warning Production Sample)

Actual JSONB ledger output demonstrating mathematical reconciliation:

```json
{
  "weather_warning": {
    "rows": {
      "raw_rows": 186,
      "actual_final": 128,
      "removed_rows": 58,
      "removal_reasons": {
        "weather_warning_duplicate_stn_tmfc_tmseq": 58
      },
      "row_count_matches": true,
      "unexplained_row_loss": 0,
      "explained_removed_rows": 58
    },
    "validation_summary": [
      {"rule": "row_tracking", "status": "PASS"},
      {"rule": "row_reconciliation", "status": "PASS", "expected": 128, "observed": 128},
      {"rule": "new_nulls_detection", "status": "PASS"},
      {"rule": "semantic_preservation", "status": "PASS"}
    ]
  }
}
```

$$\text{RAW Rows}(186) - \text{Excluded Duplicates}(58) = \text{Curated Rows}(128) \implies \text{Unexplained Loss} = 0$$

---

## ⏱️ 6. Automated Operations & Webhook Ops Digest

Powered by APScheduler, the pipeline coordinates multi-cadence workloads aligned with source update intervals:

| Pipeline Domain | Managed Tables (Total 19) | Cron Cadence | Synchronization Target |
| :--- | :--- | :--- | :--- |
| **`weather`** | Short-Term Observations & Forecasts (2) | 10m & 40m past hour | KMA short-term release intervals |
| **`air_quality`** | Real-time Air Quality Telemetry (1) | 15m past hour | AirKorea finalized hourly readings |
| **`weather_warning`** | Warning Alerts & Advisory Conditions (2) | Every 10 minutes | Real-time weather emergency monitoring |
| **`durunubi`** | Trail Courses, Routes, Waypoints (2) | Mon 04:00 KST | Weekly trail network updates |
| **`bicycle_accident`** | High-Risk Cycling Hazard Zones (1) | Mon 05:00 KST | KoROAD weekly accident updates |
| **`facility`** | Nationwide Sports Facilities (1) | 1st of month 04:00 KST | Monthly registry synchronization |
| **`public_open_facility`** | Public Open Facilities (1) | 1st of month 05:00 KST | Monthly open amenity releases |
| **`aed`** | AED Public Defibrillator Locations (1) | 1st of month 06:00 KST | Monthly medical apparatus registry |
| **`culture_bigdata`** | Sports Facilities, Programs, Transit (8) | 1st of month 07:00 KST | Monthly culture big data batch crawling |

---

### 6.1 Real-Time Operations Channel Integration

Rather than drowning engineers in unformatted terminal dumps, the pipeline formats alerts into actionable, structured Discord cards:

<table>
  <thead>
    <tr>
      <th align="center" width="50%">📊 Daily Operations Digest</th>
      <th align="center" width="50%">🚨 Real-Time Incident Isolation</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td valign="top">
        <img src="docs/images/webhook_digest_v4.png" alt="Daily Operations Digest" width="100%">
      </td>
      <td valign="top">
        <img src="docs/images/webhook_incident_v4.png" alt="Real-Time Incident Alert" width="100%">
      </td>
    </tr>
  </tbody>
</table>

#### 📋 Operational Mechanisms
1. **Daily Operations Digest**:
   - Aggregates overall batch executions (32 runs: 26 passed, 6 failed) while **cleanly isolating Data Quality pass rates (26 passed)**.
   - Highlights fleet average execution time (11m 53s) alongside the **peak bottleneck dataset (`culture_bigdata` at 1h 55m 46s)** to inform resource allocation.
   - Summarizes failed runs (`air_quality`, `bicycle_accident`, `bus_stop`) with explicit operational guidance.
2. **Real-Time Incident Alert**:
   - Fires within 1 second of an unexpected exception.
   - Includes failure stage (`Data Ingestion`), elapsed time (1m 40s), and the **unique execution `Run ID` (`981a0c3effff4a...`)**, enabling immediate correlation with raw error traces.
3. **Transparent Escalation & On-Call Collaboration**:
   - Broadcasts alerts into shared operational channels (`pipeline-monitoring`) rather than isolated logs, empowering cross-functional engineers (backend, data, product) to coordinate incident triage promptly.

---

## 📂 7. Repository Structure

```text
.
├── collector/                       # Public API collectors with integrated payload validation
├── culture_bigdata_selenium.py      # Resilient Selenium scraper & PostgreSQL COPY bulk loader
├── dataset_processors.py            # Dataset-specific cleansing & 16-rule DQ normalization engines
├── dq_profiler.py                   # RAW-to-PROCESSED integrity auditing & reconciliation engine
├── pipeline_elt.py                  # Multi-Schema transactional ELT orchestrator
├── pipeline_metadata.py             # Schema registries, column mappings, and DQ rule definitions
├── pipeline_monitoring.py           # Execution history logger, audit serializer, and digest generator
├── pipeline_scheduler.py            # APScheduler daemon managing multi-cadence ingest schedules
├── webhook_notifier.py              # Webhook dispatcher for operational digests and incident alerts
├── tests/                           # Automated regression, reconciliation, and audit test suite
│   ├── test_audit_evidence.py
│   ├── test_collector_checkpoint_recovery.py
│   └── test_dq_profiler.py
└── requirements.txt                 # Project dependencies
```

---

## 📄 License & Attribution

This project was built for production data reliability engineering and submitted to the **2026 KSPO Public Big Data Utilization Competition**.  
All rights reserved.





