# Engineering Design Case Studies 

A structured, interview-focused learning repository that reverse-engineers how real-world products (Twitter, YouTube, Netflix, Uber, Amazon, etc.) are built — from product requirements down to production-grade data engineering implementation details.

## Why This Exists

Most system design prep treats "design Twitter" as a backend/API exercise. This repo treats it as a **full-stack systems problem**: product thinking → backend data model → data engineering design → production LLD. The goal is to build the kind of layered reasoning that shows up in real DE interviews (fresher through intermediate) and in real production pipelines — not just memorized diagrams.

Each case study is worked through the same four levels, so patterns become reusable across companies and problems instead of being one-off memorized solutions.

## The Four Levels

Every case study is broken down through these levels, in order:

| Level | Focus | Covers |
|---|---|---|
| **1. Product Understanding** | Framing the problem | Functional & non-functional requirements, APIs, HLD, scale estimation |
| **2. Backend Data Model** | How the data is structured and served | ER diagrams, database design, read/write flow, caching strategy |
| **3. Data Engineering Design** | How data moves and is transformed | Event modeling, Kafka topics & schemas, Bronze/Silver/Gold, warehouse design, partitioning, data quality, Airflow DAGs, monitoring, KPIs |
| **4. Production-Level LLD** | How it actually runs in production | Directory/lake layout, file formats, Spark internals, consumer group design, schema evolution, retry/idempotency, checkpointing, CDC, SCD 1/2, backfills, cost optimization, security, CI/CD, testing, runbooks |

## Repository Structure

```
Production-Data-Engineering-Case-Studies/

├── 01_Twitter
├── 02_YouTube
├── 03_Netflix
├── 04_Spotify
├── 05_Airbnb
├── 06_Uber
├── 07_Amazon
├── 08_LinkedIn
├── 09_GoogleDrive
├── 10_WhatsApp
│
├── Common_Patterns
│   ├── Kafka
│   ├── Spark
│   ├── Airflow
│   ├── CDC
│   ├── Data_Modeling
│   ├── Monitoring
│   ├── Partitioning
│   └── Design_Patterns
│
└── Interview_Cheat_Sheets
```

Each individual case study folder follows the same internal template:

```
Product/
│
├── 01_Product_Requirements/
├── 02_HLD/
├── 03_Backend_LLD/
│   ├── Service_Design.md
│   ├── Class_Diagram.md
│   ├── Sequence_Diagrams.md
│   ├── Design_Patterns.md
│   ├── Database_Transactions.md
│   ├── Caching.md
│   ├── Concurrency.md
│   ├── Pagination.md
│   ├── Rate_Limiting.md
│   └── Security.md
│
├── 04_Data_Engineering/
│   ├── Event_Modeling.md
│   ├── Kafka.md
│   ├── Spark.md
│   ├── Bronze.md
│   ├── Silver.md
│   ├── Gold.md
│   ├── Warehouse.md
│   ├── Data_Quality.md
│   ├── Airflow.md
│   ├── Monitoring.md
│   └── KPIs.md
│
├── 05_Production_Operations/
│   ├── Deployment.md
│   ├── CI_CD.md
│   ├── Observability.md
│   ├── Disaster_Recovery.md
│   ├── Cost_Optimization.md
│   └── Performance_Tuning.md
│
└── README.md
```

## Flagship Projects & Difficulty

Projects are tackled roughly in increasing order of difficulty, each building on concepts from the last:

| Project | Primary Focus | Difficulty |
|---|---|---|
| 1. URL Shortener (Bitly) | Basic HLD, Backend LLD, caching, database design | ⭐ |
| 2. WhatsApp | Messaging, events, delivery, streaming | ⭐⭐ |
| 3. Twitter/X | Social feeds, fan-out, ranking | ⭐⭐⭐ |
| 4. YouTube | Media processing, CDN | ⭐⭐⭐ |
| 5. Netflix | Streaming, recommendations | ⭐⭐⭐⭐ |
| 6. Uber | Real-time location, geospatial | ⭐⭐⭐⭐ |
| 7. Airbnb | Marketplace, booking, pricing | ⭐⭐⭐⭐ |
| 8. Amazon | Large-scale commerce | ⭐⭐⭐⭐⭐ |

## Topic Priority (Where to Focus)

Not all topics carry equal interview weight. This table is used to allocate study time realistically rather than covering everything equally:

| Topic | Backend | Data Engineering | Interview Frequency |
|---|---|---|---|
| HLD | ✅ | ✅ | ⭐⭐⭐⭐⭐ |
| ER Diagram | ✅ | ✅ | ⭐⭐⭐⭐⭐ |
| Kafka Topics | ❌ | ✅ | ⭐⭐⭐⭐⭐ |
| Bronze/Silver/Gold | ❌ | ✅ | ⭐⭐⭐⭐⭐ |
| Spark Pipeline Design | ❌ | ✅ | ⭐⭐⭐⭐⭐ |
| Cache Strategy | ✅ | ✅ (context) | ⭐⭐⭐⭐ |
| Backend LLD | ✅ | ⭐ Useful context | ⭐⭐⭐ |
| Class Diagrams | ✅ | Low | ⭐⭐ |

## Prerequisites

Before starting a case study, these should be reasonably solid:

**Database Fundamentals**
SQL (advanced joins, window functions, CTEs), normalization (1NF–3NF), denormalization, indexing (clustered/non-clustered), transactions, isolation levels, partitioning basics.

**Data Engineering Fundamentals**
Batch vs streaming, ETL vs ELT, data lakes vs data warehouses, medallion architecture, star/snowflake schema, fact & dimension tables, SCD types, file formats (Parquet, ORC, Avro), partitioning.

**Distributed Systems Basics**
Not mastery — just working understanding of load balancers, CDNs, caches, replication, sharding, CAP theorem, eventual consistency, message queues, service discovery, microservices.

**Cloud Basics** (conceptual only, to start)
Object storage, compute, networking, IAM, managed databases.

## Data Engineering Pattern Catalog

The `Common_Patterns/` folder captures reusable patterns organized by pipeline layer, so a pattern learned in one case study (e.g., idempotent processing in WhatsApp) transfers directly to another (e.g., Uber's location pipeline):

- **Ingestion** — batch, streaming, CDC, API pull, webhook, Kafka Connect, Debezium, event sourcing, outbox pattern
- **Bronze** — raw preservation, immutable storage, schema-on-read, append-only, partitioning, small file management
- **Silver** — deduplication, schema validation, standardization, enrichment, null handling, late-arriving data, watermarking, exactly-once / idempotent processing
- **Gold** — star/snowflake schema, wide tables, data marts, aggregate/KPI tables, materialized views, feature tables
- **Storage** — Delta Lake, Iceberg, Hudi, lakehouse, partition evolution, compaction, Z-ordering, clustering
- **Streaming** — windowing (tumbling/sliding/session), watermarks, checkpointing, stateful/stateless processing
- **Data Quality** — freshness, completeness, uniqueness, validity, referential integrity, drift detection, quarantine tables
- **Orchestration** — DAG design, dependency management, retry strategy, backfill/catch-up, scheduling, SLA monitoring, dynamic DAGs
- **Warehouse** — SCD types 1–6, fact/dimension/bridge/snapshot tables, incremental models, surrogate keys, degenerate dimensions
- **Serving** — BI serving, semantic layer, OLAP cubes, feature store, REST APIs, reverse ETL, data sharing
- **Monitoring** — lineage, observability, metrics, logging, alerting, cost monitoring, data contracts

## Suggested Time Budget per Case Study

| Activity | Time |
|---|---|
| Discussion & discovery | 2–3 days |
| HLD | 2 days |
| Backend LLD | 2–3 days |
| Data Engineering Design | 4–5 days |
| Review & interview questions | 2 days |

**Total: ~12–15 days per case study**

## How to Use This Repo

1. Pick the next flagship project in difficulty order (start with URL Shortener if starting fresh).
2. Work through Levels 1 → 4 in sequence — don't skip to Data Engineering Design without a working Backend LLD, since DE decisions (partitioning, CDC, schema evolution) depend on the backend data model.
3. As patterns repeat across projects, extract them into `Common_Patterns/` instead of re-deriving them each time.
4. Use `Interview_Cheat_Sheets/` as the final compression step — one-page summaries per pattern for rapid pre-interview review.

## Status

🚧 Work in progress — case studies are added and refined incrementally.