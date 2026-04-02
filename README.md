# 📊 Event-Driven Profile Ingestion Pipeline (AWS)

## Overview

This project implements an **event-driven data pipeline on AWS** that ingests profile data, enriches it using an external API (People Data Labs), and transforms it into analytics-ready datasets using a **medallion architecture (Raw → Bronze)**.

The pipeline is triggered automatically via **S3 events** and orchestrated using **AWS Lambda**.

---

## 🏗️ Architecture
```
S3 (raw/input CSV)
↓ (trigger)
Lambda
↓
Enrich via API
↓
S3 (raw/api_response JSON)
↓ (trigger)
Lambda
↓
Flatten & Transform
↓
S3 (bronze tables - CSV)
```

---

## ⚙️ Key Components

### 1. Lambda Entry Point

- Handles all S3-triggered events
- Extracts bucket and object key
- Routes processing based on file path and type

**Key behavior:**
- Only processes S3 events
- Handles multiple records per invocation
- Returns structured success/error response
- Logs full traceback for debugging

---

### 2. Event Routing Layer

The router determines which stage of the pipeline to execute:

| Condition | Action |
|----------|--------|
| `raw/input/*.csv` | Input ingestion + API enrichment |
| `raw/api_response/*.json` | Bronze transformation |
| Others | Ignored |

---

### 3. Input Ingestion Stage (`raw/input/`)

Triggered when a CSV file is uploaded.

**Steps:**
1. Retrieve API key securely from AWS Secrets Manager
2. Read CSV from S3
3. Loop through each record
4. Call People Data Labs API for enrichment
5. Aggregate results
6. Write enriched data as JSON to S3 (`raw/api_response/`)

**Output JSON includes:**
- run timestamp
- run ID
- source file reference
- record count
- enriched API results

---

### 4. API Enrichment

Each input record is enriched using:
- `first_name`
- `last_name`

This demonstrates:
- External API integration
- Batch processing
- Error-resilient design (continues per record)

---

### 5. Bronze Transformation Stage (`raw/api_response/`)

Triggered when a raw JSON file is created.

**Steps:**
1. Read raw API response
2. Extract partition date
3. Flatten nested JSON into structured datasets
4. Write datasets as CSV into S3 bronze layer

---

## 🧱 Bronze Data Model

The raw API response is flattened into 4 datasets:

### 1. `people`
- Core person-level attributes

### 2. `experience`
- Work experience (one-to-many)

### 3. `education`
- Education history (one-to-many)

### 4. `profiles`
- Social/media profiles (one-to-many)

---

## 📂 S3 Data Layout
```
data-lake/
├── raw/                          # Landing + raw API outputs
│   ├── input/                    # Source input files
│   │   └── profiles.csv
│   │
│   └── api_response/             # Enriched API responses (JSON)
│       └── date=YYYY-MM-DD/
│           └── <timestamp>_<run_id>.json
│
└── bronze/                       # Flattened, analytics-ready datasets
    ├── people/                   # Core person attributes
    │   └── date=YYYY-MM-DD/
    │       └── <run_id>.csv
    │
    ├── experience/               # Work history (1-to-many)
    │   └── date=YYYY-MM-DD/
    │       └── <run_id>.csv
    │
    ├── education/                # Education records (1-to-many)
    │   └── date=YYYY-MM-DD/
    │       └── <run_id>.csv
    │
    └── profiles/                 # Social/profile links
        └── date=YYYY-MM-DD/
            └── <run_id>.csv
```

---

## 🔐 Configuration

Environment variables:

| Variable | Description |
|--------|-------------|
| `DATA_LAKE_BUCKET` | Main S3 bucket |
| `INPUT_PREFIX` | `raw/input/` |
| `RAW_PREFIX` | `raw/api_response/` |
| `BRONZE_PREFIX` | `bronze/` |
| `PDL_SECRET_ARN` | Secrets Manager ARN for API key |

---

## 🔑 Secrets Management

- API key is retrieved dynamically from **AWS Secrets Manager**
- No hardcoding of credentials
- Demonstrates production-ready security practice

---

## 🧠 Design Decisions

### 1. Event-Driven Pipeline
- No scheduler required
- Fully reactive based on S3 uploads

### 2. Single Lambda with Routing
- Simplifies deployment
- Keeps logic centralized
- Easy to extend for more stages

### 3. Raw Data Preservation
- Stores original API response before transformation
- Enables:
  - Reprocessing
  - Debugging
  - Auditing

### 4. Medallion Architecture
- **Raw layer** → ingestion
- **Bronze layer** → structured datasets
- Easily extendable to Silver/Gold

### 5. Modular Code Design
- Separation of concerns:
  - handler → entry point
  - routes → decision logic
  - processors → business logic
  - utils → shared helpers

---

## 🚀 End-to-End Flow

1. Upload CSV → `raw/input/`
2. Lambda triggered
3. API enrichment executed
4. Raw JSON written → `raw/api_response/`
5. Lambda triggered again
6. Data flattened
7. Bronze datasets written → `bronze/`

---

## 📌 Assumptions

- Input CSV contains `first_name` and `last_name`
- Only S3-triggered events are processed
- Files must follow expected naming/prefix conventions
- Output datasets are only written if data exists

---


## 🗄️ Querying Data with Athena (Glue Catalog)

To make the bronze datasets queryable, this project includes SQL scripts under the `sql/` folder to create external tables in the **AWS Glue Data Catalog (`profiles_bronze`)**.

These tables can be created and queried using **Amazon Athena**.

---

### 📁 SQL Files

The following SQL files are provided:
```
sql/
├── people.sql
├── experience.sql
├── education.sql
└── profile.sql
```


Each file corresponds to one bronze dataset and defines:
- table schema
- S3 location
- CSV SerDe configuration
- partitioning strategy (`date`)
- partition repair command

---

### 🧱 Tables Created

| Table | Description |
|------|-------------|
| `profiles_bronze.people` | Core person-level attributes |
| `profiles_bronze.experience` | Work experience records |
| `profiles_bronze.education` | Education records |
| `profiles_bronze.profiles` | Social/profile links |

---

### ⚙️ How to Create Tables in Athena

1. Open **Amazon Athena**
2. Select your query result location (S3)
3. Glue Catalogs are Created via terraform when the bucket is created
4. Run each SQL file from the sql/ folder

---
### 📌 Partition Handling
Each table is partitioned by date, matching the S3 layout:
```
bronze/<dataset>/date=YYYY-MM-DD/<run_id>.csv
```
After creating the table, partitions are loaded using:
```
MSCK REPAIR TABLE profiles_bronze.<table_name>;
```
This scans S3 and registers available partitions automatically. This has to be done whenever new data is added. This can be automated in the future.

---
### 📊 Example Queries
```
-- Get latest people records
SELECT *
FROM profiles_bronze.people
ORDER BY date DESC
LIMIT 10;

-- Filter by partition
SELECT *
FROM profiles_bronze.experience
WHERE date = '2026-04-01'
LIMIT 10;
```

---
### 🧠 Notes
Tables are created as external tables, meaning:
1. Data remains in S3
2. No duplication of storage
3. CSV format is used with OpenCSVSerde
4. Header rows are skipped using:
    'skip.header.line.count'='1'
---
### 💡 Design Consideration
This approach ensures:
1. Data in S3 is immediately queryable
2. Clear mapping between bronze layer and analytics layer
3. Easy integration with downstream tools (Athena, Glue, BI tools)



## 🧪 What This Demonstrates

This project showcases:

- Event-driven architecture (S3 + Lambda)
- External API ingestion
- Secure secret handling
- Data lake design (medallion architecture)
- Data transformation & normalization
- Partitioned data storage for analytics
- Clean, modular Python code structure

---

## 🔮 Future Improvements

- Add retry/backoff for API calls
- Add schema validation for input CSV
- Convert bronze CSV → Parquet (better performance)
- Integrate AWS Glue for table creation
- Add monitoring (CloudWatch metrics, alerts)
- Implement Silver/Gold layers
- Add unit/integration tests

---

## 📝 Note for Interviewers

This solution focuses on demonstrating **clean architecture, best practices, and clarity of design** rather than over-engineering.

It shows how to build a scalable ingestion pipeline with:
- clear separation of stages,
- reproducible processing,
- and analytics-ready outputs.

---