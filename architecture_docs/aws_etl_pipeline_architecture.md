# AWS Event-Driven ETL Pipeline — Architecture & Deep Dive

## Table of Contents

1. [Overview](#overview)
2. [Architecture Diagram (Conceptual)](#architecture-diagram)
3. [Component Breakdown](#component-breakdown)
   - [Amazon S3 — Source & Sink](#amazon-s3--source--sink)
   - [Amazon EventBridge — Event Router](#amazon-eventbridge--event-router)
   - [AWS Step Functions — Orchestrator](#aws-step-functions--orchestrator)
   - [Amazon SQS — Dead Letter Queue](#amazon-sqs--dead-letter-queue)
   - [AWS Glue — ETL Engine](#aws-glue--etl-engine)
   - [AWS Glue Data Catalog & Athena](#aws-glue-data-catalog--athena)
4. [ETL Script Deep Dive](#etl-script-deep-dive)
   - [Job Arguments & Initialization](#job-arguments--initialization)
   - [Delimiter Corruption Check (Pre-Spark)](#delimiter-corruption-check-pre-spark)
   - [Dynamic Frame Loading](#dynamic-frame-loading)
   - [Spark-Level Corruption Check](#spark-level-corruption-check)
   - [SQL Transformation Query](#sql-transformation-query)
   - [Drop Null Fields](#drop-null-fields)
   - [Data Quality Rules](#data-quality-rules)
   - [Silver Layer Sink & Catalog Registration](#silver-layer-sink--catalog-registration)
5. [Step Functions State Machines](#step-functions-state-machines)
   - [orchestrate\_data\_ingestion](#orchestrate_data_ingestion)
   - [replay\_failed\_ingestion](#replay_failed_ingestion)
6. [End-to-End Data Flow](#end-to-end-data-flow)
7. [Error Handling & Resilience Strategy](#error-handling--resilience-strategy)
8. [Data Lakehouse Layers](#data-lakehouse-layers)

---

## Overview

This pipeline is a fully **event-driven, serverless ETL system** on AWS. When a CSV file is uploaded to an S3 bucket, the pipeline automatically triggers, validates, transforms, and registers the data into the AWS Glue Data Catalog — making it immediately queryable via Amazon Athena without any manual intervention.

The architecture follows a **Medallion / Lakehouse pattern**: raw CSV data lands in a "raw" S3 prefix and gets processed into a "silver" layer stored as compressed Parquet, partitioned for query efficiency.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          EVENT-DRIVEN ETL PIPELINE                              │
│                                                                                 │
│  ┌──────────────┐    EventBridge     ┌─────────────────────────────────────┐    │
│  │   S3 Bucket  │ ─────────────────► │  orchestrate_data_ingestion (SFN)  │    │
│  │              │  "Object Created"  │                                     │    │
│  │ raw_data/    │  on raw_data/      │  RunGlueJob ──► Success             │    │
│  │ sales_data/  │  sales_data/       │       │                             │    │
│  │   *.csv      │                    │    (on fail)                        │    │
│  └──────────────┘                    │       ▼                             │    │
│         ▲                            │  SendToDLQ ──► FailState            │    │
│         │                            └─────────────────────────────────────┘    │
│         │                                    │ (Glue Job runs)                  │
│         │                                    ▼                                  │
│         │                          ┌──────────────────┐                         │
│         │                          │   AWS Glue Job   │                         │
│         │                          │ ingest_sales_data│                         │
│         │                          │                  │                         │
│         │                          │ 1. CSV Validate  │                         │
│         │                          │ 2. SQL Transform │                         │
│         │                          │ 3. Drop Nulls    │                         │
│         │                          │ 4. DQ Check      │                         │
│         │                          │ 5. Write Parquet │                         │
│         │                          └──────────────────┘                         │
│         │                                    │                                  │
│         │                                    ▼                                  │
│         │                          ┌──────────────────┐                         │
│         │                          │  S3 Silver Layer │                         │
│         │                          │  data-sink-one/  │◄── Glue Catalog ──►     │
│         │                          │  silver_layer/   │    (silver_table)  Athena│
│         │                          │  sales_data/     │                         │
│         │                          └──────────────────┘                         │
│                                                                                 │
│  ┌────────────────────┐    replay_failed_ingestion (SFN)                        │
│  │  SQS DLQ           │ ◄─────────────────────────────────────────────────────  │
│  │sales-ingestion-dlq │ ─────► Read ──► Re-run Glue ──► Delete from DLQ        │
│  └────────────────────┘                                                         │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Component Breakdown

### Amazon S3 — Source & Sink

**Source bucket:** `aws-glue-s3-bucket-one`

- Raw CSV files are uploaded to the prefix `raw_data/sales_data/`.
- Amazon EventBridge notifications are enabled on this bucket (as shown in the S3 console screenshot — "Send notifications to Amazon EventBridge for all events in this bucket: On").
- This setting means every S3 operation (PUT, COPY, etc.) fires an event to the default EventBridge event bus automatically, with no additional SNS/SQS setup needed.

**Sink bucket:** `data-sink-one`

- Processed data is written to `silver_layer/sales_data/`, partitioned by `category`.
- Data is stored in **Snappy-compressed Parquet** — a columnar format optimized for Athena queries (faster reads, lower cost due to compression and column pruning).

---

### Amazon EventBridge — Event Router

**Rule name:** `ActivateLambdaFuncEventBridgeRules`

**Event Pattern:**
```json
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": { "name": ["aws-glue-s3-bucket-one"] },
    "object": { "key": [{ "prefix": "raw_data/sales_data/" }] }
  }
}
```

This rule acts as a **precise filter**:
- It only responds to S3 `Object Created` events (i.e., new file uploads — not deletes or reads).
- It is scoped specifically to the `aws-glue-s3-bucket-one` bucket.
- It further narrows to only files under the `raw_data/sales_data/` prefix, ignoring all other paths in the bucket.

**Target:** The `orchestrate_data_ingestion` Step Functions state machine.

**Input transformation:** Currently disabled, meaning the raw EventBridge event JSON (including `$.detail.bucket.name` and `$.detail.object.key`) is passed directly to Step Functions as the input payload. This is why the state machine can reference `$.detail.bucket.name` and `$.detail.object.key` directly.

---

### AWS Step Functions — Orchestrator

Two state machines coordinate the pipeline:

1. **`orchestrate_data_ingestion`** — triggered by EventBridge for each new file upload. Runs the Glue job and handles failures by routing to the DLQ.
2. **`replay_failed_ingestion`** — manually triggered to re-process messages that ended up in the DLQ.

Both are covered in detail in the [Step Functions State Machines](#step-functions-state-machines) section.

---

### Amazon SQS — Dead Letter Queue

**Queue name:** `sales-ingestion-dlq`

The DLQ is a standard SQS queue that acts as a **failure holding area**. When the `orchestrate_data_ingestion` state machine exhausts retries and the Glue job still fails, the entire event payload (including the original S3 bucket name and key) is sent to this queue as a JSON message.

This ensures:
- **No data loss** — failed ingestion events are not silently dropped.
- **Replayability** — the `replay_failed_ingestion` state machine can pick up these messages, re-run the Glue job, and delete the message from the queue on success.

---

### AWS Glue — ETL Engine

**Job name:** `ingest_sales_data`

The Glue job is a PySpark script (using the AWS Glue `DynamicFrame` abstraction) that performs the full ETL pipeline: validate → extract → transform → quality check → load.

The job accepts two runtime arguments:
- `--source_bucket`: S3 bucket name passed from the Step Functions state machine.
- `--source_key`: the specific S3 object key (file path) to process.

This makes the Glue job **reusable and file-specific** — it processes exactly the file that triggered the event, not an entire folder.

---

### AWS Glue Data Catalog & Athena

After the Glue job writes Parquet to S3, it registers (or updates) the table in the Glue Data Catalog:

- **Database:** `aws-glue-tutorial-aditya`
- **Table:** `silver_table_sales_data`
- **Partition key:** `category`

The Glue Data Catalog acts as a central **Hive-compatible metastore**. Once the table is registered, Amazon Athena can immediately query it using standard SQL without any additional configuration. Partitioning by `category` means Athena scans only the relevant partition files when filtering by category, reducing query cost and time.

---

## ETL Script Deep Dive

### Job Arguments & Initialization

```python
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'source_key'])
```

`getResolvedOptions` is an AWS Glue utility that reads job parameters passed at runtime. The Step Functions state machine injects `--source_bucket` and `--source_key` from the EventBridge payload, so each Glue job execution knows exactly which file to process.

The Spark and GlueContext are initialized in the standard way, with `job.init()` registering the job run with AWS Glue's bookmarking and monitoring infrastructure.

---

### Delimiter Corruption Check (Pre-Spark)

```python
raw_lines = spark.read.text(f"s3://{source_bucket}/{source_key}").collect()
```

This is a **pre-parsing validation** step that runs before Spark even attempts to interpret the CSV structure. It reads the file as raw text lines and checks that every row has the same number of columns as the header row, using Python's `csv.reader` to correctly handle quoted fields containing commas.

**Why this matters:** A common data quality issue in CSV pipelines is files where a different delimiter (e.g., semicolons instead of commas) was accidentally used. Spark in `PERMISSIVE` mode would silently parse such a file into a single-column DataFrame rather than raising an error. This check catches that structural corruption early and raises a descriptive exception with the line numbers and previews of bad rows.

```python
def count_csv_columns(line: str) -> int:
    try:
        return len(next(csv.reader(io.StringIO(line))))
    except StopIteration:
        return 0
```

This helper correctly handles quoted fields — for example, `"Smith, John",25,Engineer` has 3 columns even though there are 4 commas.

---

### Dynamic Frame Loading

```python
RawdatasourceS3_node = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "mode": "PERMISSIVE"},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [f"s3://{source_bucket}/{source_key}"], "recurse": False},
    ...
)
```

`PERMISSIVE` mode means Spark will attempt to parse every row and put unparseable values as `null` rather than failing the entire job. This is a deliberate choice — combined with the corruption checks above and the null detection below, it allows the pipeline to catch and report issues rather than crashing opaquely.

`recurse: False` ensures Spark reads only the specific file passed in, not every file in the directory — which is critical for event-driven processing.

---

### Spark-Level Corruption Check

After loading into a DataFrame, a second round of corruption detection runs:

```python
null_exprs = [F.col(c).isNull().cast("int") for c in df.columns]
bad_rows = df.filter(reduce(lambda a, b: a + b, null_exprs) > expected_cols * 0.7)
```

This computes the number of null columns per row and flags any row where more than 70% of columns are null. Such rows indicate structural parsing failures — e.g., a row that Spark couldn't parse properly in `PERMISSIVE` mode.

Three checks are performed in sequence:
1. **No columns inferred** → file couldn't be read at all.
2. **Empty DataFrame** → file has a header but no data rows.
3. **Null-heavy rows** → rows that Spark failed to parse correctly.

---

### SQL Transformation Query

The core transformation is a Spark SQL query that cleans and casts the raw string columns into proper typed values:

| Raw Column | Example Raw Value | Transformation | Output Type |
|---|---|---|---|
| `discounted_price` | `₹149` | Strip non-numeric chars | `DOUBLE` |
| `actual_price` | `₹1,000` | Strip non-numeric chars | `DOUBLE` |
| `discount_percentage` | `85%` | Strip non-numeric chars | `DOUBLE` |
| `rating` | `3.9` | Strip non-numeric chars | `DOUBLE` |
| `rating_count` | `24,871` | Strip non-digits | `INT` |

```sql
CAST(REGEXP_REPLACE(discounted_price, '[^0-9.]', '') AS DOUBLE) AS discounted_price
```

The regex `[^0-9.]` removes everything that is not a digit or a decimal point — cleaning currency symbols, thousand separators, and percent signs in a single pass. For `rating_count`, the pattern `[^0-9]` is stricter (no decimal point) since counts are always integers.

The remaining string columns (`product_id`, `product_name`, `category`, `about_product`, `user_id`, `user_name`, `review_id`, `review_title`, `review_content`, `img_link`, `product_link`) are passed through unchanged.

---

### Drop Null Fields

```python
DropNullFields_node = drop_nulls(
    glueContext, frame=SQLQuery_node,
    nullStringSet={"", "null"},
    nullIntegerSet={-1},
    transformation_ctx=...
)
```

The `drop_nulls` function (using `_find_null_fields` internally) scans the schema recursively and identifies **entire columns** where all distinct values fall within the null set. It then drops those columns entirely using `DropFields.apply`.

- String columns containing only empty strings or the literal `"null"` string are dropped.
- Integer/Long/Double columns containing only `-1` are dropped.

This is a **schema-level cleanup** step — it removes columns that carry no real data, keeping the silver layer lean.

---

### Data Quality Rules

```
Rules = [
    ColumnCount = 16,
    IsComplete "rating",
    IsComplete "rating_count",
    IsComplete "discounted_price",
    IsComplete "actual_price",
    ColumnValues "rating" >= 0,
    ColumnValues "rating_count" >= 0,
    ColumnValues "discounted_price" >= 0,
    ColumnValues "actual_price" >= 0,
    ColumnValues "discount_percentage" >= 0,
    IsUnique "product_id"
]
```

AWS Glue Data Quality (`EvaluateDataQuality`) evaluates these rules against the transformed DataFrame. Key rules explained:

- **`ColumnCount = 16`**: Ensures the expected schema shape is preserved after transformation. A mismatch would indicate a schema drift in the source data.
- **`IsComplete`**: Ensures critical business columns (`rating`, `rating_count`, `discounted_price`, `actual_price`) have no nulls — these fields are required for downstream analytics.
- **`ColumnValues >= 0`**: Business logic guard — prices, ratings, and percentages cannot be negative. Any negative value indicates a parsing error or data corruption.
- **`IsUnique "product_id"`**: Ensures no duplicate products are loaded into the silver layer, maintaining data integrity for downstream joins and aggregations.

If DQ evaluation fails, the job raises an exception, causing Step Functions to route the event to the DLQ.

---

### Silver Layer Sink & Catalog Registration

```python
SilverlayerdatasinkS3 = glueContext.getSink(
    path="s3://data-sink-one/silver_layer/sales_data/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=["category"],
    enableUpdateCatalog=True,
    transformation_ctx=...
)
SilverlayerdatasinkS3.setCatalogInfo(
    catalogDatabase="aws-glue-tutorial-aditya",
    catalogTableName="silver_table_sales_data"
)
SilverlayerdatasinkS3.setFormat("glueparquet", compression="snappy")
SilverlayerdatasinkS3.writeFrame(DropNullFields_node)
```

Key decisions here:

- **`partitionKeys=["category"]`**: Data is physically organized into S3 folder paths like `silver_layer/sales_data/category=Electronics/`. Athena queries with a `WHERE category = 'Electronics'` filter skip all other partitions entirely.
- **`updateBehavior="LOG"`**: If a partition already exists, Glue logs the conflict rather than overwriting or failing, giving visibility into re-processing scenarios.
- **`enableUpdateCatalog=True`**: After writing, Glue automatically updates the Glue Data Catalog table schema and partition metadata — no manual `MSCK REPAIR TABLE` needed in Athena.
- **`compression="snappy"`**: Snappy offers a balance between compression ratio and read/write speed, well-suited for Athena's scan-heavy query patterns.

---

## Step Functions State Machines

### orchestrate_data_ingestion

This state machine is the **primary orchestrator**, invoked directly by the EventBridge rule on every new S3 file upload.

```
RunGlueJob
    │ (success)
    ▼
  Success
    │ (failure after retries)
    ▼
SendToDLQ ──► FailState
```

**RunGlueJob:**
- Uses the `arn:aws:states:::glue:startJobRun.sync` integration — Step Functions waits synchronously until the Glue job completes (or fails) before moving to the next state.
- Passes `--source_bucket` and `--source_key` extracted directly from the EventBridge event payload (`$.detail.bucket.name`, `$.detail.object.key`).
- **Retry policy:** On any error, retries up to 2 times with a 30-second initial interval and a 2x exponential backoff (30s → 60s → fail).

**SendToDLQ:**
- After all retries are exhausted, the entire Step Functions execution input (`$`) — which contains the original EventBridge event with bucket and key — is sent as the SQS message body.
- This preserves all information needed to replay the failed job later.

---

### replay_failed_ingestion

This state machine is designed to be **manually triggered** (e.g., after fixing a data quality issue) to re-process failed events from the DLQ.

```
ReceiveFromDLQ
    │
CheckIfEmpty ──► (no messages) ──► Success
    │ (messages present)
    ▼
ReplayBatch (Map: parallel over messages)
    │
    └── ExtractPayload
             │
        CheckCauseType
             │ (cause is JSON string starting with "{")
             ▼
        ParseJsonCause ──► RunGlueJob ──► DeleteMessage ──► SuccessState
             │ (not JSON)
             ▼
        DeleteMessage (skip malformed/unrecoverable messages)
```

**Key design decisions:**

- **`ReceiveMessage` with `MaxNumberOfMessages: 10`**: SQS returns up to 10 messages per call, processed in parallel by the Map state (`MaxConcurrency: 2`).
- **`CheckCauseType` with `StringMatches: "{*"`**: The DLQ message body contains a Step Functions `Cause` field, which itself may be a JSON string (the Glue job failure details). This check determines whether the cause is a parseable JSON object or a plain error string.
- **`ParseJsonCause` using `States.StringToJson`**: Deserializes the nested JSON cause string to extract the original Glue job `Arguments` (bucket and key).
- **`DeleteMessage` after success**: Messages are only deleted from the DLQ after a successful Glue re-run, ensuring at-least-once delivery semantics — if the replay job crashes mid-run, the message stays in the DLQ for the next replay attempt.
- **`FailState` on Glue failure**: If the replayed Glue job fails again, the Map iterator fails without deleting the SQS message, preserving it for future investigation or another replay.

---

## End-to-End Data Flow

```
1. User/process uploads sales_data_2024.csv to:
   s3://aws-glue-s3-bucket-one/raw_data/sales_data/sales_data_2024.csv

2. S3 emits "Object Created" event to EventBridge (enabled via bucket properties)

3. EventBridge rule matches the event (correct bucket + prefix)
   → Triggers orchestrate_data_ingestion Step Functions execution
   → Input: { "detail": { "bucket": { "name": "aws-glue-s3-bucket-one" }, 
                          "object": { "key": "raw_data/sales_data/sales_data_2024.csv" } } }

4. Step Functions starts Glue job ingest_sales_data with:
   --source_bucket = "aws-glue-s3-bucket-one"
   --source_key    = "raw_data/sales_data/sales_data_2024.csv"

5. Glue job executes:
   a) Pre-Spark delimiter check (raw text scan)
   b) Load CSV into DynamicFrame
   c) Post-Spark corruption check (null-heavy row detection)
   d) SQL transformation (clean prices, cast types)
   e) Drop fully-null columns
   f) Data quality evaluation (16 columns, completeness, ranges, uniqueness)
   g) Write Snappy Parquet to s3://data-sink-one/silver_layer/sales_data/category=<X>/
   h) Register/update table silver_table_sales_data in Glue Catalog

6. Step Functions moves to Success state

7. Athena can now query:
   SELECT * FROM "aws-glue-tutorial-aditya"."silver_table_sales_data"
   WHERE category = 'Electronics' AND rating >= 4.0
```

---

## Error Handling & Resilience Strategy

| Failure Scenario | Detection Point | Response |
|---|---|---|
| Wrong delimiter (semicolons vs commas) | Pre-Spark text scan | Glue job fails with descriptive error |
| Empty file / header-only file | Pre-Spark text scan | Glue job fails immediately |
| Spark parsing failure (null-heavy rows) | Post-load null check | Glue job fails with row count |
| Type casting failure (bad data) | SQL query try/catch | Glue job raises `RuntimeError` |
| Missing required columns (nulls in rating, price) | DQ `IsComplete` rules | DQ evaluation raises exception |
| Negative prices or ratings | DQ `ColumnValues >= 0` rules | DQ evaluation raises exception |
| Duplicate product_id | DQ `IsUnique` rule | DQ evaluation raises exception |
| Transient Glue failures (timeouts, infra) | Step Functions Retry | Retried up to 2x with backoff |
| Persistent failures after retries | Step Functions Catch | Event payload sent to DLQ |
| DLQ message replay failure | `replay_failed_ingestion` Catch | Message retained in DLQ |

---

## Data Lakehouse Layers

| Layer | Location | Format | Description |
|---|---|---|---|
| **Raw** | `s3://aws-glue-s3-bucket-one/raw_data/sales_data/` | CSV | Original files, unchanged. Source of truth. |
| **Silver** | `s3://data-sink-one/silver_layer/sales_data/` | Parquet (Snappy) | Cleaned, typed, partitioned, quality-validated. Queryable via Athena. |

The pipeline currently implements two of the three medallion layers. A potential future **Gold layer** could aggregate the silver data into business-level metrics (e.g., average discount by category, top-rated products) using an additional Glue job or an Athena CTAS query.
