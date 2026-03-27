### GAPs that I need to fix that were present in Implementation of version 5 
#### High priority these affect correctness and reliability

**1. Lambda idempotency permanently blocks `FAILED` files on re-upload** (RESOLVED)

The `put_item` with `attribute_not_exists(file_key)` blocks any file whose `file_key` already exists in DynamoDB regardless of whether status is `SUCCESS`, `FAILED`, or `IN_PROGRESS`. This means if a corrupted file fails and a corrected version is uploaded with the same filename, Lambda will silently skip it forever. The fix is to check status first — only permanently block `SUCCESS`, allow `FAILED` records to be overwritten:

```python
existing = dynamodb.get_item(
    TableName=TABLE_NAME,
    Key={"file_key": {"S": file_key}}
)
item = existing.get("Item")

if item and item.get("status", {}).get("S") == "SUCCESS":
    print(f"Already succeeded, skipping: {file_key}")
    continue

# New file or previously failed allow through
try:
    dynamodb.put_item(
        TableName=TABLE_NAME,
        Item={
            "file_key": {"S": file_key},
            "status": {"S": "IN_PROGRESS"},
            "retry_count": {"N": "0"},
            "created_at": {"S": timestamp}
        },
        ConditionExpression="attribute_not_exists(file_key) OR #s = :failed",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":failed": {"S": "FAILED"}}
    )
    files.append({"bucket": bucket, "key": key, "file_key": file_key})
except dynamodb.exceptions.ConditionalCheckFailedException:
    print(f"Currently IN_PROGRESS, skipping: {file_key}")
```

**2. `DQFailureSilent` data quality runs but the job can still write bad data** (RESOLVED)

Your `EvaluateDataQuality` is wrapped in a `try/except` that raises `RuntimeError` on failure which is correct. However `EvaluateDataQuality().process_rows()` with `"strategy": "BEST_EFFORT"` logs DQ results but does not fail the Glue job on rule violations by itself it depends entirely on your `try/except` catching the exception. The problem is that `process_rows` with `BEST_EFFORT` does not always throw even when rules fail it publishes metrics and continues. This means data that fails the `IsUnique "product_id"` or `ColumnCount = 16` check can silently pass through to the Silver layer. Change the strategy:

```python
EvaluateDataQuality().process_rows(
    frame=deduped_dynamic_frame,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "...",
        "enableDataQualityResultsPublishing": True,
        "failureAction": "FAIL"      # ← add this
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "FAIL_JOB",   # ← change this
        "observations.scope": "ALL"
    }
)
```

**3. `dropDuplicates()` with no arguments is too aggressive** (RESOLVED)

Your deduplication uses `df_clean.dropDuplicates()` which compares every single column. This means two rows that have identical product data but different `review_id` values will both be kept which is correct. But two rows that are truly identical across all 16 columns will lose one copy which is also correct. The real problem is that `dropDuplicates()` on a large DataFrame triggers a full shuffle across the entire Spark cluster, which is very expensive. For this dataset the correct key for deduplication is `review_id` since that is the natural unique identifier per review:

```python
deduped_df = df_clean.dropDuplicates(["review_id"])
```

This is both semantically correct you are deduplicating on the business key and significantly faster because Spark can partition by `review_id` rather than hashing all 16 columns.

**4. `sales-ingestion-dlq` visibility timeout is 12 hours**

After `replay_failed_ingestion` reads a message via `ReceiveFromDLQ`, that message becomes invisible for 12 hours even if `UpdateFailure` ends with `"End": true` and does not delete it. From a data engineering perspective this means the message is effectively locked for half a day between replay attempts. Reduce the visibility timeout to 5 minutes so the message becomes available again quickly after each replay attempt.

---

#### Medium priority these affect observability and operational quality

**5. No `row_count` written to DynamoDB after successful ingestion**

Your DynamoDB schema in the proposed architecture diagram shows `row_count` as an attribute, but the `UpdateSuccess` state only writes `status` and `updated_at`. You never capture how many rows were actually ingested. This is a significant gap for a data platform without row counts you cannot detect silent data loss (a file that processed successfully but produced 0 rows, or a file that should have 500 rows but only produced 50). Add a row count capture in the Glue script:

```python
row_count = deduped_df.count()
print(f"Glue Visual ETL | Final row count: {row_count}")
```

Then pass it as a Glue job metric or as a separate DynamoDB update. One way is to write it as a Glue job property that the Step Function can read from the `glueResult` after success, then include it in `UpdateSuccess`.

**6. No CloudWatch alerting on DLQ depth**

There is no monitoring documented. In production the most critical metric to alert on is `ApproximateNumberOfMessagesVisible` on `sales-ingestion-dlq` if this grows it means files are failing faster than they are being replayed. A simple CloudWatch alarm that fires when DLQ message count exceeds 0 would give you immediate visibility. Similarly, a CloudWatch alarm on Glue job failures and Step Function execution failures would complete the observability picture.

**7. Debug print statements left in the Glue script** (RESOLVED)

The Glue script has several production debug lines that should either be removed or controlled by a log level flag:

```python
print("Glue Visual ETL | DEBUG bucket:", source_bucket)
print("Glue Visual ETL | DEBUG columns:", df.columns)
print("Glue Visual ETL | DEBUG schema:", df.schema)
print("Glue Visual ETL | DEBUG sample rows:", df.limit(2).toPandas())
```

The `df.limit(2).toPandas()` line is particularly costly it materialises a Pandas DataFrame on the driver node and prints raw data values to CloudWatch logs. This is a privacy concern if the data contains PII and a cost concern since it triggers a Spark action. Remove or gate these behind a debug flag.

**8. `raw_lines = spark.read.text(...).collect()` loads entire file to driver** (RESOLVED)

The delimiter corruption check reads the entire file as text and calls `.collect()` which brings every single line into the Lambda driver's memory. For small files like your sales CSVs this is fine, but if a file is 500MB this will OOM the driver. A safer approach is to sample or limit:

```python
raw_lines = spark.read.text(f"s3://{source_bucket}/{source_key}").limit(1000).collect()
```

For corruption detection you typically only need to scan a representative sample rather than every row.

#### Lower priority these are good engineering hygiene

**9. `AmazonSQSFullAccess` on the replay Step Function role is too broad**

The IAM role for `replay_failed_ingestion` uses `AmazonSQSFullAccess`. This grants full permissions to all SQS queues in the account. The minimum required is `sqs:ReceiveMessage`, `sqs:DeleteMessage`, and `sqs:GetQueueAttributes` scoped to only `sales-ingestion-dlq`. You noted this is for learning purposes which is fine, but worth flagging for when this moves to a real environment.

**10. No TTL on DynamoDB records** (RESOLVED)

The `file_processing_registry` table will grow forever every file ever processed keeps a permanent record. For a data engineering use case you typically only need records for the last 30–90 days for audit purposes. Add a `ttl` attribute when writing records in Lambda and enable DynamoDB TTL on that attribute. This keeps the table lean and reduces read costs over time:

```python
import time
ttl_value = int(time.time()) + (90 * 24 * 60 * 60)  # 90 days from now

dynamodb.put_item(
    TableName=TABLE_NAME,
    Item={
        "file_key": {"S": file_key},
        "status": {"S": "IN_PROGRESS"},
        "retry_count": {"N": "0"},
        "created_at": {"S": timestamp},
        "ttl": {"N": str(ttl_value)}   # ← add this
    },
    ...
)
```