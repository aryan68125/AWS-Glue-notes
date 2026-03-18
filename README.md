# AWS Glue NOTES
This is a notes on AWS Glue for data engineers.

# Prerequisites
## Dataset used:
https://github.com/darshilparmar/uber-etl-pipeline-data-engineering-project/blob/main/data/uber_data.csv

## AWS CLI :
AWS CLI setup documentation : <br>
AWS CLI docs : https://docs.aws.amazon.com/cli/latest/userguide/cli-usage-commandstructure.html

## Topics to cover : 
- AWS S3
- Managed Tables
- Lamda Triggers
- Glue Catalog
- Glue Crawlers
- Cron Triggers
- IAM 
- Schema Evolution
- Spark Scripts
- CTAS
- Incremental Load
- Glue Data Brew
- External Tables
- Event Based Trigger 
- Custom Functions
- Work flow orchestration
- Custom collection methods in AWS Glue

## AWS Glue
### **What is ETL/ELT?** <br>
We want to **E**xtract the data and **L**oad that data somewhere and that loaded data should be **T**ransformed as per requirments.
- Data engineer will pull data from the sources 
- Data engineer make sure that the data is in the form which can be used by the downstream services or application
- This is the part where ETL/ELT comes in and its designed and maintained by data engineer.


| Aspect                       | ETL                                         | ELT                            |
| ---------------------------- | ------------------------------------------- | ------------------------------ |
| Order                        | **Extract → Transform → Load**              | **Extract → Load → Transform** |
| Where transformation happens | Before loading (external processing engine) | Inside the data warehouse      |
| Best for                     | Traditional data warehouses                 | Modern cloud data platforms    |
| Scalability                  | Limited by ETL server                       | Scales with warehouse compute  |

**ETL :** <br>
**Pros :** <br>
- Clean data before storage
- Strong governance
- Lower storage cost (no raw dump)

**cons :**<br>
- Less flexible
- Slower to adapt to new schema
- Doesn't scale well for massive data

**ELT :** <br>
**Pros :**
- Highly scalable
- Store raw data (future proof)
- Faster iteration
- Works well with analytics + ML

**Cons :**<br>
- Raw data storage costs
- Requires powerful warehouse

#### Points to remember : 
- ETL scaling is:
    - Vertical (bigger machine)
    - Operationally heavy
- ELT scaling is:
    - Horizontal (distributed warehouse engine)
    - Cloud-managed

#### Deep architectural comparison

| Property             | ETL                  | ELT                              |
| -------------------- | -------------------- | -------------------------------- |
| Raw data retained    | Usually no           | Yes                              |
| Schema flexibility   | Low                  | High                             |
| Reprocessing ability | Hard                 | Easy                             |
| Scaling compute      | Hard                 | Elastic                          |
| Best for             | Structured reporting | Analytics + AI + experimentation |
| Change tolerance     | Low                  | High                             |

### **Why AWS Glue?** <br>
AWS glue is a combination of many components that are stiched together. It is a serverless data integration service.

It helps you:
- Discover data
- Catalog metadata
- Transform data
- Prepare data for analytics

AWS glue docs : 
https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html

AWS glue pricing related details are here : 
https://aws.amazon.com/glue/pricing/

### **What is Serverless?**
Serverless means that you don't need to manage the underlying infrastructure, scaling etc. you just have to run the service.

## AWS Glue components 
- Direct tools: 
    - Data Catalog
    - Crawler
    - Glue Data Brew
- Indirect tools:
    - S3
    - Athena
    - IAM
    - Lambda

**NOTE :** IAM is the most important component in AWS you cannot build anything without IAM in AWS because security is the most important thing. <br>
IAM is a security through which multiple components talk to each other.

**What is IAM ?** <br>
AWS Identity and Access Management (IAM) is a web service for securely controlling access to AWS services. With IAM, you can centrally manage users, security credentials such as access keys, and permissions that control which AWS resources users and applications can access.
Docs link related to IAM in AWS docs : <br>
https://docs.aws.amazon.com/iam/

**What is AWS S3?** <br>
Amazon Simple Storage Service (Amazon S3) is an object storage service that offers industry-leading scalability, data availability, security, and performance. Customers of all sizes and industries can use Amazon S3 to store and protect any amount of data for a range of use cases, such as data lakes, websites, mobile applications, backup and restore, archive, enterprise applications, IoT devices, and big data analytics <br>
- AWS S3 bucket means our data lake
- Bucket means object storage i.e I can upload anything
https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html

**Points to remember :**
- When creating AWS S3 bucket make sure that you enable bucket versioning because when this is enabled then if you are getting a file with the same name and type then the s3 will not replace the file it will simply give the file a new name which is a good thing in this case.
- Always create folders when working with S3
- We should not work on file level in data egineering we should always work on folder level.

## AWS Glue Catalog
Suppose you a data stored in csv files and that csv file is stored in the S3 bucket and you want to query this data using SQL. <br>
Normally this is not possible because csv is just a text file how can someone query data using SQL stored in csv files. Csv files are not a structured data <br>
Whenever you use SQL to query data that data needs to be a structured data i.e rows and columns <br>
You may say that if you open a csv file in excel you can view it in rows and columns, but excel is a software that is built to render the data inside the csv file in rows and columns but that doesn't mean the data iself is stored in rows and columns to be able to call it a structured data. <br>
So here we will apply an abstraction layer and this abstraction layer will be applied by **Data Catalog**. <br>
The moment you apply the abstraction applied by the **Data Catalog** the csv files stored in S3 starts behaving like objects of the database. FYI catalogs are equivalent to database. **So whenever you go inside the AWS Glue service and under the section Data Catalog you create a new database that means you are creating a new catalog.**<br>
This catalog will register the metadata and schema of the files and will create an object in the catalog. <br>
**ADVANTAGE :**
- This AWS Glue catalog gives the devs the power to use their SQL skills to query the data stored in csv, parquete etc. files (a non structured data) in the same way they would have queried the objects of databases.
- Catalog is the backbone of all the queries that you do in Athena. 
- If catalog is not there you will not be able to query the data.

There are three things in Data Catalog: 
- External table
- CTAS managed
- CTAS external

Here are some references that you can use from the official docs <br> 
https://docs.aws.amazon.com/glue/latest/dg/components-overview.html#data-catalog-intro
Getting started with the AWS Glue Data Catalog <br>
https://docs.aws.amazon.com/glue/latest/dg/start-data-catalog.html

### How to create databases in AWS Glue?
You can create a database in AWS Glue by going to the menue option named database under the Data Catalog section on the side panel provided by AWS. <br>
On top right corner you will find the Add database button press it and create your database <br>

### How to create tables in AWS Glue catalog?
There are two ways to create a table in catalog:
- Manual
- Through crawlers

#### External table
When you have the data in the datalake (S3 bucket) and you want to register the data in the data catalog. <br>
Table metadata lives in the catalog but the real data is stored in the S3 bucket which is managed by you. <br>
amazon athena docs link : <br>
https://docs.aws.amazon.com/athena/

## AWS Athena
#### How to create External table using Athena.
External table is the same as externally managed tables that I read in databricks.

**STEP 1:** <br>
Go to athena query console > settings > set query result path <br>
NOTE : The source and destination should not be the same i.e you cannot give the same s3 bucket url where you have the csv files to save your athena query results, it must be different or else you will keep getting errors saying destination to store query results is not provided when you try to create the external table.

**STEP 2:** <br>
From the left side panel > select create > and from the create table from data source section > select S3 bucket data

**STEP 3:** <br>
You will be greeted with a page where you will have to set 
- The table name, table description
- Dataset : set the location to the csv file in the s3 bucket that you are trying to use to create an external table from.
- Add bulk column button to set the column names and their data types. Since I am creating an external managed table from a csv file I can set all the columns to be of string type because csv files are not a structured data and it stores data in form of strings itself saperated by comma.

**STEP 4:** <br>
Once you did all the above steps correctly you will see a query to create external table generated for you. Your query should look like this : 

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `aws-glue-tutorial-aditya`.`uber_data_external_table` (
  `vendorid` string,
  `tpep_pickup_datetime` string,
  `tpep_dropoff_datetime` string,
  `passenger_count` string,
  `trip_distance` string,
  `pickup_longitude` string,
  `pickup_latitude` string,
  `ratecodeid` string,
  `store_and_fwd_flag` string,
  `dropoff_longitude` string,
  `dropoff_latitude` string,
  `payment_type` string,
  `fare_amount` string,
  `extra` string,
  `mta_tax` string,
  `tip_amount` string,
  `tolls_amount` string,
  `improvement_surcharge` string,
  `total_amount` string
) COMMENT "This table holds the data related to customer behaviour and ride metrics for all the taxis that work under uber fleet"
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://aws-glue-s3-bucket-one/raw_data/'
TBLPROPERTIES ('classification' = 'parquet');
```

#### Issues I ran into : After creating an external table
After I was able to successfully create an external table. I tried querying it using this sql query. 

```sql
SELECT * FROM "aws-glue-tutorial-aditya"."uber_data_external_table" limit 10;
```

The moment I ran this query I was slapped by this error right on my face.

```bash
HIVE_BAD_DATA: Malformed Parquet file. Expected magic number: PAR1 got: 6.8 [s3://aws-glue-s3-bucket-one/raw_data/uber_data.csv]

This query ran against the "aws-glue-tutorial-aditya" database, unless qualified by the query. Please post the error message on our forum  or contact customer support  with Query Id: 0a40a5c0-1kk6-4fuc-9kyo-21uc7c7e5521
```

**Reason why this happened?** <br>
After some research I found out that in the UI I may have accidently selected the paraquet format instead of csv. <br>
Look at the error where it says ```Expected magic number: PAR1 got: 6.8``` this means that the metadata created by athena says that it is a paraquet file but my actual data file in the s3 is a csv file and hence I am getting a file format mismatch error.

**Solution :**
Since I am trying to create an external table where the actual data is stored in csv file hence I will have to choose the file format to be of type CSV under the Data format section. <br>
Once you do that and re-create the external table you will get the generated query that looks something like this. 

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `aws-glue-tutorial-aditya`.`uber_data_external_table` (
  `vendorid` string,
  `tpep_pickup_datetime` string,
  `tpep_dropoff_datetime` string,
  `passenger_count` string,
  `trip_distance` string,
  `pickup_longitude` string,
  `pickup_latitude` string,
  `ratecodeid` string,
  `store_and_fwd_flag` string,
  `dropoff_longitude` string,
  `dropoff_latitude` string,
  `payment_type` string,
  `fare_amount` string,
  `extra` string,
  `mta_tax` string,
  `tip_amount` string,
  `tolls_amount` string,
  `improvement_surcharge` string,
  `total_amount` string
) COMMENT "This is a test table to see if I am able to create an externally managed table using a csv file"
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://aws-glue-s3-bucket-one/raw_data/'
TBLPROPERTIES ('classification' = 'csv');
```

If you look at the generated SQL code closely, you will see that instead of:

`OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'`

you now have:

`OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'`

Now when I query the table to get the data from my csv file using athena it works 

```sql
SELECT * FROM "aws-glue-tutorial-aditya"."uber_data_external_table" limit 10;
```

OR 

```sql
SELECT * FROM uber_data_external_table LIMIT 10;
```

**Output:**  

| #  | vendorid | tpep_pickup_datetime | tpep_dropoff_datetime | passenger_count | trip_distance | pickup_longitude | pickup_latitude | ratecodeid | store_and_fwd_flag | dropoff_longitude | dropoff_latitude | payment_type | fare_amount | extra | mta_tax | tip_amount | tolls_amount | improvement_surcharge | total_amount |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2 | 1 | 2016-03-01 00:00:00 | 2016-03-01 00:07:55 | 1 | 2.5 | -73.97674560546875 | 40.765151977539055 | 1 | N | -74.00426483154298 | 40.74612808227539 | 1 | 9.0 | 0.5 | 0.5 | 2.05 | 0.0 | 0.3 | 12.35 |
| 3 | 1 | 2016-03-01 00:00:00 | 2016-03-01 00:11:06 | 1 | 2.9 | -73.98348236083984 | 40.767925262451165 | 1 | N | -74.00594329833984 | 40.7331657409668 | 1 | 11.0 | 0.5 | 0.5 | 3.05 | 0.0 | 0.3 | 15.35 |

#### Conclusion
- You can query your data stored in a string format separated by comma in a csv file using SQL just like you would query any other database object and this alone gives you a lot of power.
- You can gain this un godly power just by registering external table on top of those csv files. You suddenly find your self capable performing joins transform data using SQL the way you want based on your business requirements. 
- One another super power that you get if decide to use AWS athena.
    - You can query your logs provided the developer had the foresight to generate logs that is seperated by either comma like csv for pipe like psv or tabs like tsv then you can use athena to query your application logs using sql all you have to do is register an external table for that log file in athena. 
    - Why you would want to query your logs using Athena? 
        - You can make debugging in PROD a hell of a lot easier if you decide to design your logs Athena friendly in moments where your project is working fine in local, dev and QA environment but some error is happening in the PROD and you need to know why is this happening and from where? 
        - So make sure you always generate athena friendly logs in your projects if you are going to write it from scratch.

#### CTAS
There are two types of CTAS
- CTAS external
- CTAS managed

CTAS means **C**reate **T**able **A**s **S**elect. If I want to create a new table instead of using the csv file (source file) I want to use the already existing external or managed table then in that case CTAS will help me achieve my goal.

#### CTAS External
Below are the steps to create a external CTAS table where the altered csv files will be stored in the s3 bucket and its metadata will be registered in Athena. 
#### Issues I faced when creating a CTAS External in athena
I first ran this SQL query where I filtered the records based on total_amount where the total_amount must have value in it more than 100 

```sql
SELECT *
FROM uber_data_external_table
WHERE TRY_CAST(total_amount AS DOUBLE) > 100
LIMIT 10;
```
Once the sql query ran successfully and I was satisfied with the result I got I ended up creating a CTAS of this query.

```sql
CREATE TABLE uber_data_total_amount_filtered
WITH (
    format = "PARQUET",
    external_location = "s3://athena-query-result-bucket-268859/uber_table_query_result/filtered_data"
    write_compression = 'SNAPPY'
) AS
SELECT *
FROM uber_data_external_table
WHERE TRY_CAST(total_amount AS DOUBLE) > 100;
```

Then when I ran this sql query athena again slapped me with this error 

```bash
line 5:5: mismatched input 'write_compression'. Expecting: '%', ')', '*', '+', ',', '-', '.', '/', 'AND', 'AS', 'AT', 'OR', '[', '||', <EOF>, <predicate>
```

**Why this error happened?** <br>
If you look closely at this portion of the above code you will see that I have made several mistakes in the above query.

**Mistake 1:**<br>

```sql
WITH (
    format = "PARQUET",
    external_location = "s3://athena-query-result-bucket-268859/uber_table_query_result/filtered_data"
    write_compression = 'SNAPPY'
)
```

As you can see I forgot to put comma after defining external_location and white_compression.

**Mistake 2:** <br>
Another mistake that I did I wrapped the PARQUET and the external_location in double quotes which is wrong. I din't realise that athena query editor would treat it as a literal string instead of the provided value.

Here is the corrected code block 

```sql
WITH (
    format = 'PARQUET',
    external_location = 's3://athena-query-result-bucket-268859/uber_table_query_result/filtered_data',
    write_compression = 'SNAPPY'
)
```

Here is the complete sql query to create this CTAS 

```sql
CREATE TABLE uber_data_total_amount_filtered
WITH (
    format = 'PARQUET',
    external_location = 's3://athena-query-result-bucket-268859/uber_table_query_result/filtered_data',
    write_compression = 'SNAPPY'
) AS
SELECT *
FROM uber_data_external_table
WHERE TRY_CAST(total_amount AS DOUBLE) > 100;
```

Now when I use this sql query 

```sql
SELECT * fROM uber_data_total_amount_filtered;
```

I get this output where total_amount is greater than 100

| # | vendorid | tpep_pickup_datetime | tpep_dropoff_datetime | passenger_count | trip_distance | pickup_longitude | pickup_latitude | ratecodeid | store_and_fwd_flag | dropoff_longitude | dropoff_latitude | payment_type | fare_amount | extra | mta_tax | tip_amount | tolls_amount | improvement_surcharge | total_amount |
|---|----------|----------------------|------------------------|-----------------|--------------|------------------|------------------|------------|--------------------|-------------------|------------------|--------------|------------|-------|---------|------------|-------------|----------------------|--------------|
| 1 | 2 | 2016-03-01 00:00:00 | 2016-03-01 00:00:00 | 5 | 30.43 | -73.97174072265625 | 40.79218292236328 | 3 | N | -74.17716979980467 | 40.69505310058594 | 1 | 98.0 | 0.0 | 0.0 | 0.0 | 15.5 | 0.3 | 113.8 |
| 2 | 2 | 2016-03-10 07:09:59 | 2016-03-10 07:36:24 | 1 | 11.43 | -73.87612915039062 | 40.77179336547852 | 5 | N | -73.9721450805664 | 40.847412109375 | 1 | 90.0 | 0.0 | 0.5 | 8.0 | 18.04 | 0.3 | 116.84 |
| 3 | 2 | 2016-03-10 07:20:15 | 2016-03-10 07:50:14 | 6 | 17.34 | -74.00823974609375 | 40.70524978637695 | 3 | N | -74.17768096923827 | 40.6953010559082 | 1 | 65.5 | 0.0 | 0.0 | 24.99 | 17.5 | 0.3 | 108.29 |
| 4 | 2 | 2016-03-10 07:20:50 | 2016-03-10 07:52:07 | 5 | 18.41 | -74.00350189208984 | 40.742141723632805 | 3 | N | -74.17713928222656 | 40.695011138916016 | 1 | 66.5 | 0.0 | 0.0 | 16.86 | 17.5 | 0.3 | 101.16 |
| 5 | 2 | 2016-03-10 07:25:36 | 2016-03-10 08:13:46 | 2 | 25.82 | -73.99234008789062 | 40.731342315673835 | 3 | N | -74.36077880859375 | 40.742630004882805 | 1 | 87.5 | 0.0 | 0.0 | 25.08 | 12.5 | 0.3 | 125.38 |
| 6 | 2 | 2016-03-10 07:26:30 | 2016-03-10 07:45:42 | 2 | 10.78 | -73.84951782226561 | 40.748043060302734 | 5 | N | -73.70035552978516 | 40.758079528808594 | 1 | 84.95 | 0.0 | 0.0 | 17.05 | 0.0 | 0.3 | 102.3 |
| 7 | 2 | 2016-03-10 07:26:57 | 2016-03-10 08:15:27 | 1 | 21.83 | -73.92046356201173 | 40.74689102172852 | 3 | N | -74.1777801513672 | 40.69536972045898 | 1 | 79.0 | 0.0 | 0.0 | 28.89 | 17.0 | 0.3 | 125.19 |
| 8 | 2 | 2016-03-10 07:34:42 | 2016-03-10 08:04:53 | 1 | 19.88 | -73.78182983398438 | 40.64485549926758 | 4 | N | -73.58753204345702 | 40.72392272949219 | 1 | 83.0 | 0.0 | 0.5 | 16.76 | 0.0 | 0.3 | 100.56 |
| 9 | 2 | 2016-03-10 07:36:42 | 2016-03-10 08:29:31 | 1 | 29.3 | -73.86388397216798 | 40.76948165893555 | 4 | N | -73.42192840576173 | 40.77008819580078 | 1 | 125.5 | 0.0 | 0.5 | 25.26 | 0.0 | 0.3 | 151.56 |
| 10 | 2 | 2016-03-10 07:40:55 | 2016-03-10 08:39:06 | 1 | 44.49 | -74.01718139648438 | 40.70839691162109 | 5 | N | -74.64081573486328 | 40.574981689453125 | 1 | 225.0 | 0.0 | 0.0 | 47.56 | 12.5 | 0.3 | 285.36 |
| 11 | 2 | 2016-03-10 07:49:11 | 2016-03-10 07:49:24 | 1 | 0.0 | -73.94847869873048 | 40.7972526550293 | 5 | N | -73.94839477539062 | 40.797229766845696 | 2 | 200.0 | 0.0 | 0.0 | 0.0 | 0.0 | 0.3 | 200.3 |
| 12 | 2 | 2016-03-10 07:51:38 | 2016-03-10 08:33:43 | 1 | 18.69 | -73.98118591308595 | 40.741558074951165 | 3 | N | -74.17737579345702 | 40.69525909423828 | 1 | 70.5 | 0.0 | 0.0 | 15.0 | 15.0 | 0.3 | 100.8 |
| 13 | 2 | 2016-03-10 07:53:24 | 2016-03-10 08:59:22 | 1 | 43.65 | -74.00305938720702 | 40.72332382202149 | 5 | N | -74.45540618896483 | 40.84825134277344 | 1 | 143.33 | 0.0 | 0.5 | 39.16 | 12.5 | 0.3 | 195.79 |
| 14 | 2 | 2016-03-10 07:57:54 | 2016-03-10 09:24:29 | 4 | 45.68 | -73.87310028076173 | 40.7741584777832 | 4 | N | -73.19992828369139 | 40.708599090576165 | 2 | 212.5 | 0.0 | 0.5 | 0.0 | 0.0 | 0.3 | 213.3 |
| 15 | 2 | 2016-03-10 08:00:55 | 2016-03-10 08:03:20 | 1 | 1.51 | -74.18675994873048 | 40.69744873046875 | 5 | N | -74.17733764648438 | 40.69512176513672 | 1 | 100.0 | 0.0 | 0.0 | 0.01 | 0.0 | 0.3 | 100.31 |
| 16 | 2 | 2016-03-10 08:01:08 | 2016-03-10 08:02:46 | 1 | 0.0 | -73.81371307373048 | 40.89707946777344 | 5 | N | -73.81371307373048 | 40.897090911865234 | 1 | 106.0 | 0.0 | 0.0 | 15.0 | 0.0 | 0.3 | 121.3 |
| 17 | 2 | 2016-03-10 08:05:32 | 2016-03-10 08:58:31 | 1 | 19.24 | -73.9886703491211 | 40.77503967285156 | 3 | N | -74.17704010009764 | 40.69496154785156 | 1 | 77.0 | 0.0 | 0.0 | 17.96 | 12.5 | 0.3 | 107.76 |
| 18 | 2 | 2016-03-10 08:06:43 | 2016-03-10 09:05:58 | 1 | 40.27 | -74.0052719116211 | 40.744930267333984 | 5 | N | -74.48404693603516 | 40.52020263671875 | 1 | 171.0 | 0.0 | 0.5 | 20.0 | 17.0 | 0.3 | 208.8 |
| 19 | 2 | 2016-03-10 08:08:23 | 2016-03-10 08:57:15 | 5 | 13.87 | -73.87235260009764 | 40.77408218383789 | 5 | N | -73.97357177734375 | 40.86507034301758 | 1 | 100.0 | 0.0 | 0.0 | 20.06 | 0.0 | 0.3 | 120.36 |
| 20 | 2 | 2016-03-10 08:10:49 | 2016-03-10 08:57:12 | 1 | 25.62 | -73.96985626220702 | 40.75365829467773 | 1 | N | -73.80845642089844 | 41.0317497253418 | 1 | 69.0 | 0.0 | 0.5 | 34.0 | 5.54 | 0.3 | 109.34 |

#### CTAS Managed
- Everything is the same except the data is now owned by athena because the data will be stored in the S3 bucket managed by Athena. Hence the data will not be owned by you but AWS.
- Unlike External CTAS here in managed CTAS you don't have to provide the location of where you want to store the data.
- What is a managed location? 
    - There is a special folder that AWS glue will create whenever you want to create a managed data or managed table in AWS
- Unlike External CTAS if you delete the managed CTAS then the actual data will also be deleted along with the metadata. Hence we can say that the data is not owned by you but AWS Glue.
- How to create a managed CTAS
```sql
CREATE TABLE uber_data_most_tipped_filtered_managed_table 
AS 
SELECT * FROM uber_data_external_table WHERE TRY_CAST(tip_amount AS DOUBLE) > 5; 
```

#### Conclusion
**Advantages of External CTAS :**<br>
- Full control over data location 
    - This is mandatory in:
        - Medallion architecture
        - Enterprise data lakes
        - Glue + Athena + EMR setups
    - Managed CTAS:
        - Dumps data in Athena’s results bucket
        - Breaks lake organization
        - Mixes temp + permanent data
- Data is not tied to athena
    - External CTAS output can be used by:
        - AWS Glue
        - Amazon Redshift Spectrum
        - EMR / Spark
        - AWS Lake Formation
        - Other Athena workgroups
    - Managed CTAS data:
        - Is logically Athena-owned
        - Lives in query-result bucket
        - Not intended as a shared dataset
- Predictable permissions and security
    - With external CTAS:
        - You control bucket policies
        - You control IAM roles
        - You control cross-account access
    - Managed CTAS:
        - Uses Athena’s execution role
        - Harder to govern
        - Risk of accidental exposure
- Lifecycle and cost control
    - External CTAS lets you:
        - Apply S3 lifecycle rules
        - Transition to Glacier
        - Auto-delete old partitions
    - Managed CTAS:
        - Lives in Athena results bucket
        - Often accumulates junk
        - Higher long-term cost
- Required for production pipelines
    - External CTAS is required for:
        - Silver / Gold layers
        - Repeatable pipelines
        - Versioned datasets
        - CI/CD-controlled analytics tables
    - Managed CTAS are not pipeline safe.

#### **External CTAS VS Managed CTAS** <br>

| Aspect              | Managed CTAS   | External CTAS |
| ------------------- | -------------- | ------------- |
| Data lake friendly  | No             | yes           |
| Governance          | No             | yes           |
| Reusable by Glue    | questionable   | yes           |
| Safe for production | No             | yes           |
| Easy cleanup        | No             | manual        |
| Best for            | Experiments    | Pipelines     |

**NOTE :** <br>
- Usually when we create a Managed table or CTAS in traditional platform like Databricks and if you drop that table when data is also deleted along with table's metadata.
    - Databricks chooses storage location inside DBFS / Unity catalog
    - Owns both metadata and data
    - Treats it as fully lifecycle-managed
    - So when I drop a managed table in databricks then it not only is able to delete the metadata of the table but also its actual data.
        - How you drop a managed CTAS
        ```sql
        DROP TABLE uber_data_most_tipped_filtered_managed_table;
        ```
- But in AWS the behaviour is different. Even if you delete the Managed table in AWS athena your managed table data in S3 will not be deleted because:
    - Athena is a query engine only
    - Athena is serverless
    - Athena is stateless
    - Athena does not own storage
    - All data lives in
        - All data lives in AWS S3
        - Glue Data Catalog is only used to store table's metadata
    - **What managed table actually means in Athena?**
        - Athena chooses an S3 path automatically
        - Usually inside the Athena results bucket
        - But the data is still in my S3 bucket
    - So when I delete a managed table from AWS using Athena
        - Athena deletes the managed table's metadata
        - Athena does not deletes the S3 objects.
        - S3 is an independednt service of Athena i.e Athena does not assumes it owns the S3 bucket.
- **So we can say that the difference between the managed and external table in case of Athena is that:**
    - When creating external table you have to tell Athena where to store your query result
    - When creating managed table you don't have to tell Athena where to store the query result it will automatically store the query result in the S3 bucket that you set in the Athena's settings

Athena related docs : https://docs.aws.amazon.com/athena/latest/ug/what-is.html

## AWS Glue Crawlers 
- Normally when you have data in files lets say a bunch of csv files in S3 and you want to query those csv files using Athena.
- The only way you do that is by registering those files as a table in AWS Glue catalog using Athena
    - One of the way to register the tables is to define the schema like I did using the AWS UI where I used bulk add columns option provided under column details section.
    - This method is only good for when you only have 8 to 10 columns beyond that this method doesn't make any sense.
    - Suppose when we do not want to prune the columns and we simple want to do the lift and shift and during the lift and shift we find out the number of columns are really really huge lets say 100 columns are there in the table.
    - Writing 100s of columns manually is not possible in this case.
- This is where the concept of crawler comes into the picture.
    - Now when we use crawler we can just use it to crawl through all of the csv files where our actual data is present (It has the capability to crawl through multiple hirerachial folders : In form of partitions we have to crawl through the hirerachial folders)
    - This crawler will crawl through all of the files and this will infer the schema (It will guess which column should be of which type) and it will automatically register this table in AWS glue catalog.
    - Creawler is also used for schema evolution
        - Suppose we have recieved a file in day 1 where the file has 100 columns and in day 2 we recieve a file where the file has 101 columns instead of 100, this is called schema evolution.
        - We just have to say it to our crawler that if you see any other column just add it to the table.
        - It can literally infer the schema whenever you want.

#### NOTE:
- When I was registering a managed or external table in AWS glue catalog from data files stored in S3 I was able to do that because I was the root user and as a root user I had the permissions to access the S3 bucket and Glue catalogs etc. 
- But when I try to register the crawler to register the managed or external tables using the data files stored in S3 bucket then I will have to give appropriate permissions to crawler to register the table on my behalf.
- By default crawler won't be able to see anything in S3 by itself because S3 is fully secured and crawler also doesn't have any permissions to execute anything in AWS glue.
- This means crawler cannot do anything without IAM (permissions)
- IAM is a component of AWS which allows us to provide access to users and services within the AWS ecosystem.
- So a crawler must carry the access (IAM aka Role) under the hood and this access is actually carry the permissions for AWS glue and S3 bucket.

#### What are IAM roles?
- An IAM Role is a set of permissions that can be assumed temporarily by:
    - AWS services
    - Users
    - Applications
    - Other AWS accounts
- Unlike IAM users, roles do not have permanent credentials (no fixed passwords or access key)
- They provide temporary security credentials when assumed.

| Feature                      | IAM User | IAM Role     |
| ---------------------------- | -------- | ------------ |
| Has permanent credentials    | Yes      |   No         |
| Used by humans               | Yes      |   Usually no |
| Used by AWS services         | No       |   Yes        |
| Temporary access             | No       |   Yes        |
| Best practice for production | No       |   Yes        |

### Before you create a crawler you need to create a role that the crawler will use to access S3 and AWS glue catalog for it to be able to register tables from the data present in the S3 bucket
**STEP 1:**<br>
- Search for IAM in AWS and once you hit enter you will be greeted with IAM main page.
- Once you are there you need to go to the side panel on the left and select Role option there.

**STEP 2:**<br>
- Create a new role for Glue service and then hit next
- After hitting next you will be greeted with a new page where you will have to configure the permissions policies for your role that you are creating.

![create_a_role_for_glue](images/create_a_role_for_glue.png)

**STEP 3:**<br>
- In the permission policies page search for glue related permissions
- There you need to check AWSGlueServiceRole IAM policy 
- AWSGlueServiceRole is an AWS managed IAM policy designed for: Allowing AWS Glue to run jobs, crawlers, and workflows. It gives Glue the basic permissions it needs to function properly.
    - This allows:
        - Creating tables
        - Updating metadata
        - Managing crawlers
        - Reading the data catalog

![providing_access_to_glue_service_role](images/providing_access_to_glue_service_role.png)

**STEP 4:** <br>
- Now in permissions policies page I need to search for s3
- There from the list select AmazonS3FullAccess
- AmazonS3FullAccess is an AWS managed IAM policy that grants full administrative access to all Amazon S3 resources in your account.
    - Whoever has this policy can do anything in S3.
    - For learning purposes and to keep things simple I am going to use this policy.
    - It is not at all recommended to use this AmazonS3FullAccess policy in the production environment since it can compromise the data sitting in the S3 in the event if the security key gets leaked.
    - You should always use your custom policy and exactly define what permissions you want to give to crawler when performing operations in S3.

![provide_s3_permissions_to_this_role](images/provide_s3_permissions_to_this_role.png)

**STEP 5:**<br>
- Once you give appropriate permissions just hit next and in the next page you just have to give a name to the role and hit create and your new role will be created.

### How to register a table in AWS glue using AWS crawler?
**STEP 1:**<br>
Go to AWS glue and on the side panel you will find the option named crawlers press this opiton button. After pressing this button you will be greeted with the crawler page.

**STEP 2:**<br>
In this crawler page you will see create crawler button by pressing this button you will greeted with another screen create crawler page.

**STEP 3:**<br>
- In set crawler properties page give your crawler a name and hit next button

![set_crawler_properties](images/set_crawler_properties.png)

**STEP 4:**<br>
- Here in this page I have to set the data sources : Just click on ```add data sources``` button

![choose_data_source_and_classifiers](images/choose_data_source_and_classifiers.png)

- A new ```add data sources``` pop up will open

![add_source_pop_up](images/add_source_pop_up.png)

- Here in this add data sources pop up I am going to set S3 location and hit next after saving the S3 bucket location in that pop up.
- After hitting next you will be greeted with Configure security settings

![choose_s3_path](images/choose_s3_path.png)

**STEP 5:**<br>
- In Configure security settings page you will have to attach the IAM role that you created earlier to this crawler.
- If you have not created the IAM role then this page will provide you with an option to create a new IAM role right then and there but in this case that role will only have the AWSGlueServiceRole permission and not AmazonS3FullAccess which if not carefull can cause problem down the line.

**STEP 6:**<br>
- Attach the database and tell the crawler what should be your table name even though it is optional and hit create.
- Your new crawler will be created and after creation hit run 
- After the successful run of the crawler the table will be created in AWS glue catalog.

#### **Error I faced when ingesting data from csv files using crawler** <br>
- In my case the table that it generated was not right for some reason It added the column names as col_1, col_2 etc.. and the actual column names were inserted as the first row in the table.
- ![error_when_ingesting_data_via_crawler](images/error_when_ingesting_data_via_crawler.png)
#### Solution : Error I faced when ingesting data from csv files using crawler
- The reason why the crawler when ingesting data from the csv file stored in S3 created a table where the actual column names were added in the first row instead of the header is because when creating crawler I forgot to create classifier for it.
- **How to create classifier for crawler?**
    - ![crawler_classifier](images/crawler_classifier.png)
    - You need to configure your crawler's classifier depending on the type of file you want to ingest
    - Here in this case I am trying to register the table for my csv file using crawler hence I chose the options as shown in the picture above.
- One thing to note even after adding this crawler I was not able to get the proper results when I attempted to re-ingest a csv file after deleting its data from the s3 bucket manually where athena stores its query results. 
- The reason for it to happen is because if you crawled a file without any classifer attached to it and you attached the classifier after you crawled that file.
- The classifier will not be used even if you re-ingest that file after deleting all its data from the s3 bucket where athena stores its query result. 
- The only way is to delete the csv file from the source , delete the data where the query results are stored and then re-run the classifier.


## AWS Visual ETL
AWS Visual ETL is a blend of data factory and data flows. <br>
![AWS_Visual_ETL](images/AWS_Visual_ETL.png)

- This is the visual editor where you define your ETL jobs. This is visual ETL builder with No code philosophy. 
- Keep in mind eveything you do in this visual canvas there is a script behind it. The script is being generated behind the scene dynamically based on the componenets you choose and set in your ETL pipeline when desinging the ETL pipeline using this AWS visual ETL tool.
- In here we work with apache spark under the hood when using this tool using the low or no code philosophy.

### Nodes : 
Nodes are tasks that you add in your AWS visual ETL job job.

AWS has created different connectors that allows us to pull data from different sources.

The first transformation in any data engineering pipeline is that you will have to apply drop duplicate transformation before anything else because one must not have duplicate values in the table. 

### Setting up version control for ETL pipeline
![version_control](images/version_control.png)
One thing to note is that when setting up your version control you must have a main branch and if you newly created a repository on github then your branch doesn't exists so you may get an error when trying to save the job.

To solve this issue you will have to create a file and push it into the repo the moment you do it the your AWS visual ETL version control tool automatically picks up the main branch hence you are now able to set a branch to which the code will be pushed to.


You will generally not have to set the version control related stuff in your pipeline you will simply create a new branch and set that branch to it but you should know it.


## How to trigger crawlers on file arrival?
### Architecture
```bash
S3 (new file)
   ↓  (Event notification)
Lambda
   ↓
Glue Crawler (StartCrawler API)
   ↓
Glue Data Catalog updated
   ↓
Athena ready
```

### Create an IAM role for Lambda function
For this I will be needing an IAM role with a custom policy. 
Policy that I require is this : 
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartCrawler",
        "glue:GetCrawler",
        "glue:StopCrawler"
      ],
      "Resource": "arn:aws:glue:ap-south-1:YOUR_ACCOUNT_ID:crawler/crawler_demo"
    }
  ]
}
```
along with this custom policy I will also be needing AmazonS3FullAccess policy provided my AWS

**STEP 1 :** 
- Create a custom policy which gives permissions to other AWS services (in this my Lambda function) to execute and run my AWS glue crawler. <br>
- Go to the side panel > policies > create policy
![policy_page](images/IAM/policy_page.png)

**STEP 2:** 
- Here in the create policy page you will see specify permissions section.
- Toggle from visual to JSON here you will have to paste this JSON code that will define your custom policy
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartCrawler",
        "glue:GetCrawler",
        "glue:StopCrawler"
      ],
      "Resource": "arn:aws:glue:ap-south-1:YOUR_ACCOUNT_ID:crawler/crawler_demo"
    }
  ]
}
```
![specify_permission](images/IAM/specify_permission.png)

**STEP 3 :**
- In review and create page you will have to provide the policy name and description what your custom policy does 
- After that hit create policy button
![review_and_create_policy](images/IAM/review_and_create_policy.png)

**STEP 4 :**
- After you have created your custom policy you will be able to search it using the name that you gave to your policy
- For demo purpose I have created a custom policy with a name called ```test_policy```
![search_custom_policy](images/IAM/search_custom_policy.png)

**NOTICE :** I have created a custom policy named ```AWSGlueCrawlerPermissionPolicy``` this is one of the policy that will be requiring by the IAM role that I will be attaching to my lambda function for it work properly and trigger my crawler when new files arrive in my S3 bucket.

**STEP 5:** 
- Now I am going to create a new IAM role.
- Go to the side panel > Roles > create role
![IAM_role_create](images/IAM/IAM_role_create.png)

**STEP 6:**
- In Trusted entity type section here I will select AWS service 
- Under use case section I will select Lambda option and then hit next
![select_trusted_entity](images/IAM/select_trusted_entity.png)

**STEP 7:** 
- In Add permissions page under permission policies section search for you custom policy that you created earlier in my case my custom policy name is ```AWSGlueCrawlerPermissionPolicy``` and then select that policy to be included in the IAM role that you are creating.
![search_and_select_custom_policy](images/IAM/search_and_select_custom_policy.png)
- Now you will have to search for ```s3``` You will have to add this ```AmazonS3FullAccess``` policy to your role
- After adding these two policies to your IAM role you can hit next
![s3_bucket_policy](images/IAM/s3_bucket_policy.png)

**STEP 8:**
- After hitting next you will be greeted to a page where you will have to provide the name and description of what this role does.
![name_review](images/IAM/name_review.png)

**NOTICE :** In my case I have already created a role with a name ```TriggerGlueCrawler``` for my lambda function where I have attached my custom policy and AmazonS3FullAccess policy and made sure that my lambda function has approprioate permissions to function as intended.

### Create a Lambda function
- After the IAM role for this Lambda function is created successfully I will now create the lambda function itself.
- This lambda function will be responsible for triggering my AWS glue crawler programatically using a python script when a new file arrives in the S3 bucket.

**STEP 1:**
- Go to Lambda function page my searching it in the search box
- Go to the side panel > Functions > Create functions page.
![create_lambda_function](images/Lambda_function/create_lambda_function.png)

**STEP 2:**
- In this page provide function name 
- Select I will be selecting python as a runtime
- Make sure you don't select durable execution option otherwise your lambda function won't work without setting the destination.
- Under the change default execution role select the option ```Use existing role```
    - From the drop down menu select the role that you created earlier in my case the role that I created was ```TriggerGlueCrawler``` hence I will be selecting that.
- After this click on create function 
![create_lambda_function_page](images/Lambda_function/create_lambda_function_page.png)

**STEP 3:**
- Write Lambda function in this page and click on deploy button after you have written your code to trigger the crawler service 
![write_lambda_code](images/Lambda_function/write_lambda_code.png)
- This code below will start AWS glue crawler when this executes in Lambda function.
- After this click on create function button and your lambda function will be created
```python

import boto3

def lambda_handler(event, context):
    
    crawler_name = "customers_data_silver_table"
    glue = boto3.client('glue')
    
    crawler = glue.get_crawler(Name=crawler_name)
    
    state = crawler['Crawler']['State']
    
    if state == 'READY':
        glue.start_crawler(Name=crawler_name)
        return {
            'statusCode': 200,
            'body': f"Crawler {crawler_name} started."
        }
    else:
        return {
            'statusCode': 200,
            'body': f"Crawler already running."
        }
```

**STEP 4:** **Create an event Listner for S3 bucket**
- Now you will have to set the trigger for your lambda function 
- For this there are two options either you set the trigger by using the menu and options provided inside the lambda function itself or you can do it from S3 bucket
- I chose to attach the lambda function to an event (trigger) after creating it in the S3 bucket properties 
- Go to Amazon S3 > Buckets > properties
- Under the Event notifications menu you will see create event notification button click it to create a new event notification.
![Event_notifications](images/Lambda_function/Event_notifications.png)

**STEP 5:** **Attach the previously created Lambda function to that S3 bucket event**
- After clicking the create event notification button you will be greeted with this page.
- In this page you need to provide your event name , set and configure the event types, and last but not least you will have to set the lambda function that you created just now to this s3 bucket event essentially tie this as a trigger to the lambda function
![create_event_s3_page](images/Lambda_function/create_event_s3_page.png)
![create_event_s3_page2](images/Lambda_function/create_event_s3_page2.png)

 **So how this works ?** <br>
- The moment my ETL pipeline reads the csv file from the ingestion s3 directory it processes the data and then creates parquet files in this S3 bucket. 
- The moment new parquet files arrive after a successfull ETL execution the S3 put event triggers the lambda function which has the python script to trigger the AWS glue crawler. 
- Which in turns creates metadata in AWS glue catalog using those parquet files and now the data in those files can be queried using AWS Athena using sql.

### Create an AWS glue crawler
**STEP 1:**
- Create a new crawler
- This crawler must crawl through a bunch of parquet files in AWS S3 bucket and register their metadata and create a table in AWS glue catalog. 
- This will give us the ability to use AWS Athena to query the data from the parquet file present in S3 bucket.
- Go to AWS glue > side bar > crawlers > create crawler
![create_crawler](images/aws_glue/crawler/create_crawler.png)

**STEP 2:**
- After you click create crawler button you will be greeted with this page.
- Here you will set the name and description for your crawler
- After that hit next
![set_crawler_properties](images/aws_glue/crawler/set_crawler_properties.png)

**STEP 3:**
- After you hit next you will be greeted with this page.
- Here in this page you will add the source where you will have to provide the details regarding your Source s3 bucket location and the folders inside it where the files exists.
- If you are registering the metadata of a table where the data is in csv file then you will have to compulsory create a classifier if you haven't already created one
- But here in this case since the files are in parquet format we don't have to set any classifier.
![set_source_classifier](images/aws_glue/crawler/set_source_classifier.png)

**STEP 4:**
- Here in this page you will have to attach an IAM role that will have appropriate policies attached to it and will allow the crawler to access S3 bucket and AWS glue catalog for it to be able to read the parquet file and register the metadata related to those files in AWS glue catalog so that Athena can use it to query data from those files using SQL.
- It is a best practice that you create your IAM role before hand 
- This IAM role must have ```AWSGlueServiceRole``` and ```AmazonS3FullAccess``` policies attached to it. 
![configure_security](images/aws_glue/crawler/configure_security.png)

**STEP 5:**
- Here you will have to set the output and scheduling 
- Select the database that you created in your AWS glue catalog
- Inside the advanced options set how AWS must handle the table updates if the changes in schema occurs (schema evolution handling)
- Last but not least set the crawler shcedule I have set it to on demand.

## NOTE :
Here are some of the custom policies I experimented with and it worked like a charm hence I am including it here just in case if you need it

- **Create a policy that gives permission to a service to start and get glue crawler**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:StartCrawler",
                "glue:GetCrawler"
            ],
            "Resource": "arn:aws:glue:ap-south-1:406868976171:crawler/RegisterParquetFiles"
        }
    ]
}
```
- **Create a policy to allow services to access glue visual ETL**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "glue:StartJobRun",
            "Resource": "arn:aws:glue:ap-south-1:406868976171:job/Process_Sales_data"
        }
    ]
}
```
- **Create SQS policy to allow services to access SQS messages (This is not needed but I am mentioning it here just in case you want to go this route)**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Statement1",
            "Effect": "Allow",
            "Action": [
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes",
                "sqs:ChangeMessageVisibility"
            ],
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:S3FileArrivalQueue"
        }
    ]
}
```

## Incremental Load
- Suppose I have csv file named ```day1.csv``` file and on day 1 my glue pipeline ingested data from it.
- Then came day2 now there is another file ```day2.csv``` file and on day 2 my glue pipeline ran to ingest data from it but now along with the data in ```day2.csv``` file ```day1.csv``` file is also ingested.
- This is the default behaviour of AWS glue.
- This is not a desirable behaviour because this will cause unecessary data redundancy issue and will cause data management issues down in the long run as our data grows in size.
- In order to mitigate this issue we can use the option called Idempotency.
- Idempotancy means exactly once behaviour. We can achieve it using bookmarks.
- What a bookmark does is it keeps track of the data that has already been ingested by AWS glue visual ETL pipeline and makes sures that the ingestion process only runs for new files only.

### Implementing Incremental Load (Version 1)
- Things that I want to implement here:
    - Implement automatic visual ETL pipeline trigger when a new file arrives in S3 bucket automatically using Lambda function.
    - Then after ingestion again use Lambda funtion to register the data in AWS glue catalog so that we can use Athena to use SQL to query the data in parquet files prsent in the S3 bucket.
### Architectural diagram
```bash
                ┌─────────────────────────┐
                │     Upstream System     │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │   S3 Source Bucket      │
                │   (CSV Files)           │
                └────────────┬────────────┘
                             │  (PUT Event)
                             ▼
                ┌─────────────────────────┐
                │      Lambda Function    │
                │  (Start Glue Job)       │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │     Glue Visual ETL     │
                │  - Bookmark Enabled     │
                │  - Clean + Transform    │
                │  - Convert Types        │
                │  - Write Parquet        │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │   S3 Data Sink Bucket   │
                │     (Parquet Files)     │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │   Glue Data Catalog     │
                └────────────┬────────────┘
                             │
                             ▼
                ┌─────────────────────────┐
                │        Athena           │
                │      (SQL Queries)      │
                └─────────────────────────┘
```
### Preparing IAM roles for the services
**STEP 1:** Create an IAM role for Lambda function that starts AWS glue visual ETL pipeline to make sure that it has proper permissions to trigger AWS visual ETL pipeline when put event is triggered in source AWS S3 bucket.
- Create a custom policy for this IAM role. You are free to use the code below to attach it to the Role that you will be creating for this Lambda function.
- It gives appropriate permissions to allow the lambda function to trigger AWS glue visual ETL pipeline.
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "glue:StartJobRun",
      "Resource": "arn:aws:glue:REGION:ACCOUNT_ID:job/YOUR_GLUE_JOB_NAME"
    }
  ]
}
```
- Now attach this policy to the IAM role which will then be used by the lambda function that is going to trigger this AWS glue visual ETL when new files arrives in the source S3 bucket.

**STEP 2:** Create an IAM role for AWS glue 
- This role allows AWS glue to access S3 bucket and It also allows AWS Glue to run jobs, crawlers, and workflows.
- Here in this case we don't need to create a custom policy we can simply use AWS managed policies ```AWSGlueServiceRole``` 
    - For AWS access policy use this json instead to create a custom policy to allow glue to access source S3 securely
    ```json
        {
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket"
              ],
              "Resource": [
                "arn:aws:s3:::aws-glue-s3-bucket-one",
                "arn:aws:s3:::aws-glue-s3-bucket-one/*",
                "arn:aws:s3:::data-sink-one",
                "arn:aws:s3:::data-sink-one/*"
              ]
            }
          ]
        }
    ```
- Once this role is created we have to attach it to our AWS glue visual ETL job that we have created so that it has appropriate permissions to carry out its job properly.
### Create AWS glue visual ETL job
- This AWS glue visual ETL job is responsible for : 
    - Ingesting data from csv file
    - Cleaning data and converting columns that have numbers into double or integers from string data types accordingly, removing un-necessary symbols.
    - Saving the file in the parquet file in an output S3 bucket.
    - Last but not least this ETL must only read new files that arrive in S3 bucket and not the old ones that have already been processed.
- When saving the job 
    - It will ask you to attach the IAM role that you created without this it will not allow you to save your AWS glue visual ETL job so this is mandatory.
    - ![AWS_glue_creation1](images/aws_glue/visualETL/AWS_glue_creation1.png)
    - You must also set job bookmarks to enable so that ETL only reads new files and ignores older files in S3 that have already been processed when it gets triggered.
    - ![enable_bookmarks](images/aws_glue/visualETL/enable_bookmarks.png)
    - We are doing this to save us from un-necessary costing that happens when the files are re-processed even when it is not required. Its a waste of precious compute resources.
- Your AWS glue visual ETL pipeline should look something like this.
![visual_ETL_pipeline](images/aws_glue/visualETL/visual_ETL_pipeline.png)
- Set data source S3 bucket (Setting up data ingestion node)
![set_data_source_s3](images/aws_glue/visualETL/set_data_source_s3.png)
- Here I have used SQL node to clean data (Setting up data cleaning node)
```sql
    SELECT
        product_id,
        product_name,
        category,
        about_product,
        user_id,
        user_name,
        review_id,
        review_title,
        review_content,
        img_link,
        product_link,

        -- discounted_price: ₹149 → 149.0
        CAST(
            REGEXP_REPLACE(discounted_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS discounted_price,

        -- actual_price: ₹1,000 → 1000.0
        CAST(
            REGEXP_REPLACE(actual_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS actual_price,

        -- discount_percentage: 85% → 85.0
        CAST(
            REGEXP_REPLACE(discount_percentage, '[^0-9.]', '')
            AS DOUBLE
        ) AS discount_percentage,

        -- rating: 3.9 → 3.9
        CAST(
            REGEXP_REPLACE(rating, '[^0-9.]', '')
            AS DOUBLE
        ) AS rating,

        -- rating_count: 24,871 → 24871
        CAST(
            REGEXP_REPLACE(rating_count, '[^0-9]', '')
            AS INT
        ) AS rating_count

    FROM myDataSource;
```
- Use Drop Null Fields node to drop the rows that have all the columns as null (This is a pre-build node provided by AWS)
![drop_null_field_node](images/aws_glue/visualETL/drop_null_field_node.png)
- Set the target to be an S3 bucket using data target S3 bucket node 
    - Here you will have to set the file format in which you want to get output into. The best option to go with is parquet format since it has the actual data along with the table's metadata inside of it. 
    - In the data catalog options if you want your visual ETL pipeline to automatically register the table in AWS glue catalog so that you can use athena to query the table then you will have to go with this option ```Create a table in the Data Catalog and on subsequent runs, keep existing schema and add new partitions```
        - We want to prevent schema changing on its own. We would rather want our pipeline to fail than to cause issues as listed below:
            - Senario 1 the data type of a column changes then 
                - Athena queries may fail
                - Downstream dashboards break
                - BI tools complain
                - Historical partitions may mismatch types
            - Senario 2 Column is removed 
                - Older partitions still have that column but the new one's won't hence again breaking the downstream services that depeneds on that particular column
                - Athena behaves inconsistently
            - Senario 3 A new column is added
                - This one is usually safe — but even then, you should manage schema intentionally.
    - ![target_s3_set_format](images/aws_glue/visualETL/target_s3_set_format.png)
    - Next you will have to set the database that you must have created in AWS glue data catalog
    - Then you will have to set the table name 
    - Add partition key if you think its needed
    - ![target_s3_set_database_table_name_and_partition](images/aws_glue/visualETL/target_s3_set_database_table_name_and_partition.png)
- This is the generated script from the visual ETL pipeline 
```python
    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.gluetypes import *
    from awsgluedq.transforms import EvaluateDataQuality
    from awsglue import DynamicFrame

    def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
        for alias, frame in mapping.items():
            frame.toDF().createOrReplaceTempView(alias)
        result = spark.sql(query)
        return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
    def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
        if isinstance(schema, StructType):
            for field in schema:
                new_path = path + "." if path != "" else path
                output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
        elif isinstance(schema, ArrayType):
            if isinstance(schema.elementType, StructType):
                output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
        elif isinstance(schema, NullType):
            output.append(path)
        else:
            x, distinct_set = frame.toDF(), set()
            for i in x.select(path).distinct().collect():
                distinct_ = i[path.split('.')[-1]]
                if isinstance(distinct_, list):
                    distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
                elif isinstance(distinct_, str) :
                    distinct_set.add(distinct_.strip())
                else:
                    distinct_set.add(distinct_)
            if isinstance(schema, StringType):
                if distinct_set.issubset(nullStringSet):
                    output.append(path)
            elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
                if distinct_set.issubset(nullIntegerSet):
                    output.append(path)
        return output

    def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
        nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
        return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

    args = getResolvedOptions(sys.argv, [
        'JOB_NAME'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Default ruleset used by all target nodes with data quality enabled
    DEFAULT_DATA_QUALITY_RULESET = """
        Rules = [
            ColumnCount > 0
        ]
    """

    # Script generated for node Raw data source S3
    RawdatasourceS3_node1772431168408 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", 
        connection_options={"paths": ["s3://aws-glue-s3-bucket-one/raw_data/sales_data/"], "recurse": True}, 
        transformation_ctx="RawdatasourceS3_node1772431168408")

    # Script generated for node SQL Query
    SqlQuery61 = '''
    SELECT
        product_id,
        product_name,
        category,
        about_product,
        user_id,
        user_name,
        review_id,
        review_title,
        review_content,
        img_link,
        product_link,

        -- discounted_price: ₹149 → 149.0
        CAST(
            REGEXP_REPLACE(discounted_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS discounted_price,

        -- actual_price: ₹1,000 → 1000.0
        CAST(
            REGEXP_REPLACE(actual_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS actual_price,

        -- discount_percentage: 85% → 85.0
        CAST(
            REGEXP_REPLACE(discount_percentage, '[^0-9.]', '')
            AS DOUBLE
        ) AS discount_percentage,

        -- rating: 3.9 → 3.9
        CAST(
            REGEXP_REPLACE(rating, '[^0-9.]', '')
            AS DOUBLE
        ) AS rating,

        -- rating_count: 24,871 → 24871
        CAST(
            REGEXP_REPLACE(rating_count, '[^0-9]', '')
            AS INT
        ) AS rating_count

    FROM myDataSource;
    '''
    SQLQuery_node1772431252856 = sparkSqlQuery(glueContext, query = SqlQuery61, mapping = {"myDataSource":RawdatasourceS3_node1772431168408}, transformation_ctx = "SQLQuery_node1772431252856")

    # Script generated for node Drop Null Fields
    DropNullFields_node1772431857999 = drop_nulls(glueContext, frame=SQLQuery_node1772431252856, nullStringSet={"", "null"}, nullIntegerSet={-1}, transformation_ctx="DropNullFields_node1772431857999")

    # Script generated for node Silver layer data sink S3
    EvaluateDataQuality().process_rows(frame=DropNullFields_node1772431857999, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772428328053", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
    SilverlayerdatasinkS3_node1772432020219 = glueContext.getSink(path="s3://data-sink-one/silver_layer/sales_data/", connection_type="s3", updateBehavior="LOG", partitionKeys=["category"], enableUpdateCatalog=True, transformation_ctx="SilverlayerdatasinkS3_node1772432020219")
    SilverlayerdatasinkS3_node1772432020219.setCatalogInfo(catalogDatabase="aws-glue-tutorial-aditya",catalogTableName="silver_table_sales_data")
    SilverlayerdatasinkS3_node1772432020219.setFormat("glueparquet", compression="snappy")
    SilverlayerdatasinkS3_node1772432020219.writeFrame(DropNullFields_node1772431857999)
    job.commit()
```

### NOTE:
- During testing you may want to ingest the same file again and again but since you have set the bookmark checkbox to be true in your AWS glue visual ETL pipeline it will not read the same file twice 
- In order to solve this problem you have the capability to reset bookmark for your visual ETL pipeline 
![reset_bookmark](images/aws_glue/visualETL/reset_bookmark.png)

### Start AWS glue visual ETL automatically
- Right now I have to manually run AWS glue visual ETL pipeline 
- I want to automate this process 
    - I want the pipeline to only run when a new csv file arrives in the source S3 bucket.
- One of the ways we can do this is utilizing a Lambda function that triggers this pipeline when a new file arrives in the source S3 bucket

**STEP 1:**
- First we have to create a role for our lambda function with appropirate policies attached to it
- ![create_role_for_lambda_function](images/aws_glue/Lambda_function/create_role_for_lambda_function.png)
- Create a policy with this rule
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "glue:StartJobRun",
            "Resource": "arn:aws:glue:ap-south-1:406868976171:job/ingest_sales_data"
        }
    ]
}
```
- Attach that policy to this IAM role 
- You will also need to attach this ```AWSLambdaBasicExecutionRole``` policy so that lambda function could actually write logs to cloudwatch service.

**STEP 2:** 
- Create a lambda function to trigger AWS glue visual ETL pipeline 
- First we have to set the name for our Lambda function
- Choose the runtime (your preferred programming language) 
- Select the architecture you want to use on which your lambda function will run on
- ![set_lambda_function_name](images/aws_glue/Lambda_function/set_lambda_function_name.png)
- Attach IAM role to this Lambda function in order to make sure that it has permission to actually start AWS glue visual ETL pipeline 
- ![attach_IAM_role_to_lambda](images/aws_glue/Lambda_function/attach_IAM_role_to_lambda.png)
- Write the code to trigger ETL pipeline in glue for your lambda function here in this page
- ![write_code_to_trigger_aws_glue_ETL](images/aws_glue/Lambda_function/write_code_to_trigger_aws_glue_ETL.png)
- Set Lambda Environment Variable
- ![add_lambda_function_env_vars](images/aws_glue/Lambda_function/add_lambda_function_env_vars.png)
- here is the lambda function code 
```python
    import json
    import boto3
    import os

    glue_client = boto3.client("glue")

    GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]

    def lambda_handler(event, context):
        try:
            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME
            )

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Glue job started successfully",
                    "jobRunId": response["JobRunId"]
                })
            }

        except Exception as e:
            return {
                "statusCode": 500,
                "body": str(e)
            }
```


**STEP 3:** 
- Create an event notification in the source S3 bucket where the files are dumped by the upstream applications/services and attached the lambda function that you created earlier to this event
- ![create_source_s3_event_notification](images/aws_glue/automate_visual_ETL/create_source_s3_event_notification.png)
- Set the event name and event type 
    - Here I have set the event type to be of PUT type
    - The PUT type event detects if a new object is created in S3 bucket if yes then it will trigger the lambda function which has the code to start the AWS glue visual ETL pipeline 
- ![FileArrivalEvent](images/aws_glue/automate_visual_ETL/FileArrivalEvent.png)
- In the same page if you scroll down you will find the options to attach a lambda function that you created earlier to this event 
- ![set_lambda_function_here](images/aws_glue/automate_visual_ETL/set_lambda_function_here.png)

### Implementing Incremental Load (make this implementation production ready)
In order to make incremental load pipeline production ready there few things I need to implement

#### Proposed architecture for this implementation
```bash
                              ┌──────────────────────────┐
                              │     Upstream System      │
                              │  (Apps / APIs / Batch)   │
                              └──────────────┬───────────┘
                                             │
                                             ▼
                              ┌──────────────────────────┐
                              │   S3 RAW Landing Zone    │
                              │  (Versioning Enabled)    │
                              └──────────────┬───────────┘
                                             │
                                (S3 PUT Event Notification)
                                             │
                                             ▼
                              ┌──────────────────────────┐
                              │     EventBridge Rule     │
                              └──────────────┬───────────┘
                                             │
                                             ▼
                              ┌──────────────────────────┐
                              │        Lambda Trigger    │
                              │ - Extract bucket/key     │
                              │ - Validate event         │
                              │ - Idempotency check      │
                              │ - Write metadata record  │
                              └──────────────┬───────────┘
                                             │
                           ┌─────────────────┴──────────────────┐
                           ▼                                    ▼
              ┌──────────────────────┐              ┌──────────────────────┐
              │ DynamoDB Metadata    │              │    SQS (DLQ)         │
              │ - File processed?    │              │ - Failed events      │
              │ - Status tracking    │              │ - Replay capability  │
              └──────────────────────┘              └──────────────────────┘
                                             │
                                             ▼
                              ┌──────────────────────────┐
                              │   Glue Visual ETL Job    │
                              │ - Bookmark Enabled       │
                              │ - File-level ingestion   │
                              │ - Schema enforcement     │
                              │ - Data quality rules     │
                              └──────────────┬───────────┘
                                             │
                                             ▼
                              ┌──────────────────────────┐
                              │   S3 SILVER Layer        │
                              │  (Partitioned Parquet)   │
                              └──────────────┬───────────┘
                                             │
                                             ▼
                              ┌──────────────────────────┐
                              │   Glue Data Catalog      │
                              │  (Schema Controlled)     │
                              └──────────────┬───────────┘
                                             │
                                             ▼
                              ┌──────────────────────────┐
                              │        Athena            │
                              │   (Analytics / BI)       │
                              └──────────────────────────┘
```

- **Right now I have attached the lambda function that starts visual ETL pipeline is directly attached to the event notification of the source S3 bucket**
    - This is not a reliable way to trigger Lambda functions 
    - S3 events notifications are:
        - At least once delivery
        - Can produce duplicate events
        - Can occasionally miss events (rare, but possible)
        - No ordering guarantees
    - So if 
        - 10 files at once or the same file is retired 
    - You might
        - Trigger glue twice for the same file
        - Process duplicate data
        - Overwrite partitions incorrectly
    - For small hobby pipelines --> Fine 
    - For production ingestion --> Risky
    - If 500 files arrive in 1 minute then S3 will invoke lambda function 500 times and Lambda function will try to start 500 glue jobs , Now you hit:
        - Glue concurrency limit
        - API throttling
        - Random failures
        - Partial ingestion
    - No replay capability:
        - You cannot easily replay missed events 
        - S3 does not perists event history for you 
    - Tight coupling
        - Right now bucket is tightly coupled to 
            - One Lambda function
            - One Glue job
        - If tomorrow 
            - I want to send the same event to another system
            - Or introduce validation
            - Or introduce duplication
            - I must rewrite everything and that's a brittle architecture
- What production Data platform do instead 
    - You can either use EventBridge
    - ```bash
        S3 → EventBridge → SQS → Lambda → Glue
        ```
    - Or your can use SQS directly removing EventBridge
    - ```bash
        S3
        ↓
        SQS (queue)
        ↓
        Lambda (polls queue)
        ↓
        Glue
        ```
    - I went with the EventBridge method 
    - Pros of using this architecture
        - Each layer is independent.
        - If tomorrow you want 
            - Another Lambda 
            - A step function
            - A montioring pipeline 
            - A metadata tracker 
        - You just add another EventBridge rule.
        - You don't need to touch S3 
    - Using SQS as the buffer
        - Amazon SQS gives you:
            - Persistent message storage.
            - Visibility timeout
            - Controlled retries 
            - Dead-Letter queue
        - If lambda fails:
            - Message becomes visible again
            - Gets retried 
            - After N retries --> Moves to DLQ
    - Replay capability
        - If something breaks 
            - Bad glue transformation
            - Schema issue 
            - Partition error
        - You can:
            - Reprocess DLQ messages 
            - Re-drive message from SQS
            - Manually replay failed ingestion
        - This is critical in production data systems . Without a queue replay is painful.
    - Backpressure & Concurrency Control
        - If 500 files arrive:
            - With SQS
                - Message wait safely
                - Lambda scales gradually
                - You can control:
                    - Batch size 
                    - max concurrency
                    - parallelism
                - You just introduced flow control into your ingestion
    - Event Filtering at the Platform Layer
        - Because I used:
            - I can filter events before they even hit SQS:
                - Only .csv files
                - Only raw/sales/ prefix
                - Ignore temp files
                - Ignore test uploads
            - That reduces noise and cost.
    - Clean Failure Isolation
        - If:
            - Glue API throttles 
            - Lambda fails 
            - Network hiccup happens
        - The event is safe inside SQS
    - Horizontal scalability
        - Add muliple Lambda consumers
        - Scale out ingestion
        - Process different datasets in parallel
    - This architecture scales horizontally
    - Observability & Monitoring
        - Now you can monitor 
            - SQS queue depth 
            - DLQ message count 
            - Lambda error rate
            - Glue job failures
        - If queue depth spikes:
            - Ingestion bottleneck
        - If DLQ grows 
            - Transformation bug

### Implementation steps (Version 2)
#### Create a DLQ
- A Dead Letter Queue is a special queue where messages are sent after they fail processing multiple times. It’s a safety net for failed events.
- Storing events in case of failure is necessary so that we can replay the ingestion pipeline after fixing the errors. Hence making sure that data is not lost silently

**STEP 1:** 
- Give your SQS a name and enable encryption
![create_DLQ_1](images/production_grade_glue_implementation/sqs_dlq_implementation/create_DLQ_1.png)
![create_DLQ_2](images/production_grade_glue_implementation/sqs_dlq_implementation/create_DLQ_2.png)
![create_DLQ_3](images/production_grade_glue_implementation/sqs_dlq_implementation/create_DLQ_3.png)
- This is the access policy I used:
```json
{
  "Version": "2012-10-17",
  "Id": "__default_policy_ID",
  "Statement": [
    {
      "Sid": "__owner_statement",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::aws_account_id:root"
      },
      "Action": "SQS:*",
      "Resource": "arn:aws:sqs:ap-south-1:aws_account_id:sales-ingestion-dlq"
    },
    {
      "Sid": "AllowEventBridgeToSendMessage",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:ap-south-1:aws_account_id:sales-ingestion-dlq",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:aws:events:ap-south-1:aws_account_id:rule/ActivateLambdaFuncEventBridgeRules"
        }
      }
    },
    {
      "Sid": "AWSEvents_ActivateLambdaFuncEventBridgeRules_dlq_41c9320f-edd2-4fde-ad71-2985e68b1d7c",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:ap-south-1:aws_account_id:sales-ingestion-dlq",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:aws:events:ap-south-1:aws_account_id:rule/ActivateLambdaFuncEventBridgeRules"
        }
      }
    }
  ]
}
```
- Now in the event of failure the event messages won't be lost instead it will saved here in this SQS named ```sales-ingestion-dlq``` 
![failure_DLQ_message_polling](images/production_grade_glue_implementation/sqs_dlq_implementation/failure_DLQ_message_polling.png)

#### Configuring S3 bucket for EventBridge
- Here If you have any pre-configured Event notification in your S3 bucket then delete it We don't need it anymore. 
- Instead turn on the Amazon event bridge option, this is present just below the event notification option in the properties tab of the S3 bucket.
![set_s3_bucket_event_bridge](images/production_grade_glue_implementation/S3_bucket_configurations/set_s3_bucket_event_bridge.png)

#### Create an ETL pipeline

**STEP 1:**
- Create a role that has policies with appropriate permissions for AWS glue catalog so that it can perform its actions properly without any security risk
- Create a custom policy named ```LimitedS3PermissionPolicy```
```json
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::aws-glue-s3-bucket-one",
                    "arn:aws:s3:::aws-glue-s3-bucket-one/*",
                    "arn:aws:s3:::data-sink-one",
                    "arn:aws:s3:::data-sink-one/*"
                ]
            }
        ]
    }
```
- Create a Role named ```AWSGlueRole```
- Attach policies to this role 
    - attach ```LimitedS3PermissionPolicy``` custom policy
    - attach aws managed policy named ```AWSGlueServiceRole```

**STEP 2:**
- In this you will have to attach a role to your Glue visual ETL pipeline 
![create_ETL_1](images/production_grade_glue_implementation/ETL/create_ETL_1.png)
- Now you will have to set here 
    - Enable bookmarks to make sure only new files are processed and old files are ignored
    - Enable job queue
    - Enable job insights
![create_ETL_2](images/production_grade_glue_implementation/ETL/create_ETL_2.png)


**STEP 3:**
- This is the code for the ETL pipeline 
- This ETL pipeline ingest data in form of csv file from a source S3 bucket
- It removes the rows that has all the values in its columns set as null
- It then transforms the data in a column into its appropriate datatype. The reason I had to do this is because in csv file all the columns wheather it has data in numbers are of string datatype.
- It then register the metadata in the AWS glue catalog after the actual data is stored in parquet files in the target S3. It is to make sure that the data can be queried from those output parquet files using Athena.
- ```python
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'source_bucket',
        'source_key'
    ])

    source_bucket = args['source_bucket']
    source_key = args['source_key']
    ```
    - This code accepts the arguments related to bucket and the recently PUT object key that is passed on to AWS glue ETL pipeline by Lambda function that has invoked it when the PUT event happened
- ```python
    connection_options={"paths": [f"s3://{source_bucket}/{source_key}"],"recurse": False},
    ```
    - This line is very important when it comes to optimization 
    - In the earlier version my ETL pipeline was scanning the whole folder everytime it was invoked by lambda function but now it only scans the file that has arrived in the S3 hence saving precious compute. As a result we are able to save cost.
    - If three files are uploaded to S3 then for each file three events will be generated then those three events will actiavte three lambda functions which will be invoking three separate glue visual ETL pipelines. These pipelines will then execute one by one 
- Below is the complete ETL pipeline python code 
- ```python
    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.gluetypes import *
    from awsgluedq.transforms import EvaluateDataQuality
    from awsglue import DynamicFrame

    def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
        for alias, frame in mapping.items():
            frame.toDF().createOrReplaceTempView(alias)
        result = spark.sql(query)
        return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
    def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
        if isinstance(schema, StructType):
            for field in schema:
                new_path = path + "." if path != "" else path
                output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
        elif isinstance(schema, ArrayType):
            if isinstance(schema.elementType, StructType):
                output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
        elif isinstance(schema, NullType):
            output.append(path)
        else:
            x, distinct_set = frame.toDF(), set()
            for i in x.select(path).distinct().collect():
                distinct_ = i[path.split('.')[-1]]
                if isinstance(distinct_, list):
                    distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
                elif isinstance(distinct_, str) :
                    distinct_set.add(distinct_.strip())
                else:
                    distinct_set.add(distinct_)
            if isinstance(schema, StringType):
                if distinct_set.issubset(nullStringSet):
                    output.append(path)
            elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
                if distinct_set.issubset(nullIntegerSet):
                    output.append(path)
        return output

    def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
        nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
        return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'source_bucket',
        'source_key'
    ])

    source_bucket = args['source_bucket']
    source_key = args['source_key']

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Default ruleset used by all target nodes with data quality enabled
    DEFAULT_DATA_QUALITY_RULESET = """
        Rules = [
            ColumnCount > 0
        ]
    """

    # Script generated for node Raw data source S3
    RawdatasourceS3_node1772431168408 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", 
        # connection_options={"paths": ["s3://aws-glue-s3-bucket-one/raw_data/sales_data/"], "recurse": True}, 
        connection_options={"paths": [f"s3://{source_bucket}/{source_key}"],"recurse": False},
        transformation_ctx="RawdatasourceS3_node1772431168408")
    # for debugging only
    print(f"Glue Visual ETL | Processing file: s3://{source_bucket}/{source_key}")

    # Script generated for node SQL Query
    SqlQuery61 = '''
    SELECT
        product_id,
        product_name,
        category,
        about_product,
        user_id,
        user_name,
        review_id,
        review_title,
        review_content,
        img_link,
        product_link,

        -- discounted_price: ₹149 → 149.0
        CAST(
            REGEXP_REPLACE(discounted_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS discounted_price,

        -- actual_price: ₹1,000 → 1000.0
        CAST(
            REGEXP_REPLACE(actual_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS actual_price,

        -- discount_percentage: 85% → 85.0
        CAST(
            REGEXP_REPLACE(discount_percentage, '[^0-9.]', '')
            AS DOUBLE
        ) AS discount_percentage,

        -- rating: 3.9 → 3.9
        CAST(
            REGEXP_REPLACE(rating, '[^0-9.]', '')
            AS DOUBLE
        ) AS rating,

        -- rating_count: 24,871 → 24871
        CAST(
            REGEXP_REPLACE(rating_count, '[^0-9]', '')
            AS INT
        ) AS rating_count

    FROM myDataSource;
    '''
    SQLQuery_node1772431252856 = sparkSqlQuery(glueContext, query = SqlQuery61, mapping = {"myDataSource":RawdatasourceS3_node1772431168408}, transformation_ctx = "SQLQuery_node1772431252856")

    # Script generated for node Drop Null Fields
    DropNullFields_node1772431857999 = drop_nulls(glueContext, frame=SQLQuery_node1772431252856, nullStringSet={"", "null"}, nullIntegerSet={-1}, transformation_ctx="DropNullFields_node1772431857999")

    # Script generated for node Silver layer data sink S3
    EvaluateDataQuality().process_rows(frame=DropNullFields_node1772431857999, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772428328053", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
    SilverlayerdatasinkS3_node1772432020219 = glueContext.getSink(path="s3://data-sink-one/silver_layer/sales_data/", connection_type="s3", updateBehavior="LOG", partitionKeys=["category"], enableUpdateCatalog=True, transformation_ctx="SilverlayerdatasinkS3_node1772432020219")
    SilverlayerdatasinkS3_node1772432020219.setCatalogInfo(catalogDatabase="aws-glue-tutorial-aditya",catalogTableName="silver_table_sales_data")
    SilverlayerdatasinkS3_node1772432020219.setFormat("glueparquet", compression="snappy")
    SilverlayerdatasinkS3_node1772432020219.writeFrame(DropNullFields_node1772431857999)
    job.commit()
    ```

#### Create a Lambda function 
- This Lambda function will trigger the Visual ETL pipeline

**STEP 1:**
- Create a role that gives appropirate access rights of the services to make sure that the lambda function works as intended.
- Create a custom policy named ```AWSGlueStartJobAccessPolicy```
    - Use this policy json 
    - ```json
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": "glue:StartJobRun",
                        "Resource": "arn:aws:glue:ap-south-1:406868976171:job/ingest_sales_data"
                    }
                ]
            }
        ```

- Create a role named ```TriggerAWSGlueVisualETLPipeline```
    - Attach this aws managed policy to it ```AWSLambdaBasicExecutionRole```
    - Attach this ```AWSGlueStartJobAccessPolicy``` custom policy that you made earlier to it 

**STEP 2:**
- Write the code 
- ```python
    # VERSION 2 : Works with Event Bridge and is Production READY
    import json
    import boto3
    import os

    glue_client = boto3.client("glue")

    GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]

    def lambda_handler(event, context):
        try:
            # event recieved from event bridge
            print(f"Lambda function | Event recieved from event bridge | {event}") 
            # Extract bucket and key from EventBridge event
            bucket = event["detail"]["bucket"]["name"]
            key = event["detail"]["object"]["key"]

            print(f"Lambda function | Received new object: | s3://{bucket}/{key}")

            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME,
                Arguments={
                    "--source_bucket": bucket,
                    "--source_key": key
                }
            )

            print(f"Lambda function | Started Glue job | {response['JobRunId']}")

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Glue job started successfully",
                    "jobRunId": response["JobRunId"]
                })
            }

        except Exception as e:
            print(f"Error: {str(e)}")
            raise e    except Exception as e:
            print(f"Error: {str(e)}")
            raise e
    ```

- Why this is a better code than the previous version?
    - Previous code 
        - ```python
            response = glue_client.start_job_run(
                JobName=GLUE_JOB_NAME
            )
            ```
        - It was only taking GLUE_JOB_NAME as an input argument 
        - Hence the lambda function have no idea which file triggered it.
        - In the previous version Glue used to scan for folders (Glue was setup to be a folder-based job)
        - Cannot isolate failures
        - Not idempotent
        - Hard to debug
        - Wastes compute
        - What the previous code was doing "Something happened in S3 run glue visual ETL blindly"
        - That was not event based ingestion that was reactive batch processing.
    - New version
        - ```python
                bucket = event["detail"]["bucket"]["name"]
                key = event["detail"]["object"]["key"]

                print(f"Received new object: s3://{bucket}/{key}")

                response = glue_client.start_job_run(
                    JobName=GLUE_JOB_NAME,
                    Arguments={
                        "--source_bucket": bucket,
                        "--source_key": key
                    }
                )
            ```

        - The event argument is the one recieving the event related data from the event bridge 
            - This data contains the source_bucket and object_key.
            - The lambda function then passes this data to AWS glue visual ETL pipeline so that it can use this information to only scan for the file that has arrived in the S3 bucket instead of scanning the whole folder, hence saving DBU compute and ultimately saving cost.  
        - This code allows me to implement File level ingestion logic when developing glue visual ETL pipeline.
        - It Becomes Truly Event-Driven
        - Instead of 
            - ```bash
                    File arrives → Run Glue
                ```
        - This version of code does this
            - ```bash
                    File arrives → Extract exact object key → Pass to Glue
                ```
        - Glue will now be able to process 
            - ```bash
                    s3://bucket/specific_file.csv
                ```
        - This eleminates:
            - Full folder scans 
            - Reprocessing old files
            - Hidden duplicates
        - Perfect Failure Isolation
            - ```bash
                    file_1.csv → corrupt
                    file_2.csv → good
                ```
            - With Version 1:
                - Entire folder ingestion may fail.
            - With version 2:
                - Only ```file_1.csv``` fails.
                - ```file_2.csv``` processes normally.
        - Enables DLQ + Replay
            - Since I am extracting 
                - ```bash
                        bucket
                        key
                    ```
            - That information is preserved in Amazon SQS (DLQ)
                - If Glue fails:
                    - The exact file path remains inside the message.
                    - You can replay exactly that file.
                - Version 1 cannot replay specific files because it doesn't know which file caused failure.
        - Cost Efficiency
            - Version 1 causes:
                - Folder scans
                - Reading multiple files
                - Unnecessary spark overhead
            - Version 2:
                - Reads exactly one file
                - Lower DPU usage
                - Faster execution
                - Better cost control
        - Debuggability:
            - Version 2 prints:
                - ```python
                        print(f"Received new object: s3://{bucket}/{key}")
                    ```
                - Now your cloud watch logs show:
                - ```bash
                        Received new object: s3://raw/sales/file123.csv
                    ```
            - This allows us to immediately know
                - Which file triggered
                - Which file failed
                - Which file succeeded
        - Enables Idempotency
            - Before starting Glue:
                - Check DynamoDB
                - Check metadata store
                - Check processed files table
        - Horizontal Scalability
            - Version 2:
                - 100 Lambda invocations
                - 100 independent glue runs
                - Fully parallel
                - Isolated failures
            - Version 1:
                - Possibly 1 glue run scanning entire folder
                - Bottleneck risk
                - Partial processing
        - Architecturally Clean
            - Version 1 = "Batch job trigger"
            - Version 2 = "Event payload processor"

**STEP 3:**
- Attach the role that you created for this lambda function here in this page.
- Attaching role with appropriate permission is necessary for security purposes.
- We want this lambda function to access services in AWS securely and give minimum possible permission for it to be able to function properly.
![lambda_function_attach_role](images/production_grade_glue_implementation/lambda_setup/lambda_function_attach_role.png)
![add_role_to_lambda](images/production_grade_glue_implementation/lambda_setup/add_role_to_lambda.png)

**STEP 4:**
- Add environment variables that is required by this lambda function here 
![environment_variable_lambdafunction](images/production_grade_glue_implementation/lambda_setup/environment_variable_lambdafunction.png)

**NOTE :**
- If you have set everything correctly then you will see the logs in AWS cloudwatch for your lambda function
![cloudwatch_logs_for_lambda](images/production_grade_glue_implementation/lambda_setup/cloudwatch_logs_for_lambda.png)
- Here all your lambda function logs will be dumped
![cloudwatch_logs_for_lambda2](images/production_grade_glue_implementation/lambda_setup/cloudwatch_logs_for_lambda2.png)


#### Create an EventBridge
**STEP 1:**
- Setup trigger events
![create_event_bridge_1](images/production_grade_glue_implementation/event_bridge_setup/create_event_bridge_1.png)
- Select the S3 simple storage service for trigger event 
- Once thats done now use this event pattern filter 
    - ```json
            {
            "source": ["aws.s3"],
            "detail-type": ["Object Created"],
            "detail": {
                "bucket": {
                "name": ["aws-glue-s3-bucket-one"]
                },
                "object": {
                "key": [{
                    "prefix": "raw_data/sales_data/"
                }]
                }
            }
            }
        ```
    - Here ```"raw_data/sales_data/"``` is the directory inside this S3 where your files are arriving.
- ![create_event_bridge_2](images/production_grade_glue_implementation/event_bridge_setup/create_event_bridge_2.png)

**STEP 2:**
- When configuring the targets for the EventBridge make sure that you have disabled the input transformation configuration
- What this does ? --> Use Target input transformer to customize the text from an event before EventBridge passes the information to the target. When target input transformer is not defined, the original event will be sent to a target.
- We don't need it here
![create_event_bridge_3](images/production_grade_glue_implementation/event_bridge_setup/create_event_bridge_3.png)

**STEP 3:**
- Select target this account since our Lambda function is present in this account only
![create_event_bridge_4](images/production_grade_glue_implementation/event_bridge_setup/create_event_bridge_4.png)
- Here make sure that refrain from using your own custom role for your EventBridge instead let AWS create a new Role for your EventBridge on your behalf
![create_event_bridge_5](images/production_grade_glue_implementation/event_bridge_setup/create_event_bridge_5.png)
- Setup retry policy for your lambda function that triggers visual ETL when this file arrival PUT event happens in S3 bucket
- Setup the age of events in hours 
    - This will set the number of hours an unprocessed event can be kept
- Here attach your SQS which will serve as DLQ to store messages in case of a failure after the fix is completed the stored messages will be re-processed.
    - This prevent data loss in case of a failure.

![create_event_bridge_6](images/production_grade_glue_implementation/event_bridge_setup/create_event_bridge_6.png)

#### Room for further improvements in this 
Current setup works because:
- I enabled Glue Job Bookmarks
- I trigger Glue per file
- I are not doing complex orchestration
- I are not handling replay or partial failure logic

Right now my incremental guarantee depends entirely on:
- Glue bookmarks

That works for:
- Simple pipelines
- Single-account setups
- No replay requirements
- No external state management
- No manual backfills

But bookmarks are:
- Internal to Glue
- Opaque
- Not easily inspectable
- Hard to coordinate across multiple services

Current implementation have the infrastructure for replayability, but I have NOT fully implemented replayability yet.
- GAP 1 — Current system Don’t Track Processing State (BIGGEST ISSUE)
    - REQUIRED FIX → Add Idempotency Store (DynamoDB)
    - This is mandatory for replayability.
- GAP 2 — Current System Doesn't Detect Glue Failures Properly
    - Step Functions (BEST PRACTICE)
        - waits for completion
        - retries
        - catches failure
        - triggers DLQ
- GAP 3 — Current system Don’t Have Replay Workflow Yet
    - Right now DLQ exists but:
    - user replay manually (not production-grade).

## Improvement implementation on current system (Version 3)
- In this version I am going to implement Step function and replace the lambda function that I was using earlier to trigger the ETL pipeline.
- Reasons : 
    - Since (Version 2) implementation of this ETL pipeline where I was using lambda function instead of a step function I was not able to reliably detect if the ETL pipeline failed and hence was not able to send the event messages sent by S3 to DLQ reliably in case of a failure.
### Implementation 
#### Step 1 : Source S3 bucket setup
- Enable events to be sent to EventBridge in your S3 bucket 
- ![enable_event_bridge](images/production_grade_glue_version2_dlq_/S3_settings/enable_event_bridge.png)

#### Step 2 : EventBridge setup 
- The name of the event bridge is ```ActivateLambdaFuncEventBridgeRules```
- Setup Trigger events for your EventBridge
- ![setting_up_trigger_event](images/production_grade_glue_version2_dlq_/EventBridge/setting_up_trigger_event.png)

- Event pattern (filter) that I set when setting up the Event Bridge
```json
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["aws-glue-s3-bucket-one"]
    },
    "object": {
      "key": [{
        "prefix": "raw_data/sales_data/"
      }]
    }
  }
}
```

- Setting up the targets that must be invoked if file is put inside the source S3 bucket
- ![step_function_target_setup](images/production_grade_glue_version2_dlq_/EventBridge/step_function_target_setup.png)
- One thing to note is that event if the image shows that I have used an existing role you must always select ```create a new role for this specific resource``` 
- The reason the image shows the other option is because I have taken a screenshot of already existing EventBridge that's why.
- You don't need to create role for this service manually AWS automatically creates a role with appropriate permissions for event bridge to use.
- ![retry_policy_of_event_bridge](images/production_grade_glue_version2_dlq_/EventBridge/retry_policy_of_event_bridge.png)

#### Step 3 : Setup a DLQ (Dead-Letter-Queue)
- The name of this DLQ is ```sales-ingestion-dlq```
- ![dlq_conf](images/production_grade_glue_version2_dlq_/sqs/dlq_conf.png)
- ![encryption_dlq](images/production_grade_glue_version2_dlq_/sqs/encryption_dlq.png)
- Access policy of DLQ
```json
{
  "Version": "2012-10-17",
  "Id": "__default_policy_ID",
  "Statement": [
    {
      "Sid": "__owner_statement",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::406868912345:root"
      },
      "Action": "SQS:*",
      "Resource": "arn:aws:sqs:ap-south-1:406868912345:sales-ingestion-dlq"
    },
    {
      "Sid": "AllowEventBridgeToSendMessage",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:ap-south-1:406868971234:sales-ingestion-dlq",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:aws:events:ap-south-1:406868971234:rule/ActivateLambdaFuncEventBridgeRules"
        }
      }
    },
    {
      "Sid": "AWSEvents_ActivateLambdaFuncEventBridgeRules_dlq_41c9320f-edd2-4fde-ad71-2985e68b1d7c",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sqs:SendMessage",
      "Resource": "arn:aws:sqs:ap-south-1:406868971234:sales-ingestion-dlq",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "arn:aws:events:ap-south-1:406868912345:rule/ActivateLambdaFuncEventBridgeRules"
        }
      }
    }
  ]
}
```
- ![redrive_policy_dlq](images/production_grade_glue_version2_dlq_/sqs/redrive_policy_dlq.png)

#### Step 4 : Create a step function
- This step function will be used to not only start a Glue function but it will also be used to monitor if the glue job was a sucess or a failure 
```bash
RunGlueJob → Success
          ↘ SendToDLQ → FailState
```
- How this state function works ?
    - When a file lands in S3
    - EventBridge triggers this Step function
    - Step function runs glue job
    - If the glue job fails then the event is send to DLQ

```json
{
  "Comment": "Glue ETL Orchestration with DLQ",
  "StartAt": "RunGlueJob",
  "States": {
    "RunGlueJob": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "ingest_sales_data",
        "Arguments": {
          "--source_bucket.$": "$.detail.bucket.name",
          "--source_key.$": "$.detail.object.key"
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 30,
          "MaxAttempts": 2,
          "BackoffRate": 2
        }
      ],
      "Catch": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "Next": "SendToDLQ"
        }
      ],
      "Next": "Success"
    },
    "SendToDLQ": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sqs:sendMessage",
      "Parameters": {
        "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868971234/sales-ingestion-dlq",
        "MessageBody.$": "$"
      },
      "Next": "FailState"
    },
    "FailState": {
      "Type": "Fail"
    },
    "Success": {
      "Type": "Succeed"
    }
  }
}
```

- ```"arn:aws:states:::glue:startJobRun.sync"```
    - Start Glue job
    - WAIT until it finishes
- If ```.sync``` wasn’t used: Step Function would move immediately.

```json
"Arguments": {
          "--source_bucket.$": "$.detail.bucket.name",
          "--source_key.$": "$.detail.object.key"
        }
```
- Here the step function is extracting the bucket name and object key from the event bridge and then passing it on to AWS glue ETL pipeline where we will be able to access it using ```source_bucket``` and ```source_key``` params.
- In Glue ETL ```getResolvedOptions(sys.argv, ...)``` this code will catch all the params passed on by this step function

```json
"Retry": [{
  "ErrorEquals": ["States.TaskFailed"],
  "IntervalSeconds": 30,
  "MaxAttempts": 2,
  "BackoffRate": 2
}]
```
- Here I have decided to put the retry logic in the step function instead of eventBridge because it does makes more sense since step function is not only the one startiing the glue job but it is also the one waiting if the job was a success or a failure.
- Why this is done ?
    - transient cluster issues
    - network blips 
    - service throttling

```json
"Catch": [{
  "ErrorEquals": ["States.ALL"],
  "Next": "SendToDLQ"
}]
```
Meaning:
- If STILL fails after retries
- Send to DLQ

- You will have to add an AWS managed policy ```AmazonSQSFullAccess``` to the generated IAM role when creating a Step function.

**NOTICE :**

**There was an issue related to where when ETL fails the step function was not sending the message to DLQ**
- This happened only when the ETL recieved a corrupt csv file.
- This happened because of the code below
```json
"Retry": [{
  "ErrorEquals": ["States.TaskFailed"],
  "IntervalSeconds": 30,
  "MaxAttempts": 2,
  "BackoffRate": 2
}]
```
- ```"ErrorEquals": ["States.TaskFailed"],``` Glue ETL can return not only TaskFailed but other types of failure states as well
- solution is to use ```"States.ALL"``` instead of ```"States.TaskFailed"``` take a look at the code below and replace the above one with this
```json
"Retry": [
        {
          "ErrorEquals": [
            "States.ALL"
          ],
          "IntervalSeconds": 30,
          "MaxAttempts": 2,
          "BackoffRate": 2
        }
      ],
```

#### Step 5 : Create another step function to handle manual replays in case of a failure
- The name of this step function is ```replay_failed_ingestion```
- This is the second step function : ```DLQ → StepFn → Glue (retry) → Success```
- It replays job safely
    - This step function reads the message from DLQ ```sales-ingestion-dlq```
    - Extracts original payload
    - Re-runs Glue
    - Deletes message if success
- ```"MaxNumberOfMessages": 1```
    - Pulls only one message per execution this prevents overload + ensures a controlled replay.
- ```"Next": "FailState"```
    - If replay still fails then stop here
    - In this case DLQ message will not be deleted
- ```"Resource": "arn:aws:states:::aws-sdk:sqs:deleteMessage"```
    - Deletes message only if: 
        - Replay is successfull
- Here is the full code for this step function
```json
{
  "Comment": "Replay ETL from DLQ with batching, tracking, and safeguards",
  "StartAt": "ReceiveFromDLQ",
  "States": {
    "ReceiveFromDLQ": {
      "Type": "Task",
      "Resource": "arn:aws:states:::aws-sdk:sqs:receiveMessage",
      "Parameters": {
        "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
        "MaxNumberOfMessages": 10
      },
      "ResultPath": "$.dlq",
      "Next": "CheckIfEmpty"
    },
    "CheckIfEmpty": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.dlq.Messages",
          "IsPresent": false,
          "Next": "Success"
        }
      ],
      "Default": "ReplayBatch"
    },
    "ReplayBatch": {
      "Type": "Map",
      "ItemsPath": "$.dlq.Messages",
      "MaxConcurrency": 2,
      "Iterator": {
        "StartAt": "ExtractPayload",
        "States": {
          "ExtractPayload": {
            "Type": "Pass",
            "Parameters": {
              "receiptHandle.$": "$.ReceiptHandle",
              "body.$": "States.StringToJson($.Body)"
            },
            "ResultPath": "$.parsed",
            "Next": "ParseCause"
          },
          "ParseCause": {
            "Type": "Pass",
            "Parameters": {
              "receiptHandle.$": "$.parsed.receiptHandle",
              "cause.$": "States.StringToJson($.parsed.body.Cause)"
            },
            "ResultPath": "$.final",
            "Next": "CheckReplayCount"
          },
          "CheckReplayCount": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:dynamodb:getItem",
            "Parameters": {
              "TableName": "dlq_replay_tracking",
              "Key": {
                "object_key": {
                  "S.$": "$.final.cause.Arguments['--source_key']"
                }
              }
            },
            "ResultPath": "$.ddb",
            "Next": "ReplayLimitCheck"
          },
          "ReplayLimitCheck": {
            "Type": "Choice",
            "Choices": [
              {
                "Variable": "$.ddb.Item.retry_count.N",
                "NumericGreaterThanEquals": 3,
                "Next": "SkipPoisonMessage"
              }
            ],
            "Default": "RunGlueJob"
          },
          "RunGlueJob": {
            "Type": "Task",
            "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {
              "JobName": "ingest_sales_data",
              "Arguments": {
                "--source_bucket.$": "$.final.cause.Arguments['--source_bucket']",
                "--source_key.$": "$.final.cause.Arguments['--source_key']"
              }
            },
            "Retry": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "IntervalSeconds": 30,
                "MaxAttempts": 2,
                "BackoffRate": 2
              }
            ],
            "Catch": [
              {
                "ErrorEquals": [
                  "States.ALL"
                ],
                "Next": "UpdateRetryCount"
              }
            ],
            "Next": "DeleteMessage"
          },
          "UpdateRetryCount": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:dynamodb:updateItem",
            "Parameters": {
              "TableName": "dlq_replay_tracking",
              "Key": {
                "object_key": {
                  "S.$": "$.final.cause.Arguments['--source_key']"
                }
              },
              "UpdateExpression": "ADD retry_count :inc",
              "ExpressionAttributeValues": {
                ":inc": {
                  "N": "1"
                }
              }
            },
            "Next": "FailState"
          },
          "DeleteMessage": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:sqs:deleteMessage",
            "Parameters": {
              "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
              "ReceiptHandle.$": "$.final.receiptHandle"
            },
            "Next": "SuccessState"
          },
          "SkipPoisonMessage": {
            "Type": "Pass",
            "Next": "SuccessState"
          },
          "FailState": {
            "Type": "Fail"
          },
          "SuccessState": {
            "Type": "Succeed"
          }
        }
      },
      "Next": "Success"
    },
    "Success": {
      "Type": "Succeed"
    }
  }
}
```

#### Step 6 : Create a glue ETL job
- Before you create this ELT you will have to create an IAM role that give appropriate permission to this ETL for it to be able to utilize the S3 and Glue catalog so that it can register the data in the table so that the users can use Athena to query the data using sql.
- The IAM role that is used by this ETL job is named ```AWSGlueRole```
    - In this role you will have to attach an AWS managed policy called ```AWSGlueServiceRole```
    - Then one more custom policy you will have to attach is a custom policy named ```LimitedS3PermissionPolicy```
    ```json
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::aws-glue-s3-bucket-one",
                    "arn:aws:s3:::aws-glue-s3-bucket-one/*",
                    "arn:aws:s3:::data-sink-one",
                    "arn:aws:s3:::data-sink-one/*"
                ]
            }
        ]
    }
    ```
- Here is the complete code
```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

from functools import reduce
from pyspark.sql import functions as F

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
    if isinstance(schema, StructType):
        for field in schema:
            new_path = path + "." if path != "" else path
            output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, ArrayType):
        if isinstance(schema.elementType, StructType):
            output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
    elif isinstance(schema, NullType):
        output.append(path)
    else:
        x, distinct_set = frame.toDF(), set()
        for i in x.select(path).distinct().collect():
            distinct_ = i[path.split('.')[-1]]
            if isinstance(distinct_, list):
                distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
            elif isinstance(distinct_, str) :
                distinct_set.add(distinct_.strip())
            else:
                distinct_set.add(distinct_)
        if isinstance(schema, StringType):
            if distinct_set.issubset(nullStringSet):
                output.append(path)
        elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
            if distinct_set.issubset(nullIntegerSet):
                output.append(path)
    return output

def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
    nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
    return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'source_key'
])

source_bucket = args['source_bucket']
source_key = args['source_key']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """Rules = [
    ColumnCount > 0,

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
]"""

# -------- DELIMITER CORRUPTION CHECK -------- #
import csv
import io

raw_lines = spark.read.text(f"s3://{source_bucket}/{source_key}").collect()

if len(raw_lines) < 2:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (file has no data rows)")

def count_csv_columns(line: str) -> int:
    """Count columns correctly, respecting quoted fields containing commas."""
    try:
        return len(next(csv.reader(io.StringIO(line))))
    except StopIteration:
        return 0

header_line = raw_lines[0][0]
expected_col_count = count_csv_columns(header_line)

bad_row_indices = []
for i, row in enumerate(raw_lines[1:], start=2):
    line = row[0]
    actual_col_count = count_csv_columns(line)
    if actual_col_count != expected_col_count:
        bad_row_indices.append((i, actual_col_count, line[:80]))

if bad_row_indices:
    details = "\n".join(
        [f"  Line {idx}: expected {expected_col_count} cols, got {actual} → {preview}..."
         for idx, actual, preview in bad_row_indices]
    )
    raise Exception(
        f"Glue Visual ETL | Corrupted CSV detected — {len(bad_row_indices)} row(s) have wrong column count "
        f"(likely semicolons used as delimiters instead of commas):\n{details}"
    )

print(f"Glue Visual ETL | Column count validation passed: all rows have {expected_col_count} columns")
# -------- DELIMITER CORRUPTION CHECK -------- #

# Script generated for node Raw data source S3
RawdatasourceS3_node1772431168408 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "mode": "PERMISSIVE", "optimizePerformance": False}, connection_type="s3", format="csv", 
    # connection_options={"paths": ["s3://aws-glue-s3-bucket-one/raw_data/sales_data/"], "recurse": True}, 
    connection_options={"paths": [f"s3://{source_bucket}/{source_key}"],"recurse": False},
    transformation_ctx="RawdatasourceS3_node1772431168408")
# for debugging only
print(f"Glue Visual ETL | Processing file: s3://{source_bucket}/{source_key}")

df = RawdatasourceS3_node1772431168408.toDF()

print("Glue Visual ETL | DEBUG bucket:", source_bucket)
print("Glue Visual ETL | DEBUG key:", source_key)
print("Glue Visual ETL | DEBUG columns:", df.columns)
print("Glue Visual ETL | DEBUG row count:", df.count())
print("Glue Visual ETL | DEBUG schema:", df.schema)
print("Glue Visual ETL | DEBUG sample rows:", df.limit(2).toPandas())

# -------- CORRUPTION CHECK (SAFE VERSION) -------- #

# If Spark inferred no columns → corrupted file
if len(df.columns) == 0:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (no columns inferred)")

expected_cols = len(df.columns)

# If dataframe empty → corrupted file
if df.limit(1).count() == 0:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (empty dataframe)")

# Row-level corruption detection
null_exprs = [F.col(c).isNull().cast("int") for c in df.columns]

bad_rows = df.filter(
    reduce(lambda a, b: a + b, null_exprs) > expected_cols * 0.7
)

if bad_rows.limit(1).count() > 0:
    raise Exception("Glue Visual ETL | Corrupted CSV detected (null-heavy rows)")
# -------- CORRUPTION CHECK (SAFE VERSION) -------- #

# Script generated for node SQL Query
SqlQuery61 = '''
SELECT
    product_id,
    product_name,
    category,
    about_product,
    user_id,
    user_name,
    review_id,
    review_title,
    review_content,
    img_link,
    product_link,

    -- discounted_price: ₹149 → 149.0
    CAST(
        REGEXP_REPLACE(discounted_price, '[^0-9.]', '')
        AS DOUBLE
    ) AS discounted_price,

    -- actual_price: ₹1,000 → 1000.0
    CAST(
        REGEXP_REPLACE(actual_price, '[^0-9.]', '')
        AS DOUBLE
    ) AS actual_price,

    -- discount_percentage: 85% → 85.0
    CAST(
        REGEXP_REPLACE(discount_percentage, '[^0-9.]', '')
        AS DOUBLE
    ) AS discount_percentage,

    -- rating: 3.9 → 3.9
    CAST(
        REGEXP_REPLACE(rating, '[^0-9.]', '')
        AS DOUBLE
    ) AS rating,

    -- rating_count: 24,871 → 24871
    CAST(
        REGEXP_REPLACE(rating_count, '[^0-9]', '')
        AS INT
    ) AS rating_count

FROM myDataSource;
'''
SQLQuery_node1772431252856 = sparkSqlQuery(glueContext, query = SqlQuery61, mapping = {"myDataSource":RawdatasourceS3_node1772431168408}, transformation_ctx = "SQLQuery_node1772431252856")

# Script generated for node Drop Null Fields
DropNullFields_node1772431857999 = drop_nulls(glueContext, frame=SQLQuery_node1772431252856, nullStringSet={"", "null"}, nullIntegerSet={-1}, transformation_ctx="DropNullFields_node1772431857999")

# Script generated for node Silver layer data sink S3
EvaluateDataQuality().process_rows(frame=DropNullFields_node1772431857999, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772428328053", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
SilverlayerdatasinkS3_node1772432020219 = glueContext.getSink(path="s3://data-sink-one/silver_layer/sales_data/", connection_type="s3", updateBehavior="LOG", partitionKeys=["category"], enableUpdateCatalog=True, transformation_ctx="SilverlayerdatasinkS3_node1772432020219")
SilverlayerdatasinkS3_node1772432020219.setCatalogInfo(catalogDatabase="aws-glue-tutorial-aditya",catalogTableName="silver_table_sales_data")
SilverlayerdatasinkS3_node1772432020219.setFormat("glueparquet", compression="snappy")
SilverlayerdatasinkS3_node1772432020219.writeFrame(DropNullFields_node1772431857999)
job.commit()
```

### CSV CORRUPTION CHECK (DEEP EXPLANATION)
- ```spark.read.text(...).collect()```
    - Reads raw file line-by-line
    - Returns list to driver
- Why check < 2 rows?
    - ```if len(raw_lines) < 2:```
    - Means:
        - 1 row → only header
        - 0 row → empty file
    - Hence invalid dataset
- ```count_csv_columns()```
    - ```csv.reader(io.StringIO(line))```
    - Why?
        - Handles quoted commas correctly
    - Example : ```"a,b",c```
    - correctly counted as 2 columns.
- Row validation logic
    - ```for i, row in enumerate(raw_lines[1:], start=2):```
    - Starts from row 2 because: row1= header
    - Then : ```actual_col_count != expected_col_count```
    - If mismatch then corrupted row
- Error reporting
    - ```line[:80]```
    - Show preview only (Safe logging)
- Final check
    - ```if bad_row_indices:```
    - If any corrupted now --> fail job

### HOW EVENTBRIDGE + STEP FUNCTION WORKS
#### Step 1 : File arrives
- EventBridge is enabled on bucket
- S3 sends events to EventBridge automatically.

#### Step 2 : EventBridge rule triggers
Event pattern (Filter)

```json
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": {
      "name": ["aws-glue-s3-bucket-one"]
    },
    "object": {
      "key": [{
        "prefix": "raw_data/sales_data/"
      }]
    }
  }
}
```

Matches:
- PutObject
- Multipart upload complete

The code below is the rules defined in pyspark script in AWS glue catalog 
```python
# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """Rules = [
    ColumnCount > 0,

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
]"""
```
- This is an AWS Glue data quality ruleset.
- This tells glue 
    - Check if my dataset is structurally valid before writing.
- What it does ? 
    - runs all rules
    - measures pass/fail
    - logs results
    - publishes metrics
    - (optionally) fails job
- ```ColumnCount = 16,```
    - In my case the csv files had 16 columns so I have made sure that the csv that is about to be processed must have 16 columns exactly.
    - This prevents data from silent schema drift in case the upstream service/application removes a column in the csv files.
    - Cannot allow schema drift at all cost because it will break all the down stream applications.
- ```python
        ColumnValues "rating" >= 0,
        ColumnValues "rating_count" >= 0,
        ColumnValues "discounted_price" >= 0,
        ColumnValues "actual_price" >= 0,
        ColumnValues "discount_percentage" >= 0,
    ```
    - It makes sure that ```rating, rating_count, discounted_price, actual_price, discount_percentage``` columns does not have any negative data values in them.
- ```IsUnique "product_id"```
    - This makes sure that the product_id is unique and prevents data duplication
- I have set AWS glue ETL pipeline to have maximum concurrency of 10 
![setting_up_max_concurrency](images/production_grade_glue_version2_dlq_/glue_ETL/setting_up_max_concurrency.png)

- **Issue I faced :**
    - Data quality related validation issue
        - I accidently used this rule ```ColumnCount == 16,``` instead of this ```ColumnCount = 16,``` 
        - Because of this my AWS glue ETL pipeline was failing which is to be expected 
        - **The bigger concern for me was this that it was not sending message to DLQ as it should.**
        - Reason for this happening:
            - The step function checks for the final job state and not the logs that is the reason the flow in the step function never reaches to the point where it sends the message to DLQ hence no message is found in the DLQ even when the ETL job fails.
        - Solution 
            - All you have to do is wrap the code where you are evaluating the data quality rules in try except. This should send message to DLQ in case the ETL pipeline fails.
            - In order to test it I am deliberately keeping the wrong rule in the section where I have defined all of my data quality rules ```ColumnCount == 16,```. The correct code looke like this ```ColumnCount == 16,```.

#### Step 3 : Filter applied
The filter set in the Event pattern 
```bash
"prefix": "raw_data/sales_data/"
```

Only those files trigger the Step function.

#### Step 4 : Step Function starts
EventBridge sends full event payload:
```bash
detail.bucket.name
detail.object.key
```

#### Step 5 : Step Function → Glue
Passes params via:
```bash
--source_bucket
--source_key
```

#### Architecture 
Current : ```S3 → EventBridge → StepFunction → Glue → Silver S3```

### Problems with this architecture 
#### max concurrency error thrown by the ETL pipeline the moment I try to ingest more than 1 file 
- This problem happens when you go with this architecture : ```S3 → EventBridge → StepFunction → Glue → Silver S3```
- If you set the max concurrency to 10 of an AWS glue ETL then you may face the issue related to **ETL bookmark conflict**.
    - Explaination
        - 2 files arrive
        - 2 StepFn executions start
        - 2 Glue jobs run simultaneously
        - Bookmark system = SHARED STATE
        - Long story short you CANNOT run parallel Glue jobs with bookmarks enabled.
    - Solution I have to disable the bookmark for glue ETL pipeline. 
        - Reason : 
            - My event passes the which bucket recieved what file to state function which then passes that information to Glue ETL pipeline so my pipeline knows which file to ingest and from where hence bookmark is not needed here 
            - It was required when instead of implementing file level scanning I would have implemented folder level scanning then it would have been necessary 
- AWS glue ETL hitting its max concurrency limit and throwing errors
    - I have set the max concurrency of this ETL pipeline to be 10. This means the moment the number of files recieved in S3 exceeds 10 AWS glue ETL will throw errors in case of the current architecture ```S3 → EventBridge → StepFunction → Glue → Silver S3```
    - Reason:
        - 40 files arrives in S3 bucket
        - All trigger the step function and the step function trigger AWS glue ETL parallely but since I have set max parallel process to be 10 it will not allow more than that hence the error.
        - Hence we can say that this is a throttling control problem.
    - Solution : 
        - We have to improve our architecture and use some kind of queue to store all the events coming in hot right from the source S3 bucket
        - The missing piece in my current architecture was Buffering + Controlled Dispatch
        - Correction:
            - During my reseach I found that Step function can only be triggered only by
                - EventBridge
                - API
                - SDK
                - Lambda
            - Which means I cannot use SQS directly to trigger my step function which is a bummer but I can use Lamdba function to trigger my step function and I know that SQS can trigger Lambda function directly. 
            - So I have to make changes in the architecture accordingly I have to put a lambda function between SQS and Step function ```S3 → EventBridge → SQS BUFFER → Lambda (trigger) → StepFn → Glue → Silver S3```

#### **Implementation : (Version 4)**
- As discussed above I am going to implement this architecture ---> ```S3 → EventBridge → SQS BUFFER → Lambda (trigger) → StepFn → Glue → Silver S3```
- **First configure you source S3 bucket to use event bridge:**
    - ![send_events_to_event_bridge_s3](images/production_grade_glue_version3_handled_concurrency/s3/send_events_to_event_bridge_s3.png)
    - Set the source s3 in a way that it sends the file arrival event message straight to eventBridge.
- **Setup the EventBridge:**
    - ![event_bridge_diagram](images/production_grade_glue_version3_handled_concurrency/eventBridge/event_bridge_diagram.png)
    - ![event_bridge_diagram_2](images/production_grade_glue_version3_handled_concurrency/eventBridge/event_bridge_diagram_2.png)
    - ![event_bridge_diagram_2_permissions](images/production_grade_glue_version3_handled_concurrency/eventBridge/event_bridge_diagram_2_permissions.png)
    - ![event_bridge_diagram_2_DLQ](images/production_grade_glue_version3_handled_concurrency/eventBridge/event_bridge_diagram_2_DLQ.png)
    - Here is the event pattern filter.
        - ```json
            {
            "source": ["aws.s3"],
            "detail-type": ["Object Created"],
            "detail": {
                "bucket": {
                "name": ["aws-glue-s3-bucket-one"]
                },
                "object": {
                "key": [{
                    "prefix": "raw_data/sales_data/"
                }]
                }
            }
            }
            ```
    - Set the event bridge in a way that it recieves the event message of file arrival directly from S3 bucket and then send that event message to SQS queue named ```FileProcessingQueue.fifo```
- **SQS for handling file arrival event message from S3 setup:**
    - **STEP 1:**
        - ![FileProcessingQueue.fifo](images/production_grade_glue_version3_handled_concurrency/sqs/FileProcessingQueue.fifo.png)
        - Set the default visibility time of messages in this ```FileProcessingQueue.fifo``` queue to be 20 minutes. 
        - Don't forget to setup FIFO in this queue if you want your messages to execute in an orderly fashon
        - This is the access policy that I used for this queue 
        ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                "Sid": "AllowEventBridgeToSend",
                "Effect": "Allow",
                "Principal": {
                    "Service": "events.amazonaws.com"
                },
                "Action": "sqs:SendMessage",
                "Resource": "arn:aws:sqs:ap-south-1:406868976171:FileProcessingQueue",
                "Condition": {
                    "ArnEquals": {
                    "aws:SourceArn": "arn:aws:events:ap-south-1:406868976171:rule/ActivateLambdaFuncEventBridgeRules"
                    }
                }
                }
            ]
        }
        ```
- **Setup a Lambda function**
    - This lambda function will be triggered via SQS named ```FileProcessingQueue.fifo``` and then this lambda function in return will trigger a state function called ```orchestrate_data_ingestion```
    - Why this is needed because SQS cannot directly trigger Step function it can only trigger a lambda function 
    - Here is the lambda function code that I am using 
    ```python
        import json
        import boto3
        import uuid
        from datetime import datetime, timezone, timedelta

        sf = boto3.client("stepfunctions")

        STATE_MACHINE_ARN = "arn:aws:states:ap-south-1:406868976171:stateMachine:orchestrate_data_ingestion"

        def lambda_handler(event, context):

            execution_id = str(uuid.uuid4())[:8]

            IST = timezone(timedelta(hours=5, minutes=30))
            timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S %Z")

            print(f"\n===== Lambda Invocation START =====")
            print(f"ExecutionID: {execution_id}")
            print(f"Timestamp: {timestamp}")
            print("Full Event From SQS:", json.dumps(event))
            print("===================================\n")

            files = []

            for record in event["Records"]:
                body = json.loads(record["body"])

                files.append({
                    "bucket": body["detail"]["bucket"]["name"],
                    "key": body["detail"]["object"]["key"]
                })

            print("Files collected:", files)

            sf.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                input=json.dumps({"files": files})
            )

            return {"status": "ok"}
    ```
    - Here is the lambda function trigger configuration
    ![lambda_function_trigger_conf](images/production_grade_glue_version3_handled_concurrency/lambda_function/lambda_function_trigger_conf.png)
    - When configuring batch size to 5 and set up max concurrency for lambda function to 2 
    - In order for your lambda function to work properly you need to create a new role that has permission like this 
    ```json
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "states:StartExecution"
                    ],
                    "Resource": "arn:aws:states:ap-south-1:406868976171:stateMachine:orchestrate_data_ingestion"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "sqs:ReceiveMessage",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes"
                    ],
                    "Resource": "arn:aws:sqs:ap-south-1:406868976171:FileProcessingQueue.fifo"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    "Resource": "*"
                }
            ]
        }
    ```
- **Setup AWS Step function (Orchestrate AWS ETL pipeline)**
    - ```orchestrate_data_ingestion``` step function will trigger the AWS glue ETL pipeline
    - ```json
        {
        "Comment": "Glue ETL Orchestration with Controlled Concurrency",
        "StartAt": "ProcessFiles",
        "States": {
            "ProcessFiles": {
            "Type": "Map",
            "ItemsPath": "$.files",
            "MaxConcurrency": 1,
            "Iterator": {
                "StartAt": "RunGlueJob",
                "States": {
                "RunGlueJob": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "Parameters": {
                    "JobName": "ingest_sales_data",
                    "Arguments": {
                        "--source_bucket.$": "$.bucket",
                        "--source_key.$": "$.key"
                    }
                    },
                    "Retry": [
                    {
                        "ErrorEquals": [
                        "Glue.ConcurrentRunsExceededException"
                        ],
                        "IntervalSeconds": 60,
                        "MaxAttempts": 10,
                        "BackoffRate": 1.5
                    },
                    {
                        "ErrorEquals": [
                        "States.ALL"
                        ],
                        "IntervalSeconds": 30,
                        "MaxAttempts": 2,
                        "BackoffRate": 2
                    }
                    ],
                    "Catch": [
                    {
                        "ErrorEquals": [
                        "States.ALL"
                        ],
                        "Next": "SendToDLQ"
                    }
                    ],
                    "End": true
                },
                "SendToDLQ": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": {
                    "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
                    "MessageBody.$": "$"
                    },
                    "End": true
                }
                }
            },
            "End": true
            }
        }
        }
      ```
- **Setup DLQ**
    - This DLQ will be used by the step function that is responsible for replaying ETL pipeline.
    - This DLQ saves the event message sent from source s3 in the event when the ETL fails to ingest a particular file.
    - ![DLQ](images/production_grade_glue_version3_handled_concurrency/sqs/DLQ.png)
    - This is the sqs policy I used 
    ```json
    {
        "Version": "2012-10-17",
        "Id": "__default_policy_ID",
        "Statement": [
            {
            "Sid": "__owner_statement",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::406868976171:root"
            },
            "Action": "SQS:*",
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:sales-ingestion-dlq"
            },
            {
            "Sid": "AllowEventBridgeToSendMessage",
            "Effect": "Allow",
            "Principal": {
                "Service": "events.amazonaws.com"
            },
            "Action": "sqs:SendMessage",
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:sales-ingestion-dlq",
            "Condition": {
                "ArnEquals": {
                "aws:SourceArn": "arn:aws:events:ap-south-1:406868976171:rule/ActivateLambdaFuncEventBridgeRules"
                }
            }
            },
            {
            "Sid": "AWSEvents_ActivateLambdaFuncEventBridgeRules_dlq_41c9320f-edd2-4fde-ad71-2985e68b1d7c",
            "Effect": "Allow",
            "Principal": {
                "Service": "events.amazonaws.com"
            },
            "Action": "sqs:SendMessage",
            "Resource": "arn:aws:sqs:ap-south-1:406868976171:sales-ingestion-dlq",
            "Condition": {
                "ArnEquals": {
                "aws:SourceArn": "arn:aws:events:ap-south-1:406868976171:rule/ActivateLambdaFuncEventBridgeRules"
                }
            }
            }
        ]
    }
    ```
- **Setup AWS Step function (Replay AWS ETL pipeline)**
    - ```replay_failed_ingestion``` This step function will allow us to re-invoke the ETL pipeline manually in the event where our pipeline failed and even retry failed.
    - SOP is that :
        - suppose you recieve a corrupted csv file 
        - you pipeline retries 3 times and then fails 
        - you get a mail telling you that the ETL failed to ingest this file along with the file name 
        - The event message from S3 gets stored in the DLQ
        - after fixing the issue I can simple use this state function to manualy run the ETL pipeline 
        - since this state function reads the event message from the DLQ the ETL knows which file to reprocess.
        - This is why this replay step function is essential.
    - ```json
        {
        "Comment": "Replay ETL from DLQ (Fixed Version)",
        "StartAt": "ReceiveFromDLQ",
        "States": {
            "ReceiveFromDLQ": {
            "Type": "Task",
            "Resource": "arn:aws:states:::aws-sdk:sqs:receiveMessage",
            "Parameters": {
                "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
                "MaxNumberOfMessages": 10
            },
            "ResultPath": "$.dlq",
            "Next": "CheckIfEmpty"
            },
            "CheckIfEmpty": {
            "Type": "Choice",
            "Choices": [
                {
                "Variable": "$.dlq.Messages",
                "IsPresent": false,
                "Next": "Success"
                }
            ],
            "Default": "ReplayBatch"
            },
            "ReplayBatch": {
            "Type": "Map",
            "ItemsPath": "$.dlq.Messages",
            "MaxConcurrency": 1,
            "Iterator": {
                "StartAt": "ExtractPayload",
                "States": {
                "ExtractPayload": {
                    "Type": "Pass",
                    "Parameters": {
                    "receiptHandle.$": "$.ReceiptHandle",
                    "body.$": "States.StringToJson($.Body)"
                    },
                    "ResultPath": "$.parsed",
                    "Next": "ParseCause"
                },
                "ParseCause": {
                    "Type": "Pass",
                    "Parameters": {
                    "receiptHandle.$": "$.parsed.receiptHandle",
                    "cause.$": "States.StringToJson($.parsed.body.Cause)"
                    },
                    "ResultPath": "$.payload",
                    "Next": "RunGlueJob"
                },
                "RunGlueJob": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::glue:startJobRun.sync",
                    "ResultPath": "$.glueResult",
                    "Parameters": {
                    "JobName": "ingest_sales_data",
                    "Arguments": {
                        "--source_bucket.$": "$.payload.cause.Arguments['--source_bucket']",
                        "--source_key.$": "$.payload.cause.Arguments['--source_key']"
                    }
                    },
                    "Catch": [
                    {
                        "ErrorEquals": [
                        "States.ALL"
                        ],
                        "Next": "FailState"
                    }
                    ],
                    "Next": "DeleteMessage"
                },
                "DeleteMessage": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::aws-sdk:sqs:deleteMessage",
                    "Parameters": {
                    "QueueUrl": "https://sqs.ap-south-1.amazonaws.com/406868976171/sales-ingestion-dlq",
                    "ReceiptHandle.$": "$.payload.receiptHandle"
                    },
                    "Next": "SuccessState"
                },
                "FailState": {
                    "Type": "Fail"
                },
                "SuccessState": {
                    "Type": "Succeed"
                }
                }
            },
            "Next": "Success"
            },
            "Success": {
            "Type": "Succeed"
            }
        }
        }
      ```
      
- **Setup AWS glue ETL pipeline:**
    - Here is the python code for the ETL pipeline 
    ```python
    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.gluetypes import *
    from awsgluedq.transforms import EvaluateDataQuality
    from awsglue import DynamicFrame

    from functools import reduce
    from pyspark.sql import functions as F

    def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
        for alias, frame in mapping.items():
            frame.toDF().createOrReplaceTempView(alias)
        result = spark.sql(query)
        return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
    def _find_null_fields(ctx, schema, path, output, nullStringSet, nullIntegerSet, frame):
        if isinstance(schema, StructType):
            for field in schema:
                new_path = path + "." if path != "" else path
                output = _find_null_fields(ctx, field.dataType, new_path + field.name, output, nullStringSet, nullIntegerSet, frame)
        elif isinstance(schema, ArrayType):
            if isinstance(schema.elementType, StructType):
                output = _find_null_fields(ctx, schema.elementType, path, output, nullStringSet, nullIntegerSet, frame)
        elif isinstance(schema, NullType):
            output.append(path)
        else:
            x, distinct_set = frame.toDF(), set()
            for i in x.select(path).distinct().collect():
                distinct_ = i[path.split('.')[-1]]
                if isinstance(distinct_, list):
                    distinct_set |= set([item.strip() if isinstance(item, str) else item for item in distinct_])
                elif isinstance(distinct_, str) :
                    distinct_set.add(distinct_.strip())
                else:
                    distinct_set.add(distinct_)
            if isinstance(schema, StringType):
                if distinct_set.issubset(nullStringSet):
                    output.append(path)
            elif isinstance(schema, IntegerType) or isinstance(schema, LongType) or isinstance(schema, DoubleType):
                if distinct_set.issubset(nullIntegerSet):
                    output.append(path)
        return output

    def drop_nulls(glueContext, frame, nullStringSet, nullIntegerSet, transformation_ctx) -> DynamicFrame:
        nullColumns = _find_null_fields(frame.glue_ctx, frame.schema(), "", [], nullStringSet, nullIntegerSet, frame)
        return DropFields.apply(frame=frame, paths=nullColumns, transformation_ctx=transformation_ctx)

    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'source_bucket',
        'source_key'
    ])

    source_bucket = args['source_bucket']
    source_key = args['source_key']

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Default ruleset used by all target nodes with data quality enabled
    DEFAULT_DATA_QUALITY_RULESET = """Rules = [
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
    ]"""

    # -------- DELIMITER CORRUPTION CHECK -------- #
    # It checks for delimiter / structural corruption STARTS
    import csv
    import io

    raw_lines = spark.read.text(f"s3://{source_bucket}/{source_key}").collect()

    if len(raw_lines) < 2:
        raise Exception("Glue Visual ETL | Corrupted CSV detected (file has no data rows)")

    def count_csv_columns(line: str) -> int:
        """Count columns correctly, respecting quoted fields containing commas."""
        try:
            return len(next(csv.reader(io.StringIO(line))))
        except StopIteration:
            return 0

    header_line = raw_lines[0][0]
    expected_col_count = count_csv_columns(header_line)

    bad_row_indices = []
    for i, row in enumerate(raw_lines[1:], start=2):
        line = row[0]
        actual_col_count = count_csv_columns(line)
        if actual_col_count != expected_col_count:
            bad_row_indices.append((i, actual_col_count, line[:80]))

    if bad_row_indices:
        details = "\n".join(
            [f"  Line {idx}: expected {expected_col_count} cols, got {actual} → {preview}..."
            for idx, actual, preview in bad_row_indices]
        )
        raise Exception(
            f"Glue Visual ETL | Corrupted CSV detected — {len(bad_row_indices)} row(s) have wrong column count "
            f"(likely semicolons used as delimiters instead of commas):\n{details}"
        )

    print(f"Glue Visual ETL | Column count validation passed: all rows have {expected_col_count} columns")
    # It checks for delimiter / structural corruption ENDS
    # -------- DELIMITER CORRUPTION CHECK -------- #

    # Script generated for node Raw data source S3
    RawdatasourceS3_node1772431168408 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "mode": "PERMISSIVE", "optimizePerformance": False}, connection_type="s3", format="csv", 
        # connection_options={"paths": ["s3://aws-glue-s3-bucket-one/raw_data/sales_data/"], "recurse": True}, 
        connection_options={"paths": [f"s3://{source_bucket}/{source_key}"],"recurse": False},
        transformation_ctx="RawdatasourceS3_node1772431168408")
    # for debugging only
    print(f"Glue Visual ETL | Processing file: s3://{source_bucket}/{source_key}")

    df = RawdatasourceS3_node1772431168408.toDF()

    print("Glue Visual ETL | DEBUG bucket:", source_bucket)
    print("Glue Visual ETL | DEBUG key:", source_key)
    print("Glue Visual ETL | DEBUG columns:", df.columns)
    print("Glue Visual ETL | DEBUG row count:", df.count())
    print("Glue Visual ETL | DEBUG schema:", df.schema)
    print("Glue Visual ETL | DEBUG sample rows:", df.limit(2).toPandas())

    # -------- CORRUPTION CHECK -------- #
    # checks Spark-level corruption after parsing STARTS

    # If Spark inferred no columns → corrupted file
    if len(df.columns) == 0:
        raise Exception("Glue Visual ETL | Corrupted CSV detected (no columns inferred)")

    expected_cols = len(df.columns)

    # If dataframe empty → corrupted file
    if df.limit(1).count() == 0:
        raise Exception("Glue Visual ETL | Corrupted CSV detected (empty dataframe)")

    # Row-level corruption detection
    null_exprs = [F.col(c).isNull().cast("int") for c in df.columns]

    bad_rows = df.filter(
        reduce(lambda a, b: a + b, null_exprs) > expected_cols * 0.7
    )

    if bad_rows.limit(1).count() > 0:
        raise Exception("Glue Visual ETL | Corrupted CSV detected (null-heavy rows)")

    # checks Spark-level corruption after parsing ENDS
    # -------- CORRUPTION CHECK -------- #

    #--------- CONVERT THE COLUMNS IN THE CSV FILE TO CORRECT DATA TYPES --------#
    # Script generated for node SQL Query
    SqlQuery61 = '''
    SELECT
        product_id,
        product_name,
        category,
        about_product,
        user_id,
        user_name,
        review_id,
        review_title,
        review_content,
        img_link,
        product_link,

        -- discounted_price: ₹149 → 149.0
        CAST(
            REGEXP_REPLACE(discounted_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS discounted_price,

        -- actual_price: ₹1,000 → 1000.0
        CAST(
            REGEXP_REPLACE(actual_price, '[^0-9.]', '')
            AS DOUBLE
        ) AS actual_price,

        -- discount_percentage: 85% → 85.0
        CAST(
            REGEXP_REPLACE(discount_percentage, '[^0-9.]', '')
            AS DOUBLE
        ) AS discount_percentage,

        -- rating: 3.9 → 3.9
        CAST(
            REGEXP_REPLACE(rating, '[^0-9.]', '')
            AS DOUBLE
        ) AS rating,

        -- rating_count: 24,871 → 24871
        CAST(
            REGEXP_REPLACE(rating_count, '[^0-9]', '')
            AS INT
        ) AS rating_count

    FROM myDataSource;
    '''

    try:
        SQLQuery_node1772431252856 = sparkSqlQuery(glueContext, query = SqlQuery61, mapping = {"myDataSource":RawdatasourceS3_node1772431168408}, transformation_ctx = "SQLQuery_node1772431252856")
    except Exception as e:
        print(f"Glue Visual ETL | SQL query execution failed | {e}")
        raise RuntimeError("Failing Glue job explicitly | Failed to execute sql query")
    #--------- CONVERT THE COLUMNS IN THE CSV FILE TO CORRECT DATA TYPES --------#

    # Script generated for node Drop Null Fields
    DropNullFields_node1772431857999 = drop_nulls(glueContext, frame=SQLQuery_node1772431252856, nullStringSet={"", "null"}, nullIntegerSet={-1}, transformation_ctx="DropNullFields_node1772431857999")

    # -------- DEDUPLICATION STEP -------- #
    # This will check if there exists a row with a duplicate review_id if yes then it will drop one of the row and make sure that the row is unique
    # Doing this prevents any data duplication 
    df_clean = DropNullFields_node1772431857999.toDF()

    deduped_df = df_clean.dropDuplicates(["review_id"])

    print("Glue Visual ETL | Before dedup:", df_clean.count())
    print("Glue Visual ETL | After dedup:", deduped_df.count())

    deduped_dynamic_frame = DynamicFrame.fromDF(
        deduped_df, glueContext, "deduped_dynamic_frame"
    )
    # -------- DEDUPLICATION STEP -------- #

    # Script generated for node Silver layer data sink S3
    try:
        EvaluateDataQuality().process_rows(frame=deduped_dynamic_frame, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772428328053", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
    except Exception as e:
        print("Glue Visual ETL | DQ failed:", e)
        raise RuntimeError("Failing Glue job explicitly | Failed to run data quality rules")

    SilverlayerdatasinkS3_node1772432020219 = glueContext.getSink(path="s3://data-sink-one/silver_layer/sales_data/", connection_type="s3", updateBehavior="LOG", partitionKeys=["category"], enableUpdateCatalog=True, transformation_ctx="SilverlayerdatasinkS3_node1772432020219")
    SilverlayerdatasinkS3_node1772432020219.setCatalogInfo(catalogDatabase="aws-glue-tutorial-aditya",catalogTableName="silver_table_sales_data")
    SilverlayerdatasinkS3_node1772432020219.setFormat("glueparquet", compression="snappy")
    SilverlayerdatasinkS3_node1772432020219.writeFrame(deduped_dynamic_frame)
    job.commit()
    ```
    - Here is the configuration related to AWS glue ETL pipeline 
    - ![ETL_conf_1](images/production_grade_glue_version3_handled_concurrency/glue_ETL/ETL_conf_1.png)
    - ![ETL_conf_2](images/production_grade_glue_version3_handled_concurrency/glue_ETL/ETL_conf_2.png)
    - ![ETL_conf_3](images/production_grade_glue_version3_handled_concurrency/glue_ETL/ETL_conf_3.png)
    - ![ETL_conf_4](images/production_grade_glue_version3_handled_concurrency/glue_ETL/ETL_conf_4.png)
    - This ETL pipeline will recieve the params from lambda function using which it will know what files to process from the S3 bucket.
    - One thing to note make sure that you set max concurrency to 2 for glue ETL pipeline 
    - This is done to save cost 
    - This ETL pipeline will also prevent duplicated data in the table from ingestion.
        - ```python
            # -------- DEDUPLICATION STEP -------- #
            # This will check if there exists a row with a duplicate review_id if yes then it will drop one of the row and make sure that the row is unique
            # Doing this prevents any data duplication 
            df_clean = DropNullFields_node1772431857999.toDF()

            deduped_df = df_clean.dropDuplicates(["review_id"])

            print("Glue Visual ETL | Before dedup:", df_clean.count())
            print("Glue Visual ETL | After dedup:", deduped_df.count())

            deduped_dynamic_frame = DynamicFrame.fromDF(
                deduped_df, glueContext, "deduped_dynamic_frame"
            )
            # -------- DEDUPLICATION STEP -------- #
            ```
            - This code checks for duplicate ```review_id``` in the rows and will only allow one of them to pass so that it can be ingested.

### NOTE : Common Data Quality rules in AWS Visual ETL pipeline
Common DQ rules
You can use:
- IsComplete
- IsUnique
- RowCount
- ColumnCount
- ColumnValues BETWEEN
- ColumnLength
- MatchesRegex
- ReferentialIntegrity

## Implementation : (Version 5) (Pending to be implemented)
- In this phase of implementation I am going to implement dynamo db to keep track of files that have been ingested successfully or failed or in progress

### Theory : Implementation (Version 5)
#### DynamoDB vs RDS
-  **DynamoDB (NoSQL, key-value)**
    - Schema: flexible (no rigid schema)
    - Access pattern: key-based lookups (O(1))
    - Scaling: automatic, virtually unlimited
    - Concurrency: built-in atomic conditional writes
    - Latency: single-digit milliseconds
    - Operations: simple (get/put/update)
    - Cost model: per request
    - atomic conditional writes at scale
- **RDS (Relational DB)**
    - Schema: strict (tables, joins, constraints)
    - Access pattern: complex queries, joins
    - Scaling: vertical (or complex horizontal)
    - Concurrency: transactional (ACID), but heavier
    - Latency: higher than DynamoDB
    - Operations: SQL, joins, aggregations
    - Cost model: instance-based (always on)

# TODO tomorrows task START FROM HERE
```bash
StepFn
   ↓
Check DynamoDB
   ↓
IF processed → skip
IF not → run Glue → mark DONE
```

This gives:
- idempotency
- replay safety
- full audit
- production-grade


🔹 Recommended Schema

Partition key:

file_key (string)

Attributes:

status (RUNNING / DONE / FAILED)
execution_id
timestamp



## Creating an end-to-end ETL pipeline from source to dashboard (TODO)
```bash
Sources (OLTP DB, CSV, APIs, logs)
        ↓
Ingestion Layer (Glue / Kinesis / DMS)
        ↓
Raw S3 (Bronze)
        ↓
Transformation (Glue / Spark / dbt)
        ↓
Curated S3 (Silver/Gold)
        ↓
Athena / Redshift
        ↓
QuickSight Dashboard
```

Implement this tomorrow 
use this code for analysis make sure that you are able to provide data according to this so that analysis can be completed and the final curated data can be used in AWS dashboard use this notebook as a reference which transformation to use when.
https://www.kaggle.com/code/mhassansaboor/amazon-sales-insights-strategy

## Partition data in S3 bucket
### What is Data pruning?
Data pruning means : Skipping irrelevant data so the system doesn't scan unnecessary files or blocks.

In simple terms . 
- Instead of reading everything the engine reads only what's needed.

**Why it matters?** <br>
- In systems like Athena , Spark, Redshift spectrum etc.
- You pay based on: 
    - How much data is scanned.
- So pruning directly reduces:
    - Query time 
    - Cost
    - Resource usage

**What Is Partitioning?** <br>
- Partitioning means:
     - Physically dividing data into folders (or logical segments) based on a column.
- ```bash
s3://sales-data/
    year=2024/
        month=01/
        month=02/
    year=2025/
        month=01/
```
- Each partition holds only data for that value.

**How Partitioning Enables Pruning?** <br>
```sql
SELECT *
FROM sales
WHERE year = 2025;
```
- If data is partitioned by year, Athena:
    - Looks only inside year=2025/
    - Ignores year=2024/
- This is called:
    - Partition Pruning
- The engine prunes away irrelevant partitions.

**Without Partitioning** <br>
If everything is stored in one folder:
- Athena must:
    - Scan entire dataset
    - Even if you only need 2025
- That means:
    - More cost
    - Slower queries

