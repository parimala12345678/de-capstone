Week 4 – AWS Data Lake Implementation

Overview

In this task, a simple AWS Data Lake pipeline was created using Amazon S3, AWS Glue, and Amazon Athena.
The pipeline reads raw data from S3, processes it using a Glue PySpark ETL job, and writes the transformed data into a curated layer.

---

Architecture

S3 Data Lake Structure:

decapstome-data-lake/
│
├── raw/
│   └── output.parquet
│
├── staging/
│
├── curated/
│
└── athena-results/

---

Steps Implemented

1. Create S3 Data Lake

Created an S3 bucket:

decapstome-data-lake

Created folders:

- raw
- staging
- curated
- athena-results

Uploaded the parquet file to the raw layer.

---

2. Configure AWS Glue

Created a Glue Database:

decapstone_db

Created a Glue ETL job:

raw-to-curated-etl

Job configuration:

- Glue Version: 5.0
- Worker Type: G.1X
- Language: Python (PySpark)

---

3. Glue PySpark ETL Script

The ETL job reads parquet data from the raw layer, performs simple transformation, and writes the processed data to the curated layer.

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read raw data
df = spark.read.parquet("s3://decapstome-data-lake/raw/output.parquet")

# Transformation
df_clean = df.dropDuplicates()

# Write curated data
df_clean.write.mode("append").parquet("s3://decapstome-data-lake/curated/")

job.commit()

---

4. Query Using Athena

Configured Athena query result location:

s3://decapstome-data-lake/athena-results/

Queried the data using SQL.

Example query:

SELECT *
FROM raw
LIMIT 10;

---

Technologies Used

- AWS S3
- AWS Glue
- PySpark
- AWS Athena
- SQL

---

Screenshots

Screenshots included:

- S3 Bucket Structure
- Glue ETL Job Configuration
- Glue Job Run Status
- Athena Query Execution
- Curated Data Output

All screenshots are available in the screenshots folder.

---

Conclusion

This task demonstrates how to build a simple serverless data pipeline on AWS using S3 as a data lake, Glue for ETL processing, and Athena for querying the data.