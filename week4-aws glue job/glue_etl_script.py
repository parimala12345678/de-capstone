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

# Read parquet file from S3 raw
df = spark.read.parquet("s3://decapstone-data-lake/raw/output.parquet")

# Remove duplicates
df_clean = df.dropDuplicates()

# Write to curated
df_clean.write.mode("append").parquet("s3://decapstone-data-lake/curated/")

job.commit()