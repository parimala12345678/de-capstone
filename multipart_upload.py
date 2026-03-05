import boto3
from boto3.s3.transfer import TransferConfig

# S3 client
s3 = boto3.client('s3')

file_path = r"C:\Users\Lenovo\Desktop\de-capstone\output.parquet"
bucket = "decapstone-data-lake"
key = "staging/output.parquet"

# Multipart configuration
config = TransferConfig(
    multipart_threshold=1024 * 25,
    max_concurrency=10,
    multipart_chunksize=1024 * 25,
    use_threads=True
)

# Upload
s3.upload_file(
    file_path,
    bucket,
    key,
    Config=config
)

print("Multipart upload completed")