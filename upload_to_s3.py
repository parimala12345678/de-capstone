import boto3

# Create S3 client
s3 = boto3.client('s3')

# Local file path
file_path = r"C:\Users\Lenovo\Desktop\de-capstone\output.parquet"

# Bucket name
bucket_name = "decapstone-data-lake"

# S3 path
s3_key = "raw/output.parquet"

# Upload file
s3.upload_file(file_path, bucket_name, s3_key)

print("File uploaded successfully to S3")