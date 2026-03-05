import boto3

# Create S3 client
s3 = boto3.client('s3')

bucket_name = "decapstone-data-lake"

response = s3.list_objects_v2(Bucket=bucket_name)

print("Files in S3 bucket:")

for obj in response.get('Contents', []):
    print(obj['Key'])