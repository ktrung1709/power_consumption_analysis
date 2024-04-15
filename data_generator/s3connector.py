import boto3

def test_s3_connection(access_key_id, secret_access_key, region, bucket_name):
    try:
        # Create an S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name=region
        )
        
        # Test connection by listing buckets
        response = s3_client.list_buckets()
        
        # Check if the specified bucket exists
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        if bucket_name in buckets:
            print(f"Successfully connected to S3 bucket '{bucket_name}'.")
        else:
            print(f"Bucket '{bucket_name}' not found.")
    except Exception as e:
        print(f"Error connecting to S3 bucket: {e}")

# Specify your AWS credentials and bucket information here
access_key_id = 'AKIATMFNNGPO53WMF6WR'
secret_access_key = '1UP/8BR0A3zy11lqjT7jcMWR8IhZR+NR+h/NBcPA'
region = 'ap-southeast-1'  # Example: 'us-east-1'
bucket_name = 'electricity-consumption-master-data'

# Test the connection
test_s3_connection(access_key_id, secret_access_key, region, bucket_name)