# Written by Shashank Upadhyay (shashank.upadhyay@pwc.com)
# Extracts the data for all s3 buckets present in an AWS account and the object details present in every respective bucket.
# The Script generates output as a CSV file and Uploads the same in s3 Bucket.

import boto3
import sys,csv
from botocore.exceptions import ClientError

## Provide values
aws_access_key='<aws_access_key>'
aws_secret_key='<aws_secret_key>'
region='ap-south-1'

# Create a list to store the print statements
print_output = []

# Function to redirect print statements to a list
def print_to_list(*args, **kwargs):
    output = ' '.join(map(str, args))
    print_output.append(output)

# Redirect print statements to the list
sys.stdout.write = print_to_list

# Initializing the s3 client
response = boto3.client('s3',aws_access_key_id=aws_access_key,
                   aws_secret_access_key= aws_secret_key,
                   region_name=region).list_buckets()

# List the buckets in AWS org.
for bucket in response['Buckets']:
    print(f"Bucket Name:{bucket['Name']}")
    print(f"Bucket Creation Date: {bucket['CreationDate']}")

    try:
# Initializing new client for objects in a bucket
        obj_resp = boto3.client('s3',aws_access_key_id=aws_access_key,
                    aws_secret_access_key= aws_secret_key,
                    region_name=region).list_objects(Bucket=bucket['Name'])
        
## Print The contents of the bucket
        if "Contents" in obj_resp:
            for file in obj_resp["Contents"]:
                print(f"file_name: {file['Key']}, size: {file['Size']}KB, Last Modified: {file['LastModified']}, StorageClass: {file['StorageClass']}")
        else:
            print('Bucket is Empty')
    except ClientError as e:
        if e.response.get('Error', {}).get('Code') != 'AccessDenied':
            raise Exception ('Error') 
        else:
            print('Bucket Skipped due to Insufficient Access') 
            pass


# Write the print statements to a CSV file
with open('Total_Snapshots_Size.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    for line in print_output:
        writer.writerow([line])
