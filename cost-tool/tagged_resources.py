# Written by Shashank Upadhyay (shashank.upadhyay@pwc.com)
# Extracts the data for resources that have tags attached with them in AWS.
# The Script generates output as a CSV file and Uploads the same in s3 Bucket.

import boto3
import csv

## Please provide the Input values
aws_access_key='<AWS ACCESS KEY>'
aws_secret_key='<AWS SECRET KEY>'
region='<REGION>'
s3_bucket_name='<S3 BUCKET NAME>'
s3_key=f'tags-details_{region}.csv'

## header names of the output CSV file
field_names = ['ResourceArn', 'TagKey', 'TagValue']

## Locally stored file name which further is uploaded to s3.
args = 'tagged_resources.csv'

## Function writes the data into a CSV file
def writeToCsv(writer, args, tag_list):
    for resource in tag_list:
        print("Extracting tags for resource: " +
              resource['ResourceARN'] + "...")
        for tag in resource['Tags']:
            row = dict(
                ResourceArn=resource['ResourceARN'], TagKey=tag['Key'], TagValue=tag['Value'])
            writer.writerow(row)

## This function initiates the aws client, gets the data and calls the writecsv function
def main():
    restag = boto3.client('resourcegroupstaggingapi',aws_access_key_id=aws_access_key,aws_secret_access_key= aws_secret_key,region_name=region)
    with open(args, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, quoting=csv.QUOTE_ALL,
                                delimiter=',', dialect='excel', fieldnames=field_names)
        writer.writeheader()
        response = restag.get_resources(ResourcesPerPage=50)
        writeToCsv(writer,args, response['ResourceTagMappingList'])
        while 'PaginationToken' in response and response['PaginationToken']:
            token = response['PaginationToken']
            response = restag.get_resources(
                ResourcesPerPage=50, PaginationToken=token)
            writeToCsv(writer, args, response['ResourceTagMappingList'])

if __name__ == '__main__':
    main()
## Initializing the s3 client
    s3_client = boto3.client('s3',region_name=region,aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)

## Upload the file to S3
    s3_client.upload_file(args,s3_bucket_name,s3_key)
