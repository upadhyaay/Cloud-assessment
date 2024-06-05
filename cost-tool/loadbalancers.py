# Written by Shashank Upadhyay (shashank.upadhyay@pwc.com)
# Extracts the data for AWS load balancers and realted target groups w.r.t each LB.
# The Script generates output as a CSV file and Uploads the same in s3 Bucket.

import boto3
import csv
import sys

# Create a list to store the print statements
print_output = []

# Function to redirect print statements to a list
def print_to_list(*args, **kwargs):
    output = ' '.join(map(str, args))
    print_output.append(output)

# Redirect print statements to the list
sys.stdout.write = print_to_list

## Provide values
aws_access_key='<AWS ACCESS KEY>'
aws_secret_key='<AWS SECRET KEY>'
region='<REGION>'
s3_bucket_name='<S3 BUCKET NAME>'
s3_key = 'Load-balancers-details.csv'

## Initializing the clients
elb = boto3.client('elbv2',region_name=region,aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)
ec2 = boto3.client('ec2',region_name=region,aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)

# function to get target groups name
def gettargetgroups(arn):
    tgs=elb.describe_target_groups(LoadBalancerArn=arn)
    tgstring=[]
    for tg in tgs["TargetGroups"]:
        tgstring.append(tg["TargetGroupName"])
    return tgstring

# function to get target groups ARN
def gettargetgrouparns(arn):
    tgs=elb.describe_target_groups(LoadBalancerArn=arn)
    tgarns=[]
    for tg in tgs["TargetGroups"]:
        tgarns.append(tg["TargetGroupArn"])
    return tgarns

# Function to get target groups type 
def gettargetgrouptype(arn):
    tgs=elb.describe_target_groups(LoadBalancerArn=arn)
    tgtype=[]
    for tg in tgs["TargetGroups"]:
        tgtype.append(tg["TargetType"])
    return tgtype

# Get the instances attached to a particular target group
def getinstancename(instanceid):
    instances=ec2.describe_instances(Filters=[
        {
            'Name': 'instance-id',
            'Values': [
                instanceid
            ]
        },
    ],)
    for instance in instances["Reservations"]:
        for inst in instance["Instances"]:
            for tag in inst["Tags"]:
                if tag['Key'] == 'Name':
                    return (tag['Value'])

# Function to Get Target group health description
def gettargethealth(arn):
    inss=elb.describe_target_health(TargetGroupArn=arn)
    instanceids=[]
    for ins in inss["TargetHealthDescriptions"]:
        ins["Name"]=getinstancename(ins['Target']['Id'])
        instanceids.append(ins['Target']['Id'])
        print (ins)

## The pagesize value must be less than or equal to 400
lbs = elb.describe_load_balancers(PageSize=400)


for lb in lbs["LoadBalancers"]:
    print(f'Load-Balancer-Name:{lb["LoadBalancerName"]}')
    print(f'Load-Balancer-Type:{lb["Type"]}')
    print(f'Load-Balancer-ARN:{lb["LoadBalancerArn"]}')
    print(f'TargetGroups:{str(gettargetgroups(lb["LoadBalancerArn"]))}')
    print(f'TargetGroup-Type:{str(gettargetgrouptype(lb["LoadBalancerArn"]))}')
    print(f'TargetGroups-ARN:{str(gettargetgrouparns(lb["LoadBalancerArn"]))}')

    for tgs in gettargetgrouparns(lb["LoadBalancerArn"]):
        gettargethealth(tgs)


# Write the print statements to a CSV file
with open('Load-balancers-details.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    for line in print_output:
        writer.writerow([line])

# Initialize s3 client.
s3_client = boto3.client('s3',region_name=region,aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)

# Upload the file to S3
s3_client.upload_file('Load-balancers-details.csv',s3_bucket_name,s3_key)
