# Works out approximate EBS snapshot sizes using the "EBS Direct APIs", available since Dec 2019,
# to get the number of blocks in a volume's oldest snapshot, and number of changed blocks between
# each two snapshots.
# Still doesn't take into account any other optimisations like compression that AWS might do,
# but is a lot better than the console which just shows volume size for every snapshot.
# Also note by default the volume size is used as a quick approximation of the oldest snapshot's
# size because it contains all data for the volume. But often the oldest snapshot will actually
# be a lot smaller because EBS doesn't copy empty blocks.  You can get a more accurate estimate
# by passing "--list-blocks" or "-l" which will count the number of blocks in each oldest snapshot
# but it takes a lot longer.
# The Script generates output as a CSV file and Uploads the same in s3 Bucket.


import boto3
import argparse
import csv
import sys

parser = argparse.ArgumentParser(description='EBS Snapshot Sizes Report')
parser.add_argument('-v', '--volumeid', type=str, metavar='v', help='report only on snapshots for volume id v')
parser.add_argument('-l', '--list-blocks', action='store_true', help='accurately (but slowly) report oldest snapshot size for each volume')
args = parser.parse_args()

# Create a list to store the print statements
print_output = []

## Provide values
aws_access_key='<aws_access_key>'
aws_secret_key='<aws_secret_key>'
region='ap-south-1'
s3_bucket_name='<S3 bucket name>'
s3_key = '<s3 key>'

# Function to redirect print statements to a list
def print_to_list(*args, **kwargs):
    output = ' '.join(map(str, args))
    print_output.append(output)

# Redirect print statements to the list
sys.stdout.write = print_to_list


filters = []
if args.volumeid:
    # Filter snapshots by the specified volume id
    filters = [
        {
            'Name': 'volume-id',
            'Values': [
                args.volumeid
            ]
        }
    ]

ebs = boto3.client('ebs',region_name=region,aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)
ec2 = boto3.client('ec2',region_name=region,aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)


def initial_snapshot_size(snapshotid):
    """
    Calculate size of an initial snapshot in GiB by listing its blocks.
    """
    num_blocks = 0
    response = ebs.list_snapshot_blocks(
        SnapshotId=snapshotid,
        MaxResults=1000
    )
    blocksize_kb = response.get('BlockSize', 0) / 1024
    while True:
        num_blocks += len(response.get('Blocks', []))


        # check if there's more to retrieve    
        token = response.get('NextToken', '')
        if token == '':
            break
        response = ebs.list_snapshot_blocks(
            NextToken=token,
            SnapshotId=snapshotid,
            MaxResults=1000
        )
    return num_blocks * blocksize_kb / (1024 * 1024)


def changed_blocks_size(firstsnapshotid, secondsnapshotid):
    """
    Calculate changed blocks total size in GiB between two snapshots with common ancestry.
    """
    num_blocks = 0
    response = ebs.list_changed_blocks(
        FirstSnapshotId = firstsnapshotid,
        SecondSnapshotId = secondsnapshotid,
        MaxResults=1000
    )
    blocksize_kb = response.get('BlockSize', 0) / 1024
    while True:
        num_blocks += len(response.get('ChangedBlocks', []))


        # check if there's more to retrieve    
        token = response.get('NextToken', '')
        if token == '':
            break
        response = ebs.list_changed_blocks(
            NextToken=token,
            FirstSnapshotId = firstsnapshotid,
            SecondSnapshotId = secondsnapshotid,
            MaxResults=1000
        )
    return num_blocks * blocksize_kb / (1024 * 1024)


# Get all the snapshot data
snapshots = []
response = ec2.describe_snapshots(OwnerIds=['self'], Filters=filters, MaxResults=1000)
while True:
    snapshots.extend(response.get('Snapshots', []))

    # check if there's more to retrieve    
    token = response.get('NextToken', '')
    if token == '':
        break
    response = ec2.describe_snapshots(NextToken=token, OwnerIds=['self'], Filters=filters, MaxResults=1000)

if snapshots:
    # Sort snapshots by volume ID and then by timestamp
    snapshots.sort(key=lambda snapshot: snapshot['VolumeId'] + str(snapshot['StartTime']))

    # For each volumeid go through the snapshots, reporting oldest one based on all data in the volume,
    # then subsequent ones have size calculated from the changed blocks
    v_prev = None
    sid_prev = None
    total_gb = 0
    num_volumes = 0
    for row in snapshots:
        v = row['VolumeId']
        sid = row['SnapshotId']
        description = ec2.describe_snapshots(SnapshotIds=[sid])
        description = description['Snapshots'][0]['Description'] 

        # Strip off ms & timezone info3
        timestamp = str(row['StartTime']).split('.')[0].split('+')[0]

        # Is this for the same volume as last time?
        # And not for the special vol-ffffffff whose snapshots won't be related to each other.
        # Volume ID will be set to vol-ffffffff if EBS snapshots are copied. It loses the reference to the original volume.
        if v == v_prev and v != 'vol-ffffffff':
            # Same ancestry as previous snapshot, so work out what changed
            gb = changed_blocks_size(sid_prev, sid)
            total_gb += gb
            print(f' - {timestamp}, {sid_prev} to {sid}: {gb:0.3f} GiB')
           
        else:
            # Different ancestry to previous snapshot
            num_volumes += 1
            gb = row['VolumeSize']
            # Have we been asked to get a more accurate initial snapshot size by listing its blocks?
            if args.list_blocks:
                gb = initial_snapshot_size(sid)
            total_gb += gb
            print(f'Volume: {v}:')
            print(f'Snapshot Description : {description}')
            print(f" - {timestamp}, Initial Snapshot {sid}: {gb:0.3f} GiB")
            v_prev = v

        sid_prev = sid

    # Output GB in addition to GiB, useful for comparison with billing which is in GB-month.
    gigabytes = total_gb * 1.024**3
    print (f'Total snapshot storage estimate: {total_gb:0.3f} GiB ({gigabytes:0.3f} GB) across {num_volumes} volumes')
else:
    print('No snapshots in this region owned by me.') 

# Write the print statements to a CSV file
with open('Total_Snapshots_Size.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    for line in print_output:
        writer.writerow([line])

s3_client = boto3.client('s3',region_name=region,aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)

# Upload the file to S3
s3_client.upload_file('Total_Snapshots_Size.csv',s3_bucket_name, s3_key)
