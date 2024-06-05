# Written by Shashank Upadhyay (shashank.upadhyay@pwc.com)

# Manages "Orphaned" EBS Snapshots, i.e ones whose volume is deleted, and aren't being
# managed by AWS Backup or Amazon Data Lifecycle Manager, and aren't linked to an AMI.
# The default mode lists all orphaned snapshots.
# Pass "-e" or "--exclude-newest <n>" to exclude the newest n months of snapshots.
# Pass "-d" or "--delete" to delete snapshots instead of reporting them
# Pass "--dry-run" to do deletions in dry run mode, checking permissions
# The Script generates output as a CSV file and Uploads the same in s3 Bucket.


import boto3
from botocore.exceptions import ClientError
import argparse
from datetime import datetime, timedelta, timezone
import sys
import csv


parser = argparse.ArgumentParser(description='EBS Snapshot Orphans Report')
parser.add_argument('-e', '--exclude-newest', type=int, metavar='n', help='exclude the newest n months of snapshots')
parser.add_argument('-d', '--delete', action='store_true', help='delete matching snapshots instead of just reporting them')
parser.add_argument('--dry-run', action='store_true', help="do deletions in dry run mode that doesn't really delete")
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

# How far back should we ignore?
months = args.exclude_newest
ignore_days = months * 30 if months else 0


ec2 = boto3.client('ec2',
                   aws_access_key_id=aws_access_key,
                   aws_secret_access_key= aws_secret_key,
                   region_name=region)

snapshots = ec2.describe_snapshots(OwnerIds=['self']).get('Snapshots', [])


# Get all the snapshot data
snapshots = []
response = ec2.describe_snapshots(OwnerIds=['self'], MaxResults=1000)
while True:
    snapshots.extend(response.get('Snapshots', []))


    # check if there's more to retrieve    
    token = response.get('NextToken', '')
    if token == '':
        break
    response = ec2.describe_snapshots(NextToken=token, OwnerIds=['self'], MaxResults=1000)


if snapshots:
    # Sort snapshots by volume ID and then by timestamp
    snapshots.sort(key=lambda snapshot: snapshot['VolumeId'] + str(snapshot['StartTime']))


    # Loop over all snapshots, filtering to identify ones to process, and collecting summary info.
    v_prev = None
    num_volumes = 0
    num_volumes_ignored = 0
    num_snap_vol_ignored = 0
    num_snap_too_new = 0
    num_snap_compliance = 0
    num_snap_ami = 0
    num_snap_orphans = 0
    ignore_volume = False
    delete_confirmed = False
    for row in snapshots:
        v = row['VolumeId']
        sid = row['SnapshotId']
        
        # Is this for a different volume than last time?
        # Or for the special vol-ffffffff whose snapshots won't be related to each other?
        # Volume ID will be set to vol-ffffffff if EBS snapshots are copied. It loses the reference to the original volume.
        if v != v_prev or v == 'vol-ffffffff':
            # Different ancestry to previous snapshot
            num_volumes += 1
            v_prev = v
            ignore_volume = False


            # Does it still exist?  Don't check if vol-ffffffff as will never be the case for copies
            if v != 'vol-ffffffff':
                try:
                    volumes = ec2.describe_volumes(VolumeIds=[v]).get('Volumes', [])
                    if volumes:
                        num_volumes_ignored += 1
                        ignore_volume = True # Yes it exists, so ignore the associated snapshots.
                except ClientError as e:
                    code = e.response.get('Error', {}).get('Code')
                    if code != 'InvalidVolume.NotFound':
                        raise # Ignore the expected NotFound exception, re-raise others


        # Only process this snapshot if we're not ignoring this volume
        if ignore_volume:
            num_snap_vol_ignored += 1
        else:
            # Is the snapshot new enough to ignore?
            timestamp = row['StartTime']
            if timestamp > datetime.now(timezone.utc) - timedelta(days=ignore_days):
                num_snap_too_new += 1
                continue  # Done with this snapshot as it's too new to report, go to next row
                
            # Is the snapshot managed by AWS Backup or Amazon Data Lifecycle Manager?
            # If so, can ignore it.  Look for the identifying tags.
            for tag in row.get('Tags', []):
                if (tag.get('Key') == 'aws:backup:source-resource' or
                    tag.get('Key') == 'dlm:managed'):
                    num_snap_compliance += 1
                    break
            else:
                # Fell through the loop without hitting 'break', so no it's not managed by AWS Backup / Amazon DLM
            
                # Is the snapshot attached to an AMI?  Look for an AMI with this snapshot ID, and if found
                # then ignore it.
                images = ec2.describe_images(
                             Filters=[{'Name':'block-device-mapping.snapshot-id', 'Values':[sid]}]
                         ).get('Images', [])
                if images:
                    num_snap_ami += 1
                    continue  # Done with this snapshot as it's attached to an AMI, go to next row


                # Finally after passing all the filters, process this snapshot as an Orphan
                num_snap_orphans += 1
                ts_str = str(timestamp).split('.')[0].split('+')[0]  # Strip off ms & timezone info
                if args.delete:
                    # Delete the snapshot, swallowing any DryRunOperation exception as this just indicates a successful dry run
                    try:
                        if not args.dry_run and not delete_confirmed:
                            print("You have chosen to delete matching snapshots.  It's recommended that you first run this "
                                  "script without the '--delete' option to get a report of snapshots that will be deleted.\n"
                                  "Are you sure you want to continue (Y/N)?: ", end=''
                            )
                            response = input()
                            if response == 'Y':
                                delete_confirmed = True
                            else:
                                sys.exit()
                        ec2.delete_snapshot(SnapshotId=sid, DryRun=args.dry_run)
                    except ClientError as e:
                        if e.response.get('Error', {}).get('Code') != 'DryRunOperation':
                           raise  # Re-raise other exceptions
                    print(f"{'Skipping delete ' if args.dry_run else 'Deleted '}", end='')
                
                # Get the "Name" tag contents if available, otherwise fall back to Description attribute
                desc = row.get('Description', '')
                for tag in row.get('Tags', []):
                    if tag.get('Key') == 'Name':
                        desc = tag.get('Value', '')
                        break
                print(f'{v} / {sid} "{desc}" {ts_str}')


    # Output summary information
    print(f'Total volumes: {num_volumes}')
    print(f' - Ignored because volume still exists: {num_volumes_ignored}')
    print(f'Total snapshots: {len(snapshots)}')
    print(f' - Ignored because volume was ignored: {num_snap_vol_ignored}')
    print(f' - Of remaining, ignored because too new: {num_snap_too_new}')
    print(f' - Of remaining, ignored because managed by AWS Backup or Amazon Data Lifecycle Manager: {num_snap_compliance}')
    print(f' - Of remaining, ignored because linked to an AMI: {num_snap_ami}')
    print(f" - Remaining Orphans {'deleted' if args.delete else 'reported'}", end='')
    print(f"{' (skipped due to dry run)' if args.dry_run else ''}: {num_snap_orphans}")


else:
    print('No snapshots in this region owned by me.')

# Write the print statements to a CSV file
with open('Total_Snapshots.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    for line in print_output:
        writer.writerow([line])

s3_client = boto3.client('s3',region_name=region,aws_access_key_id=aws_access_key,aws_secret_access_key=aws_secret_key)

# Upload the file to S3
s3_client.upload_file('Total_Snapshots.csv',s3_bucket_name,s3_key)
