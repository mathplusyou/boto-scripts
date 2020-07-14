import boto3
import json
import sys
import argparse
from botocore.exceptions import WaiterError, ClientError

'''
    OVERVIEW:
    This scripts creates snapshots of a list of volumes. Created snapshots will inherit all volumes tags. By default snapshots will
    be created in the region that the volume is located. You can also specify a destination region that the created snapshots can be copied to.
    This script expects the volumes to be defined in a .json file like so:
    {
        "volumes" : ["volume1-id","volume2_id"]
    }

    SCRIPT ARGS
    The script takes the following cli arguments:
    -file <file-path>                : [Required]
    -source_region <aws_region>      : [Required] (Primary region in which snapshots will be created)
    -dest_region <aws_region>        : [Optional] (If specified, snapshots created in the source region will be copied to this region.)

    ex : python3 create-replicate-snapshots.py -file volumes.json -source_region us-west-2 -dest_region us-east-1
'''

SNAPSHOT_WAITER=10

def create_snapshots_from_vols(volumes, ec2_client):
    if len(volumes) < 1:
        raise Exception(
        f'ERROR: No snapshots in destination region that match defined tags.')
    print(f'{len(volumes)} volumes will be snapped!')
    snapshots = []
    for volume in volumes:
        volume_tags = ec2_client.describe_volumes(VolumeIds=[volume])['Volumes'][0]['Tags']
        try:
            r = ec2_client.create_snapshot(VolumeId=volume, TagSpecifications=[{"ResourceType": "snapshot", "Tags": volume_tags}])
            status_code = r['ResponseMetadata']['HTTPStatusCode']
            snapshot_id = r['SnapshotId']
            if status_code == 200:
                snapshots.append(snapshot_id)
                print(f'Snapshot : {snapshot_id} created for volume {volume}')
        except Exception as e:
            exception_message = "There was an error creating snapshot for volume with volume id "+volume+". [INFO] Error: \n"\
                + str(e)
    return snapshots

def copy_snapshot_to_dest_region(snapshots, source_client, dest_client, source_region, dest_region):
    print(f'Found {len(snapshots)} snapshots to copy from {source_region} to {dest_region}')
    for snapshot in snapshots:
        waiter = source_client.get_waiter('snapshot_completed')
        try:
            print(f'Waiting for {snapshot} to become available.')
            waiter.wait(SnapshotIds=[snapshot], WaiterConfig={
                'MaxAttempts': SNAPSHOT_WAITER})
        except WaiterError as e:
            print(f"ERROR: {snapshot} is not available to copy.")
            raise e

        try:
            snapshot_tags = source_client.describe_snapshots(SnapshotIds=[snapshot])['Snapshots'][0]['Tags']
            new_snapshot = dest_client.copy_snapshot(SourceSnapshotId=snapshot,SourceRegion=source_region, DestinationRegion=dest_region, TagSpecifications=[{"ResourceType": "snapshot", "Tags": snapshot_tags}])['SnapshotId']
            print(f'Created {new_snapshot} copy in {dest_region} from source {snapshot} in {source_region}')
        except ClientError as e:
            print(f'ERROR: failed to make copy of {snapshot} OUTPUT: {e}')
            return

def main(file, source_region, dest_region):

    with open(file) as json_data:
      volumes = [volume for volume in json.load(json_data)['volumes']]
    source_client = boto3.client('ec2', region_name=source_region)
    snapshots = create_snapshots_from_vols(volumes, source_client)

    if dest_region != "":
        dest_client = boto3.client('ec2', region_name=dest_region)
        copy_snapshot_to_dest_region(snapshots, source_client, dest_client, source_region, dest_region)
    
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-file", required=True, help="File path of the json file")
    ap.add_argument("-source_region", required=True, help="AWS region where volumes are located. By default, snapshots will be created here. : e.g 'us-west-2'.")
    ap.add_argument("-dest_region", required=False, help="[OPTIONAL] AWS region where you'd like snapshots to be copied to. : e.g. 'us-east-1'.", default="")
    kwargs = vars(ap.parse_args())
    main(**kwargs)