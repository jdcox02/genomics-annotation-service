# archive_script.py
#
# Archive free user data
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import time
import os
import sys
import json
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("../util_config.ini")
config.read("archive_script_config.ini")

# Set up AWS
sqs = boto3.client('sqs', region_name = config['sqs']['region'])
s3 = boto3.client('s3', region_name = config['s3']['region'])
glacier = boto3.client('glacier', region_name = config['glacier']['region'])
dynamodb = boto3.resource('dynamodb', region_name = config['dynamodb']['region'])
table = dynamodb.Table(config['dynamodb']['annotation_table'])

"""A14
Archive free user results files
"""

def handle_archive_queue(sqs, s3, glacier, table):

    try:
        # Read messages from the queue
        messages = sqs.receive_message(
            QueueUrl=config['sqs']['url'],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20
        )
        # Process messages
        if 'Messages' in messages:
            for message in messages['Messages']:
                process_status = process_message(json.loads(message['Body']), s3, glacier, table)
                
                # Delete message if processed or if the job should not be archived
                if process_status:
                    print("message was processed - results file was archived")
                else:
                    print("message was processed - job should not be archived")
                
                sqs.delete_message(
                    QueueUrl=config['sqs']['url'],
                    ReceiptHandle=message['ReceiptHandle']
                )
    
    except ClientError as e:
        print("Error processing queue: ", e)

def process_message(message, s3, glacier, table):
    print(message)
    job_id = message['job_id']
    user_id = message['user_id']
    submit_time = message['submit_time']

    try:
        response = table.query(
            KeyConditionExpression=Key('job_id').eq(job_id)
        )
        if 'Items' not in response or len(response['Items']) == 0:
            print(f"No job found for job_id: {job_id}")
            return False
        job = response['Items'][0]

        # Check if the archive_status is PENDING
        if job.get('archive_status') != 'PENDING':
            print(f"Job {job_id} is not pending for archival. Current status: {job.get('archive_status')}")
            return False
        
        result_filename = job['s3_key_result_file']
        username = config['s3']['username']
        result_key_name = f"{username}/{user_id}/{result_filename}"

        print(f"result key name: {result_key_name}")

        # Retrieve the result file from S3
        response = s3.get_object(Bucket=config['s3']['results_bucket'], Key=result_key_name)
        data = response['Body'].read()
        
        # Archive to Glacier
        archive_response = glacier.upload_archive(vaultName=config['glacier']['vault_name'], body=data)
        archive_id = archive_response['archiveId']
        
        # Update DynamoDB with the archive ID
        table.update_item(
            Key={'job_id': job_id, "submit_time": int(submit_time)},
            UpdateExpression="set results_file_archive_id = :id, archive_status = :status",
            ExpressionAttributeValues={
                ':id': archive_id,
                ':status': 'ARCHIVED'
            }
        )

        # Delete the file from S3 bucket
        s3.delete_object(Bucket=config['s3']['results_bucket'], Key=result_key_name)
        print(f"Deleted {result_key_name} from S3 after successful archive.")
        
        return True
    except ClientError as e:
        print("Error handling archival: ", e)
        return False

def main():

    # Get handles to SQS

    # Poll queue for new results and process them
    while True:
        handle_archive_queue(sqs, s3, glacier, table)





if __name__ == "__main__":
    main()

### EOF
