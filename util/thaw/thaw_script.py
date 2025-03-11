# thaw_script.py
#
# Thaws upgraded (premium) user data
#
# Copyright (C) 2015-2024 Vas Vasiliadis
# University of Chicago
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import boto3
import json
import os
import sys
import time

from botocore.exceptions import ClientError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import ConfigParser, ExtendedInterpolation

config = ConfigParser(os.environ, interpolation=ExtendedInterpolation())
config.read("../util_config.ini")
config.read("thaw_script_config.ini")

# Set up Amazon clients
sqs = boto3.client('sqs', region_name=config['sqs']['region'])
glacier = boto3.client('glacier', region_name=config['glacier']['region'])
dynamodb = boto3.resource('dynamodb', region_name=config['dynamodb']['region'])
table = dynamodb.Table(config['dynamodb']['annotation_table'])


"""A16
Initiate thawing of archived objects from Glacier
"""

def thaw_result_file(message):
    print(message)
    archive_id = message['archive_id']
    job_id = message['job_id']
    user_id = message['user_id']
    submit_time = message['submit_time']
    s3_key_result_file = message['s3_key_result_file']
    vault_name = config['glacier']['vault_name']
    thaw_restore_arn = config['sns']['thaw_restore_arn']

    description = json.dumps({
        'job_id': job_id,
        'archive_id': archive_id,
        's3_key_result_file': s3_key_result_file,
        'user_id': user_id,
        'submit_time': submit_time
    })


    try:
        response = glacier.initiate_job(
            vaultName = vault_name,
            jobParameters={
            'Type': 'archive-retrieval',
            'ArchiveId': archive_id,
            'Description': description,
            'Tier': 'Expedited',
            'SNSTopic': thaw_restore_arn

            })
        restore_job_id = response['jobId']
        status_message = "Expedited retrieval started"

    except glacier.exceptions.InsufficientCapacityException:

        response = glacier.initiate_job(
            vaultName = vault_name,
            jobParameters={
            'Type': 'archive-retrieval',
            'ArchiveId': archive_id,
            'Description': description,
            'Tier': 'Standard',
            'SNSTopic': thaw_restore_arn
            }
        )
        restore_job_id = response['jobId']
        status_message = "Standard retrieval started - unable to expedite"

    print(status_message)


    try:
        table.update_item(
            Key={'job_id': job_id, 'submit_time': submit_time},
            UpdateExpression="SET restore_job_id = :restore_job_id, restore_status = :status, restore_message = :message",
            ExpressionAttributeValues={
                ':restore_job_id': restore_job_id,
                ':status': 'IN_PROGRESS',
                ':message': status_message
            }
        )
    except ClientError as e:
        print(f"Error updating DynamoDB: {e}")

def handle_thaw_queue():

    # Read messages from the queue

    while True: 
        try:
            messages = sqs.receive_message(
                QueueUrl=config['sqs']['url'],
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )

            # Process messages --> initiate restore from Glacier
            if 'Messages' in messages:
                for message in messages['Messages']:
                    message_body = json.loads(message['Body'])
                    actual_message = json.loads(message_body['Message'])
                    thaw_result_file(actual_message)

                    # Delete messages
                    sqs.delete_message(
                    QueueUrl=config['sqs']['url'],
                    ReceiptHandle=message['ReceiptHandle']
                    )
        except ClientError as e:

            print(f"Error receiving or processing thaw sqs messages: {e}")



def main():

    # Get handles to resources

    # Poll queue for new results and process them
    handle_thaw_queue()


if __name__ == "__main__":
    main()

### EOF
