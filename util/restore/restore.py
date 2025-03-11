# restore.py
#
# Restores thawed data, saving objects to S3 results bucket
# NOTE: This code is for an AWS Lambda function
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
##

import boto3
import json
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    s3 = boto3.client('s3', region_name=os.environ['AWS_REGION'])
    glacier = boto3.client('glacier', region_name=os.environ['AWS_REGION'])
    dynamodb = boto3.resource('dynamodb', region_name=os.environ['AWS_REGION'])
    table = dynamodb.Table(os.environ['DYNAMODB_TABLE'])
    sqs = boto3.client('sqs', region_name=os.environ['AWS_REGION'])

    try:
        logger.info(f"Event: {json.dumps(event)}")  # Log the entire event for debugging

        for record in event['Records']:
            try:
                message_body = json.loads(record['body'])
                logger.info(f"Message body: {message_body}")

                if 'Message' in message_body:
                    actual_message = json.loads(message_body['Message'])
                else:
                    actual_message = message_body

                logger.info(f"Message parsed: {actual_message}")

                # Extracting JobDescription
                job_description = json.loads(actual_message['JobDescription'])
                job_id = job_description['job_id']
                archive_id = job_description['archive_id']
                submit_time = int(job_description['submit_time'])
                user_id = job_description['user_id']
                s3_key_result_file = job_description['s3_key_result_file']
                glacier_job_id = actual_message['JobId']

                vault_name = os.environ['GLACIER_VAULT_NAME']
                bucket_name = os.environ['S3_BUCKET_NAME']
                cnet_id = os.environ['CNETID']
                s3_restore_key = f"{cnet_id}/{user_id}/{s3_key_result_file}"

                logger.info(f"Starting restoration for job_id: {job_id}, archive_id: {archive_id}")

                response = glacier.get_job_output(vaultName=vault_name, jobId=glacier_job_id)
                body = response['body'].read()

                s3.put_object(Bucket=bucket_name, Key=s3_restore_key, Body=body)
                logger.info(f"Stored restored object in S3 at {s3_restore_key}")

                table.update_item(
                    Key={'job_id': job_id, 'submit_time': submit_time},
                    UpdateExpression="REMOVE results_file_archive_id SET restore_status = :status, restore_message = :message, archive_status = :archive_status",
                    ExpressionAttributeValues={
                        ':status': 'COMPLETED',
                        ':message': 'Restoration complete',
                        ':archive_status': 'RESTORED'
                    }
                )
                logger.info(f"Updated DynamoDB for job_id: {job_id}")

                glacier.delete_archive(vaultName=vault_name, archiveId=archive_id)
                logger.info(f"Deleted archive from Glacier for archive_id: {archive_id}")

            except Exception as e:
                logger.error(f"Error restoring archive for job_id {job_id}: {str(e)}")
                continue

            # Delete the processed message from the SQS queue only if no exceptions occurred
            sqs_url = os.environ['SQS_QUEUE_URL']
            sqs.delete_message(
                QueueUrl=sqs_url,
                ReceiptHandle=record['receiptHandle']
            )
            logger.info(f"Deleted SQS message for job_id: {job_id}")

        return {
            'statusCode': 200,
            'body': json.dumps('Restoration successful!')
        }

    except Exception as e:
        logger.error(f"Error in lambda handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error in lambda handler: {str(e)}")
        }
