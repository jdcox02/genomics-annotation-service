# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import os
import sys

import boto3
import driver
import time
import configparser
import json

"""A rudimentary timer for coarse-grained profiling
"""



#Read configuration file
#Resource - https://docs.python.org/3/library/configparser.html
config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
config.read('annotator_config.ini')

s3_region_name = config['aws']['AwsRegionName']
annot_table_name = config['gas']['AnnotationsTable']


dynamo = boto3.resource('dynamodb', region_name = s3_region_name)
ann_table = dynamo.Table(annot_table_name)
sns = boto3.client('sns', region_name=s3_region_name)
step = boto3.client('stepfunctions', region_name=s3_region_name)



class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")


def upload_file_to_s3(full_path, bucket, object_name):

    s3 = boto3.client("s3", region_name=s3_region_name)

    try:
        s3.upload_file(full_path, bucket, object_name)
        print(f"Successful upload of {object_name} to {bucket}")

    except Exception as e:
        print(f"Error uploading {object_name} to {bucket}", str(e))


def publish_sns_message(topic_arn, message):
    try:
        response = sns.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message),
            Subject="Job Completion Notification - Go archive"
        )
        print(f"Message published to SNS: {response['MessageId']}")
    except Exception as e:
        print(f"Failed to publish message to SNS: {str(e)}")

def start_step_function(state_machine_arn, message):
    # Convert message to JSON string
    input_json = json.dumps(message)

    # Start execution of the state machine
    try:
        response = step.start_execution(
            stateMachineArn=state_machine_arn,
            input=input_json
        )
        print(f"Step Function started: {response['executionArn']}")
    except Exception as e:
        print(f"Failed to start Step Function: {str(e)}")


if __name__ == "__main__":


    if len(sys.argv) == 4:
        print("Command Line Arguments:", sys.argv)
        with Timer():
            driver.run(sys.argv[1], "vcf")

        # Add code here:
        input_file_name = sys.argv[1]
        submit_time = int(sys.argv[2])
        user_role = sys.argv[3]

        print(f"Received user role: {user_role}")


        input_file_directory = os.path.dirname(input_file_name)

        print(f"*****input directory: {str(input_file_directory)}")
        cnetID = config['DEFAULT']['CnetId']
        user_id = input_file_directory.split("/")[1]
        job_id = input_file_directory.split("/")[2]
        bucket = config['s3']['ResultsBucketName']

        print("cnet_id", cnetID)

        print("user_id", user_id)

        print("job_id:", job_id)

        # 1. Upload the results file to S3 results bucket
        for filename in os.listdir(input_file_directory):
            print("filename: ", filename)
            if filename.endswith(".annot.vcf"):
                annotation_file = filename
                full_path = os.path.join(input_file_directory, filename)
                s3_key = f"{cnetID}/{user_id}/{filename}"

                try:
                    upload_file_to_s3(full_path, bucket, s3_key)
                except Exception as e:
                    print("failed to upload annotation file to s3 bucket", "error: ", str(e))

        # 2. Upload the log file to S3 results bucket
        for filename in os.listdir(input_file_directory):
            if filename.endswith(".log"):
                log_file = filename
                full_path = os.path.join(input_file_directory, filename)
                s3_key = f"{cnetID}/{user_id}/{filename}"
                try:
                    upload_file_to_s3(full_path, bucket, s3_key)
                except Exception as e:
                    print("failed to upload log file to s3 bucket","error: ", str(e))

        # 3. Clean up (delete) local job files
        # Assume filenames include the job_id for uniqueness, e.g., job_id~filename.extension
        for filename in os.listdir(input_file_directory):
            if job_id in filename:
                full_path = os.path.join(input_file_directory, filename)
                try:
                    os.remove(full_path)
                    print(f"Successfully cleaned up file {filename}")
                except Exception as e:
                    print(f"Failed to clean up file {filename}", "error:", str(e))



        if annotation_file and log_file:
            try:
                response = ann_table.update_item(Key={'job_id': job_id, "submit_time": submit_time}, 
                                                UpdateExpression="SET job_status = :new_status, s3_results_bucket = :bucket, s3_key_result_file = :result_file, s3_key_log_file = :key_log_file, complete_time = :complete_time",
                                                ExpressionAttributeValues={':new_status': 'COMPLETED',
                                                                           ':bucket': bucket,
                                                                           ':result_file': annotation_file,
                                                                           ':key_log_file': log_file,
                                                                           ':complete_time': int(time.time()),                                                },
                                                ReturnValues="UPDATED_NEW"
                )
            
            except Exception as e:
                print("failed to update database to indicate job completed", "error: ", str(e))            

        else:
            print("Required files are missing: database not updated.")


        # Conditionally send a message to SNS if the user role is free
        try:
            message = {
                'job_id': job_id,
                'submit_time': submit_time,
                'user_id': user_id,
                'status': 'COMPLETED',
                'results_bucket': bucket,
                'result_file_key': f"{cnetID}/{user_id}/{input_file_name}"
            }

            stepfunction_arn = config['stepfunction']['arn']
            start_step_function(stepfunction_arn, message)

        except Exception as e:
            print("we could not send sns mesage for archiving", "error: ", str(e))   

    else:
        print("Incorrect arguments for completing annotation job")

### EOF