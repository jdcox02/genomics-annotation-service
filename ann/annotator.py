import boto3
import json
import os
import subprocess
import configparser


#Read configuration file
#Resource - https://docs.python.org/3/library/configparser.html
config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
config.read('annotator_config.ini')

s3_region_name = config['aws']['AwsRegionName']
annot_table_name = config['gas']['AnnotationsTable']
print("Annot table name:", annot_table_name)



#Resource: 
#https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html

s3 = boto3.client('s3', region_name = s3_region_name)
dynamo = boto3.resource('dynamodb', region_name = s3_region_name)
ann_table = dynamo.Table(annot_table_name)



# Connect to SQS and get the message queue
#Resource: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html

sqs = boto3.client('sqs', region_name= s3_region_name)
queue_url = config['sqs']['QueueUrl']
queue_wait_time = int(config['sqs']['WaitTime'])
queue_max_messages = int(config['sqs']['MaxMessages'])




#Resources: 
#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html#
#https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-long-polling

# Poll the message queue in a loop 
while True:
  
    # Attempt to read the maximum number of messages from the queue
    # Use long polling - DO NOT use sleep() to wait between polls
    
    response = sqs.receive_message(QueueUrl = queue_url, 
                                   MaxNumberOfMessages=queue_max_messages, 
                                   WaitTimeSeconds=queue_wait_time)

    # Process messages received
    #extract job parameters from the message body as before


    messages = response.get('Messages', [])

    for message in messages:
        print(str(message))

        pretty_json = json.dumps(message, indent=4)
        with open('message_output.json', 'w') as file:
            json.dump(pretty_json, file, indent=4)

        receipt_handle = message['ReceiptHandle'] 
        message_body = json.loads(message["Body"])
        job_details = json.loads(message_body["Message"]) 
        key = job_details["s3_key_input_file"]
  

        job_id = job_details["job_id"]
        user_id = job_details["user_id"]
        user_role = job_details["user_role"]
        bucket = job_details["s3_inputs_bucket"]
        key = job_details["s3_key_input_file"]
        submit_time = str(job_details["submit_time"])

        print(f"user role: {user_role}")


        # Include below the same code you used in prior homework
        # Get the input file S3 object and copy it to a local file
        # Use a local directory structure that makes it easy to organize
        # multiple running annotation jobs

        local_dir = os.path.join("jobs", user_id, job_id)
        os.makedirs(local_dir, exist_ok=True)
        local_filename = os.path.join(local_dir, key.split("/")[-1])

        try:
            s3.download_file(bucket, key, local_filename)
        except Exception as e:
            print(f"Failed to download file from s3 with error: {str(e)}")


        try:
            #Add entry into dynamo db - must run before opening subprocess, otherwise condition will fail
            ann_table.update_item(
                Key={'job_id': job_id, "submit_time": int(submit_time)},
                UpdateExpression="SET job_status = :new_status",
                ExpressionAttributeValues={':new_status': 'RUNNING', ':expected_status': 'PENDING'},
                ConditionExpression="job_status = :expected_status")
            print("Status Update: Moving job from pending to running")
    

            # Run the annotator and wait for it to complete
            result = subprocess.run(
                ["python", "run.py", local_filename, submit_time, user_role],
                capture_output=True, text=True
            ) 

            print("Output:", result.stdout) 

            #Resource: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html
            # Delete the message from the queue, if job was successfully submitted


            if result.returncode == 0:
                print("Annotation process completed successfully.")
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            else:
                print(f"Annotation process failed: {result.stderr}")

        except Exception as e:
            print(f"Error processing job {job_id} with error: {str(e)}")


