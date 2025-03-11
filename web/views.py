# views.py
#
# Copyright (C) 2015-2023 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = "Vas Vasiliadis <vas@uchicago.edu>"

import uuid
import time
import json
from datetime import datetime

import boto3
from botocore.client import Config
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from flask import abort, flash, redirect, render_template, request, session, url_for, jsonify

from app import app, db
from decorators import authenticated, is_premium


import pytz


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""

#Resource: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
dynamo = boto3.resource('dynamodb', region_name = 'us-east-1')



#Resource: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/table/index.html
ann_table = dynamo.Table(app.config["AWS_DYNAMODB_ANNOTATIONS_TABLE"])


sns = boto3.client('sns', region_name='us-east-1')


@app.route("/annotate", methods=["GET"])
@authenticated
def annotate():
    # Open a connection to the S3 service
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )

    inputs_bucket_name = app.config["AWS_S3_INPUTS_BUCKET"]
    user_id = session['primary_identity']

    print("Session Data:", dict(session))

    print(f"***********************user id: {user_id}")

    # Generate unique ID to be used as S3 key (name)
    key_name = (
        app.config["AWS_S3_KEY_PREFIX"]
        + user_id
        + "/"
        + str(uuid.uuid4())
        + "~${filename}"
    )

    print(f"***********************key name: {key_name}")


    # Create the redirect URL
    redirect_url = str(request.url) + "/job"

    # Define policy conditions
    encryption = app.config["AWS_S3_ENCRYPTION"]
    acl = app.config["AWS_S3_ACL"]
    fields = {
        "success_action_redirect": redirect_url,
        "x-amz-server-side-encryption": encryption,
        "acl": acl,
        "csrf_token": app.config["SECRET_KEY"],
    }
    conditions = [
        ["starts-with", "$success_action_redirect", redirect_url],
        {"x-amz-server-side-encryption": encryption},
        {"acl": acl},
        ["starts-with", "$csrf_token", ""],
    ]

    # Generate the presigned POST call
    try:
        presigned_post = s3.generate_presigned_post(
            Bucket=inputs_bucket_name,
            Key=key_name,
            Fields=fields,
            Conditions=conditions,
            ExpiresIn=app.config["AWS_SIGNED_REQUEST_EXPIRATION"],
        )
    except ClientError as e:
        app.logger.error(f"Unable to generate presigned URL for upload: {e}")
        return abort(500)

    # Render the upload form which will parse/submit the presigned POST
    return render_template(
        "annotate.html", s3_post=presigned_post, role=session["role"]
    )


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""


@app.route("/annotate/job", methods=["GET"])
def create_annotation_job_request():

    try:
        region = app.config["AWS_REGION_NAME"]

        # Parse redirect URL query parameters for S3 object info
        s3_inputs_bucket = request.args.get("bucket")
        key = request.args.get("key")

        print(f"Received key: {key}")

        # Extract the job ID from the S3 key
        cnet_id = key.split('/')[0]
        user_id = key.split('/')[1]
        job_id = key.split('/')[2].split('~')[0]
        input_file_name = key.split('~')[-1]
        submit_time = int(time.time())
        job_status = "PENDING"

        print(f"cnet_id: {cnet_id}")
        print(f"user_id: {user_id}")
        print(f"job_id: {job_id}")

    except Exception as e:
        # Print the error with the key to understand what went wrong
        print(f"Error processing key '{key}': {str(e)}")
        return abort(500)



    # Persist job to database
    # Move your code here...

    # Send message to request queue
    # Move your code here...

    if session['role'] == 'free_user':
        archive_status = "PENDING"
    else:
        archive_status = ""


    data = {
    "job_id": job_id,
    "user_id": user_id,
    "input_file_name": input_file_name,
    "s3_inputs_bucket": s3_inputs_bucket,
    "s3_key_input_file": key,
    "submit_time": submit_time,
    "job_status": job_status,
    "archive_status": archive_status
    }

    print(data)

    #Saving job to Dynamo

    try:
        #Resource: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/put_item.html
        ann_table.put_item(Item=data)

    except Exception as e:
        abort(500)

    # Publishing job to topic
    #Resource: https://docs.aws.amazon.com/sns/latest/dg/sns-publishing.html

    topic_arn = app.config["AWS_SNS_JOB_REQUEST_TOPIC"]


    # Updating data to also include user role here - I don't want it stored in the annotations
    # table above because the role can change
    data['user_role'] = session['role']

    try: 
        sns.publish(TopicArn=topic_arn,
                    Message=json.dumps({'default': json.dumps(data)}),
                    MessageStructure='json'
        )

        return render_template("annotate_confirm.html", job_id=job_id)

    except Exception as e:
        abort(500)


"""List all annotations for the user
"""


@app.route("/annotations", methods=["GET"])
@authenticated
def annotations_list():

    #get user_id
    user_id = session['primary_identity']


    #Get list of annotations to display

    try: 
        #Resource: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/query.html
        response = ann_table.query(
        IndexName = 'user_id_index',
        KeyConditionExpression="user_id = :userid",
        ExpressionAttributeValues={
            ':userid': user_id
            }
        )
        jobs = response['Items']
    except Exception as e:
        print(f"Error querying dynamo: {e}")
        abort(500)

    #Resource: https://medium.com/@HeCanThink/pytzdata-travelling-in-different-timezone-made-easy-%EF%B8%8F-30dd95a889ce#:~:text=The%20name%20%E2%80%9Cpytz%E2%80%9D%20is%20an,end%20of%20daylight%20saving%20time.
    #Resource: https://pypi.org/project/pytz/
    cst_tz = pytz.timezone('America/Chicago')
    for job in jobs:
        job['submit_time'] = datetime.fromtimestamp(int(job['submit_time'])).astimezone(cst_tz).strftime('%Y-%m-%d @ %H:%M:%S')


    return render_template("annotations.html", annotations=jobs)


"""Display details of a specific annotation job
"""


@app.route("/annotations/<id>", methods=["GET"])
@authenticated
def annotation_details(id):

    # Open a connection to the S3 service
    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )

    print(str(id))
    try:
        response = ann_table.query(
            KeyConditionExpression=Key('job_id').eq(id)
        )

        job = response['Items'][0]
        username = app.config["AWS_S3_KEY_PREFIX"]


        cst_tz = pytz.timezone('America/Chicago')
        job['submit_time'] = datetime.fromtimestamp(int(job['submit_time'])).astimezone(cst_tz).strftime('%Y-%m-%d @ %H:%M:%S')

        if job['job_status'] == 'COMPLETED':
            job['complete_time'] = datetime.fromtimestamp(
                int(job['complete_time'])
            ).astimezone(cst_tz).strftime('%Y-%m-%d @ %H:%M:%S')

            results_bucket_name = app.config["AWS_S3_RESULTS_BUCKET"]
            result_key_name = f"{username}{job['user_id']}/{job['s3_key_result_file']}"

            job['result_file_url'] = s3.generate_presigned_url('get_object', 
                                                    Params = {
                                                    'Bucket': results_bucket_name, 
                                                    'Key': result_key_name
                                                    }, 
                                                    ExpiresIn=3600)
            print("results key name", result_key_name)


        # Check if the user_id associated with the job matches the current user
        if job['user_id'] != session['primary_identity']:
            # Return 403 Forbidden if the user_id doesn't match
            abort(403, "You are not authorized to access this job.")



        inputs_bucket_name = app.config["AWS_S3_INPUTS_BUCKET"]
        input_key_name = f"{job['s3_key_input_file']}"


        job['input_file_url'] = s3.generate_presigned_url('get_object', 
                                                            Params = {
                                                            'Bucket': inputs_bucket_name, 
                                                            'Key': input_key_name
                                                            }, 
                                                            ExpiresIn=3600)

        print("input key name", input_key_name)

        print("input_url", str(job['input_file_url']))



    except Exception as e:
        print(f"Error querying dynamo or generating URLs: {e}")
        abort(500)

    return render_template('annotation.html', job=job)


"""Display the log file contents for an annotation job
"""


@app.route("/annotations/<id>/log", methods=["GET"])
def annotation_log(id):

    s3 = boto3.client(
        "s3",
        region_name=app.config["AWS_REGION_NAME"],
        config=Config(signature_version="s3v4"),
    )

    try:
        response = ann_table.query(
            KeyConditionExpression=Key('job_id').eq(id)
        )

        job = response['Items'][0]

        if job['user_id'] != session['primary_identity']:
            abort(403, "You are not authorized to access this job.")


        username = app.config["AWS_S3_KEY_PREFIX"]


        results_bucket_name = app.config["AWS_S3_RESULTS_BUCKET"]
        log_key_name = f"{username}{job['user_id']}/{job['s3_key_log_file']}"

        print(results_bucket_name)
        print(log_key_name)


        log_file = s3.get_object(Bucket=results_bucket_name, Key=log_key_name)
        log_contents = log_file['Body'].read().decode('utf-8')
        return render_template('view_log.html', log_contents=log_contents, job_id = id)


    except Exception as e:
        error_code = e.response['Error']['Code'] if hasattr(e, 'response') else 'No response'
        error_message = e.response['Error']['Message'] if hasattr(e, 'response') else str(e)
        app.logger.error(f"Error fetching log file from {results_bucket_name}/{log_key_name}: {error_code} - {error_message}")
        abort(500, f"Failed to retrieve log file: {error_message}")


def restore_from_glacier(user_id):
    thaw_arn = app.config["AWS_SNS_THAW_REQUEST_TOPIC"]

    try:
        #Get jobs where results_file_archive_id is listed
        response = ann_table.query(
            IndexName='user_id_index',
            KeyConditionExpression=Key('user_id').eq(user_id),
            FilterExpression="attribute_exists(results_file_archive_id)"
        )
        archived_jobs = response['Items']
        print(f"archived_jobs retrieved: {response['Items']}")

        #For each archived job, restore from glacier
        for job in archived_jobs:
            sns.publish(
                TopicArn = thaw_arn,
                Message = json.dumps({
                    'user_id': user_id,
                    'job_id': job['job_id'],
                    'submit_time': int(job['submit_time']),
                    'archive_id': job['results_file_archive_id'],
                    's3_key_result_file': job['s3_key_result_file']
                    })
                )
            print(f"Sent thaw request to sns for job: {job['job_id']}")

    except ClientError as e:
        app.logger.error(f"Error initiating Glacier restore for user {user_id}: {e}")
        raise

def update_pending_archive_status(user_id):
    try:
        # Scan the entire table and filter items with user_id and archive_status as "PENDING"
        response = ann_table.scan(
            FilterExpression=Key('user_id').eq(user_id) & Key('archive_status').eq('PENDING')
        )
        
        for item in response.get('Items', []):
            ann_table.update_item(
                Key={'job_id': item['job_id'], 'submit_time': item['submit_time']},
                UpdateExpression="SET archive_status = :archive_status",
                ExpressionAttributeValues={':archive_status': ''}
            )
            print(f"Updated archive_status for job_id {item['job_id']} to blank")
                
    except Exception as e:
        print(f"Error updating archive_status: {str(e)}")


"""Subscription management handler
"""
import stripe
from auth import update_profile


@app.route("/subscribe", methods=["GET"])
def subscribe():
    # Force-upgrade the user to premium
    update_profile(identity_id=session['primary_identity'], role='premium_user')
    
    # Update role in the session
    session['role'] = 'premium_user'
    
    # Initiate restoration of the user’s data from Glacier
    restore_from_glacier(session['primary_identity'])
    
    # Update archive_status to blank for any "PENDING" jobs
    update_pending_archive_status(session['primary_identity'])
    
    # Render the confirmation page
    return render_template("subscribe_confirm.html", stripe_id="forced_upgrade")




"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Set premium_user role
"""




@app.route("/make-me-premium", methods=["GET"])
@authenticated
def make_me_premium():
    # Hacky way to set the user's role to a premium user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="premium_user")
    return redirect(url_for("profile"))


"""Reset subscription
"""


@app.route("/unsubscribe", methods=["GET"])
@authenticated
def unsubscribe():
    # Hacky way to reset the user's role to a free user; simplifies testing
    update_profile(identity_id=session["primary_identity"], role="free_user")
    return redirect(url_for("profile"))


"""Home page
"""


@app.route("/", methods=["GET"])
def home():
    return render_template("home.html"), 200


"""Login page; send user to Globus Auth
"""


@app.route("/login", methods=["GET"])
def login():
    app.logger.info(f"Login attempted from IP {request.remote_addr}")
    # If user requested a specific page, save it session for redirect after auth
    if request.args.get("next"):
        session["next"] = request.args.get("next")
    return redirect(url_for("authcallback"))


"""404 error handler
"""


@app.errorhandler(404)
def page_not_found(e):
    return (
        render_template(
            "error.html",
            title="Page not found",
            alert_level="warning",
            message="The page you tried to reach does not exist. \
      Please check the URL and try again.",
        ),
        404,
    )


"""403 error handler
"""


@app.errorhandler(403)
def forbidden(e):
    return (
        render_template(
            "error.html",
            title="Not authorized",
            alert_level="danger",
            message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party.",
        ),
        403,
    )


"""405 error handler
"""


@app.errorhandler(405)
def not_allowed(e):
    return (
        render_template(
            "error.html",
            title="Not allowed",
            alert_level="warning",
            message="You attempted an operation that's not allowed; \
      get your act together, hacker!",
        ),
        405,
    )


"""500 error handler
"""


@app.errorhandler(500)
def internal_error(error):
    return (
        render_template(
            "error.html",
            title="Server error",
            alert_level="danger",
            message="The server encountered an error and could \
      not process your request.",
        ),
        500,
    )


"""CSRF error handler
"""


from flask_wtf.csrf import CSRFError


@app.errorhandler(CSRFError)
def csrf_error(error):
    return (
        render_template(
            "error.html",
            title="CSRF error",
            alert_level="danger",
            message=f"Cross-Site Request Forgery error detected: {error.description}",
        ),
        400,
    )


### EOF
