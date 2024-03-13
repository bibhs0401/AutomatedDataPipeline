import logging
import zipfile
import boto3
import gzip
import io
from io import BytesIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# s3_resource = boto3.resource('s3')

def lambda_handler(event, context):
    try:

        # s3_cli = boto3.client('s3')
        # sourcebucketname = 'bibhusha-demo-bucket'
        # destination_bucket = 'bibhusha-demo-bucket2'
        #source_suffix = '/data'
        #sourcebucketname = f'{base_sourcebucketname}{source_suffix}'

        key = event['Records'][0]['s3']['object']['key']
        # folder_name = key.split('/')[1].split('.')[0]

        # folder_name = unzip_and_upload_files(key, sourcebucketname, destination_bucket)
        # print(folder_name)
        
        glue_job_name = 'bibhusha_unzip_files'
        
        # Set up the Glue client
        glue_client = boto3.client('glue')
        
        glue_job_params = {
            '--folder_name': key
         }

        # Trigger the Glue job
        response = glue_client.start_job_run(JobName=glue_job_name, Arguments=glue_job_params)

        # Log the run ID for reference
        run_id = response['JobRunId']
        logger.info(f"Started Glue job run with ID: {run_id}")

        return {
            'statusCode': 200,
            'body': f"Started Glue job run with ID: {run_id}"
        }
    except Exception as e:
        logger.error(f"Error triggering Glue job: {e}")
        return {
            'statusCode': 500,
            'body': f"Error triggering Glue job: {e}"
        }
