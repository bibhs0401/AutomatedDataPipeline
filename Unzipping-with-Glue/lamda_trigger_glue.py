import logging
import zipfile
import boto3
import io
from io import BytesIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:

        # s3_cli = boto3.client('s3')
        # sourcebucketname = 'bibhusha-demo-bucket'
        # destination_bucket = 'bibhusha-demo-bucket2'
        #source_suffix = '/data'
        #sourcebucketname = f'{base_sourcebucketname}{source_suffix}'

        key = event['Records'][0]['s3']['object']['key']
        
        # folder_name = unzip_and_upload_files(key, sourcebucketname, destination_bucket)
        # print(folder_name)
        
        glue_job_name = 'bibhusha-load-redshift'
        
        # Set up the Glue client
        glue_client = boto3.client('glue')
        
        # glue_job_params = {
        #     '--folder_name': folder_name
        #  }

        # Trigger the Glue job
        response = glue_client.start_job_run(JobName=glue_job_name)

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