import logging
import zipfile
import boto3
import gzip
import io
from io import BytesIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_resource = boto3.resource('s3')

def unzip_and_upload_files(filekey, sourcebucketname, destinationbucketname):
    try:
        logger.info(f"Processing file: {filekey}")

        zipped_file = s3_resource.Object(bucket_name=sourcebucketname, key=filekey)
        buffer = BytesIO(zipped_file.get()["Body"].read())
        zipped = zipfile.ZipFile(buffer)
        
        for file in zipped.namelist():
            print(f'Current file in zipfile: {file}')
            
            final_file_path = f"csv-data/{file}"  
            
            with zipped.open(file, "r") as f_in:
                destinationbucket = s3_resource.Bucket(destinationbucketname)
                
                destinationbucket.upload_fileobj(
                    io.BytesIO(f_in.read()),  
                    final_file_path,
                    ExtraArgs={"ContentType": "text/plain"}  
                )

        folder_name = filekey.split('/')[1].split('.')[0]
        #return the folder_name that triggered the lambda function
        return folder_name
        
    except Exception as e:
        print(e)
        
def lambda_handler(event, context):
    try:

        s3_cli = boto3.client('s3')
        sourcebucketname = 'your-bucket'
        destination_bucket = 'your-bucket2'
        #source_suffix = '/data'
        #sourcebucketname = f'{base_sourcebucketname}{source_suffix}'

        key = event['Records'][0]['s3']['object']['key']
        
        folder_name = unzip_and_upload_files(key, sourcebucketname, destination_bucket)
        print(folder_name)
        
        glue_job_name = 'job-name'
        
        # Set up the Glue client
        glue_client = boto3.client('glue')
        
        glue_job_params = {
            '--folder_name': folder_name
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

