import sys
import boto3
import io 
from io import BytesIO
import zipfile
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import *

## @params: [JOB_NAME, key]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'key'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

key = args['key']

# Create S3 resource
s3_resource = boto3.resource('s3')

sourcebucketname = 'bibhusha-demo-bucket'
destinationbucketname = 'bibhusha-demo-bucket2'

s3_path = f"s3://{sourcebucketname}/data/"

# List objects in the sourcebucketname/data/ directory
objects = s3_resource.Bucket(sourcebucketname).objects.filter(Prefix='data/')

for obj in objects:
    if obj.key.endswith('.zip'):
        print(f'Processing file: {obj.key}')
        
        zipped_file = s3_resource.Object(bucket_name=sourcebucketname, key=obj.key)
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

job.commit()
