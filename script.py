import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
import pandas as pd
import numpy as np

## @params: [JOB_NAME, folder_name]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'folder_name'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

folder_name = args['folder_name']

#1. read all the csv files into a data frame
s3_path = f"s3://bibhusha-demo-bucket2/csv-data/{folder_name}/"

'''
folder_name is the folder that triggered the lambda function. 
Doing this prevents from traversing through all the folders in the s3 bucket.
'''
df = spark.read.option("header", True).csv(s3_path)

#2. Tranform the data 

''' 
schema: 
ID: string (nullable = true)
Name: string (nullable = true)
Age: string (nullable = true)
Gender: string (nullable = true)
Occupation: string (nullable = true)
Salary: string (nullable = true)
Country: string (nullable = true)
Status: string (nullable = true)
'''

# type casting
df = df.withColumn("ID",col("ID").cast(IntegerType())) \
    .withColumn("Age",col("Age").cast(IntegerType())) 
    
''' 
resulting schema: 
[ID: int, Name: string, Age: int, Gender: string, Occupation: string, Salary: string, Country: string, Status: string]
''' 

# load into redshift

# convert DataFrame to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")

my_conn_options = {
        "dbtable": "public.customer",
        "database": "dev"
        }

redshift_node = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dynamic_frame,
    catalog_connection = "bibhusha-redshift-connection",
    connection_options= my_conn_options,
    redshift_tmp_dir ="s3://aws-glue-assets-658349184098-us-west-2/temporary/",
    transformation_ctx="redshift_node"
)

job.commit()