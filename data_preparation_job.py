import sys
import pyspark
import boto3

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job


# creating Spark and Glue context
sc = pyspark.SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# logic
client = boto3.client('s3')
s3_resource = boto3.resource('s3')

bucket_objects = client.list_objects_v2(Bucket='source-panini-333-bucket')
keys = [obj['Key'] for obj in bucket_objects['Contents']]

for key in keys:
    copy_source = {
        'Bucket': 'source-panini-333-bucket',
        'Key': key
    }
    s3_resource.Object('storage-panini-333-bucket', key).copy(copy_source)

# end of job
job.commit()
