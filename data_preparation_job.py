import sys
import pyspark
import boto3

from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql import Row
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job


# creating Spark and Glue context
sc = pyspark.SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

order_list = [
               ['1005', '623', 'YES', '1418901234', '75091'],\
               ['1006', '547', 'NO', '1418901256', '75034'],\
               ['1007', '823', 'YES', '1418901300', '75023'],\
               ['1008', '912', 'NO', '1418901400', '82091'],\
               ['1009', '321', 'YES', '1418902000', '90093']\
             ]

# Define schema for the order_list
order_schema = StructType([  
                      StructField("order_id", StringType()),
                      StructField("customer_id", StringType()),
                      StructField("essential_item", StringType()),
                      StructField("timestamp", StringType()),
                      StructField("zipcode", StringType())
                    ])

# Create a Spark Dataframe from the python list and the schema
df_orders = spark.createDataFrame(order_list, schema = order_schema)
df_orders.show()

dyf_orders = DynamicFrame.fromDF(df_orders, glueContext, "dyf")
dyf_applyMapping = ApplyMapping.apply( frame = dyf_orders, mappings = [ 
  ("order_id","String","order_id","Long"), 
  ("customer_id","String","customer_id","Long"),
  ("essential_item","String","essential_item","String"),
  ("timestamp","String","timestamp","Long"),
  ("zipcode","String","zip","Long")
])

dyf_applyMapping.printSchema()

# logic
# client = boto3.client('s3')
# s3_resource = boto3.resource('s3')

# bucket_objects = client.list_objects_v2(Bucket='source-panini-333-bucket')
# keys = [obj['Key'] for obj in bucket_objects['Contents']]

# for key in keys:
#     copy_source = {
#         'Bucket': 'source-panini-333-bucket',
#         'Key': key
#     }
#     s3_resource.Object('storage-panini-333-bucket', key).copy(copy_source)

# end of job
job.commit()
