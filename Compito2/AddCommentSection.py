###### TEDx Add comment section
######

import sys
import json
import pyspark
from pyspark.sql.functions import *

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session
    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

mongo_uri = "mongodb://cluster0-shard-00-00.rzscp.mongodb.net:27017,cluster0-shard-00-01.rzscp.mongodb.net:27017,cluster0-shard-00-02.rzscp.mongodb.net:27017"

connection_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2021",
    "collection": "tedx_data",
    "username": "admin",
    "password": "SXcYV2h2q1rLjz80",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame

tedx_dataset = glueContext.create_dynamic_frame.from_options(connection_type="mongodb", connection_options=connection_mongo_options).toDF()
tedx_dataset = tedx_dataset.withColumn("comments",array(struct(lit(" ").alias("id_user"),lit(" ").alias("comment"))))

tedx_dataset.printSchema()

tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=connection_mongo_options)

print("Completed!")


