###### TEDx-Load-Aggregate-Model
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




##### FROM FILES
tedx_dataset_path = "s3://thumchan-tedx-data-2021/tedx_dataset.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("multiline","true") \
    .option("escape", "\"") \
    .csv(tedx_dataset_path)
    
tedx_dataset.printSchema()


#### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}") 
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")



## READ TAGS DATASET
tags_dataset_path = "s3://thumchan-tedx-data-2021/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)

## READ WATCH NEXT
watch_next_dataset_path = "s3://thumchan-tedx-data-2021/watch_next_dataset.csv"
watch_next_dataset = spark.read \
    .option("header","true") \
    .option("multiline","true") \
    .csv(watch_next_dataset_path)
watch_next_dataset = watch_next_dataset.dropDuplicates(["idx","watch_next_idx"]) ## Delete duplicated raws


# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")) \
    .agg(collect_list("tag").alias("tags")) 
tags_dataset_agg.printSchema()


watch_next_dataset_agg = watch_next_dataset.groupBy(col("idx").alias("idx_ref")) \
    .agg(collect_list(struct("watch_next_idx", "url")).alias("watch_next"))
#.agg(collect_list("watch_next_idx").alias("watch_next"),collect_list("url").alias("url_next"))
watch_next_dataset_agg.printSchema()
watch_next_dataset_agg.show(truncate=False)

tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref")
tedx_dataset_agg = tedx_dataset_agg.join(watch_next_dataset_agg, tedx_dataset_agg.idx == watch_next_dataset_agg.idx_ref, "left")\
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx")

tedx_dataset_agg.printSchema()

print("Join process done!")



mongo_uri = "mongodb://cluster0-shard-00-00.rzscp.mongodb.net:27017,cluster0-shard-00-01.rzscp.mongodb.net:27017,cluster0-shard-00-02.rzscp.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2021",
    "collection": "tedx_data",
    "username": "admin",
    "password": "SXcYV2h2q1rLjz80",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)