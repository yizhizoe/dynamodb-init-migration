import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','target_ddb_table_name'])
target_ddb_table_name = args['target_ddb_table_name']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "json_s3", table_name = "json_ddb_from_oregon", transformation_ctx = "datasource0")

applymapping0 = ApplyMapping.apply(
    frame=datasource0,
    mappings=[
        ("street_address", "string", "street_address", "string"), 
        ("country", "string", "country", "string"), 
        ("city", "string", "city", "string"), 
        ("sex", "string", "sex", "string"), 
        ("last_name", "string", "last_name", "string"), 
        ("last_updater_region", "string", "last_updater_region", "string"), 
        ("zipcode", "string", "zipcode", "string"), 
        ("last_update_timestamp", "double", "last_update_timestamp", "double"), 
        ("govid", "string", "govid", "string"), 
        ("state", "string", "state", "string"), 
        ("pk", "string", "PK", "string"), 
        ("first_name", "string", "first_name", "string"), 
        ("email", "string", "email", "string")
    ],
    transformation_ctx="applymapping0",
)

glueContext.write_dynamic_frame_from_options(frame=applymapping0,connection_type="dynamodb",
    connection_options={
        "dynamodb.region": "cn-north-1",
        "dynamodb.output.tableName": target_ddb_table_name
    }
)
job.commit()
