import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'export_s3_path'])
s3_path = args['export_s3_path']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "user_database", table_name = "user_us", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "user_database", table_name = "user_us", transformation_ctx = "datasource0")
filtered_source = Filter.apply(frame = datasource0,
                              f = lambda x: x["country"] == "China")
## @type: ApplyMapping
## @args: [mapping = [("street_address", "string", "street_address", "string"), ("country", "string", "country", "string"), ("city", "string", "city", "string"), ("sex", "string", "sex", "string"), ("last_name", "string", "last_name", "string"), ("last_updater_region", "string", "last_updater_region", "string"), ("zipcode", "string", "zipcode", "string"), ("last_update_timestamp", "double", "last_update_timestamp", "double"), ("govid", "string", "govid", "string"), ("state", "string", "state", "string"), ("pk", "string", "pk", "string"), ("first_name", "string", "first_name", "string"), ("email", "string", "email", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = filtered_source, mappings = [("street_address", "string", "street_address", "string"), ("country", "string", "country", "string"), ("city", "string", "city", "string"), ("sex", "string", "sex", "string"), ("last_name", "string", "last_name", "string"), ("last_updater_region", "string", "last_updater_region", "string"), ("zipcode", "string", "zipcode", "string"), ("last_update_timestamp", "double", "last_update_timestamp", "double"), ("govid", "string", "govid", "string"), ("state", "string", "state", "string"), ("pk", "string", "pk", "string"), ("first_name", "string", "first_name", "string"), ("email", "string", "email", "string")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://zoe-ssm-output", "compression": "gzip"}, format = "json", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": s3_path, "compression": "gzip"}, format = "json", transformation_ctx = "datasink2")
job.commit()
