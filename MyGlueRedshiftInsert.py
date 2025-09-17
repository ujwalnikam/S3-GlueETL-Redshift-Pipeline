import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1758082660402 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://my-glue-etl-project2025/parquet_output/"], "recurse": True}, transformation_ctx="AmazonS3_node1758082660402")

# Script generated for node Change Schema
ChangeSchema_node1758082666475 = ApplyMapping.apply(frame=AmazonS3_node1758082660402, mappings=[("new_year", "string", "year", "string"), ("cnt", "long", "nooncustomer", "BIGINT"), ("qty", "long", "quantity", "BIGINT")], transformation_ctx="ChangeSchema_node1758082666475")

# Script generated for node Amazon Redshift
AmazonRedshift_node1758082669434 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1758082666475, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://my-glue-etl-project2025/temp/", "useConnectionProperties": "true", "dbtable": "public.product_tab_def", "connectionName": "My-Redshift-connection", "preactions": "CREATE TABLE IF NOT EXISTS public.product_tab_def (year VARCHAR, nooncustomer BIGINT, quantity BIGINT);"}, transformation_ctx="AmazonRedshift_node1758082669434")

job.commit()