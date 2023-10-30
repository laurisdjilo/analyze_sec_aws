import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
s3_client = boto3.Session().client("s3")

args = getResolvedOptions(sys.argv, ["database", "table_name", "table_s3a_location", "s3_bucket", "raw_s3_prefix", \
    "write_mode", "cluster_table", "num_clusters", "cluster_cols", "partition_by_file_year", "archive_s3_prefix"])

data_frame = spark.read.format("csv").option("header", "true").option("sep", "\t").load("s3://{}/{}".format(args["s3_bucket"], args["raw_s3_prefix"]))

data_frame = data_frame.withColumn("original_file_name", F.element_at(F.split(F.input_file_name(),"/"), -1))
if args["partition_by_file_year"] == "true":
    data_frame = data_frame.withColumn("original_file_year", F.element_at(F.split(F.col("original_file_name"), "q"), 1))

data_frame.cache()

file_names = data_frame.select("original_file_name").distinct().rdd.flatMap(lambda x: x).collect()

# The parameter --enable-glue-datacatalog should be set to "true". It allows us to write into the Glue Datacolog using the Spark Session
if spark.catalog._jcatalog.tableExists(args["database"], args["table_name"]):
    # Ensure that the data has not been written before
    if args["partition_by_file_year"] == "true":
        tmp = spark.sql(
                        """
                        SELECT DISTINCT original_file_name AS existing_file_name
                        FROM {}
                        WHERE original_file_year IN ({})
                            AND original_file_name IN ({})
                        GROUP BY original_file_name
                        """
                        .format(
                            args["database"]+"."+args["table_name"]
                            , ",".join(set(map(lambda x: "'"+x.split("q")[0]+"'", file_names)))
                            , ",".join(map(lambda x: "'"+x+"'", file_names))
                        )
                    )
    else:
        tmp = spark.sql(
                        """
                        SELECT DISTINCT original_file_name AS existing_file_name
                        FROM {}
                        WHERE original_file_name IN ({})
                        GROUP BY original_file_name
                        """
                        .format(
                            args["database"]+"."+args["table_name"]
                            , ",".join(map(lambda x: "'"+x+"'", file_names))
                        )
                    )
    # To take only records whose file_name do not match any existing file_name.
    data_frame = data_frame.join(F.broadcast(tmp), data_frame.original_file_name == tmp.existing_file_name, "left_anti")
    
    data_frame.write.insertInto(args["database"]+"."+args["table_name"])
else:
    if args["partition_by_file_year"] == "true" and args["cluster_table"] == "true":
        data_frame.write.option("path", args["table_s3a_location"])\
            .format("parquet").mode(args["write_mode"])\
            .partitionBy("original_file_year")\
            .bucketBy(int(args["num_clusters"]), args["cluster_cols"].split(","))\
            .saveAsTable(args["database"]+"."+args["table_name"])
    elif args["partition_by_file_year"] == "true" and args["cluster_table"] == "false":
        data_frame.write.option("path", args["table_s3a_location"])\
            .format("parquet").mode(args["write_mode"])\
            .partitionBy("original_file_year")\
            .saveAsTable(args["database"]+"."+args["table_name"])
    elif args["partition_by_file_year"] == "false" and args["cluster_table"] == "true":
        data_frame.write.option("path", args["table_s3a_location"])\
            .format("parquet").mode(args["write_mode"])\
            .bucketBy(int(args["num_clusters"]), args["cluster_cols"].split(","))\
            .saveAsTable(args["database"]+"."+args["table_name"])
    else:
        data_frame.write.option("path", args["table_s3a_location"])\
            .format("parquet").mode(args["write_mode"])\
            .saveAsTable(args["database"]+"."+args["table_name"])

# Archive the source data files
for file_name in file_names:
    s3_client.copy_object(Bucket=args["s3_bucket"], Key=args["archive_s3_prefix"]+file_name, CopySource={'Bucket': args["s3_bucket"], 'Key': args["raw_s3_prefix"]+file_name})
    s3_client.delete_object(Bucket=args["s3_bucket"], Key=args["raw_s3_prefix"]+file_name)

