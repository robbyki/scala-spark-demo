# Databricks notebook source
# MAGIC %md
# MAGIC ## DBR Demo (robbyk)
# MAGIC ### June 27, 2022 9:00 AM EDT

# COMMAND ----------

# MAGIC %python
# MAGIC slides_html="""
# MAGIC <iframe src="https://docs.google.com/presentation/d/e/2PACX-1vQyurC87TZM6lLTHElJCmCF0vdzRg2UBihgr3_K0y9vGkJZ1Hi_cUZMwOdntbQHOu3IfJ4aPvsoxeAZ/embed?start=false&loop=false&delayms=3000" frameborder="0" width="960" height="569" allowfullscreen="true" mozallowfullscreen="true" webkitallowfullscreen="true"></iframe>
# MAGIC """
# MAGIC displayHTML(slides_html)

# COMMAND ----------

# MAGIC %python
# MAGIC aws_bucket_name = "datasets-rk"
# MAGIC mount_name = "demos/rtb"
# MAGIC dbutils.fs.mount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)

# COMMAND ----------

# MAGIC %python
# MAGIC # Our S3 mounted bucket:
# MAGIC display(dbutils.fs.ls("/mnt/demos/rtb/dbr-demo/bidstreams/"))
# MAGIC # same as...
# MAGIC # display(dbutils.fs.ls("s3a://datasets-rk/dbr-demo/bidstreams"))

# COMMAND ----------

# MAGIC %python
# MAGIC # Unfortunataly until we figure out the AccessDenied issue with DLT from S3 we'll have t use local file upload
# MAGIC # dbfs cp bidstream-sample-final.json dbfs:/mnt/demos/datasets/
# MAGIC display(dbutils.fs.ls("/mnt/demos/datasets/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using Autoloader for streaming json data from cloud storage (or mnt location for now)

# COMMAND ----------

# MAGIC %md
# MAGIC ![my_test_image](https://s3.us.cloud-object-storage.appdomain.cloud/robbyk/links/medallion-arch.png?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=3173d748167f47798425cccbd10f9b09%2F20220617%2Fus-standard%2Fs3%2Faws4_request&X-Amz-Date=20220617T152554Z&X-Amz-Expires=604800&X-Amz-SignedHeaders=host&X-Amz-Signature=424c3a94a85c3921a7cb55274cf73e46f8b9191b870f6fdd6ec24573e61000ec)

# COMMAND ----------

# #this gives me java.nio.file.AccessDeniedException: s3a://datasets-rk/dbr-demo/bidstreams/bidstream-sample.json
# import dlt
# from pyspark.sql.functions import explode, col
# @dlt.table
# def rtb_dlt_bids_bronze():
#     return (
#         spark.read.format("json")
#         .option("multiLine", "true")
#         .option("inferSchema", "true")
#         .load("/mnt/demos/rtb/dbr-demo/bidstreams/bidstream-sample.json"))
# #         .load("/mnt/demos/datasets/"))

# COMMAND ----------

# Bronze Table
import dlt
from pyspark.sql.functions import explode, col


@dlt.table(name="bids_bronze", comment="raw bidrequest dataset")
def rtb_dlt_bids_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", "dbfs:/auto-loader/schemas/")
        .option("includeExistingFiles", "true")
        .option("multiLine", "true")
        .option("inferSchema", "true")
        .load("/mnt/demos/datasets/")
    )


# COMMAND ----------

# Silver Device Table
@dlt.table(name="bids_device_silver", comment="device cleanup table")
def rtb_dlt_bids_device_silver():
    # since we read the bronze table as a stream, this silver table is also updated incrementally
    df = (
        dlt.read_stream("bids_bronze")
        .select(col("id").alias("auction_id"), "device.*")
        .select("*", "geo.*")
        .drop("geo")
    )
    return df.select([col(c).alias("device_" + c) for c in df.columns])


# COMMAND ----------

# Silver Imps Table
@dlt.table(name="bids_imp_silver", comment="cleanup of imps silver quality")
def rtb_dlt_bids_imp_silver():
    df = (
        dlt.read_stream("bids_bronze")
        .withColumn("imp", explode("imp"))
        .select(col("id").alias("auction_id"), "imp.*")
    )  #     .select("*", col("banner.h").alias("banner_h"), col("banner.pos").alias("banner_pos"), col("banner.w").alias("banner_w"))\
    #     .drop("banner")
    return df.select([col(c).alias("imp_" + c) for c in df.columns])


# COMMAND ----------

# Silver Site Table
@dlt.table(name="bids_site_silver")
def rtb_dlt_bids_site_silver():
    df = dlt.read_stream("bids_bronze").select(
        col("id").alias("auction_id"),
        col("ext.ads_txt.status").alias("gdpr_status"),
        "app.*",
    )
    return df.select([col(c).alias("site_" + c) for c in df.columns])


# COMMAND ----------

# Gold Bids Table
@dlt.table(name="bids_gold", comment="filter out non-gdpr compliance bid requests")
# @dlt.expect_or_drop("valid_gdpr_status", "site_gdpr_status IS NOT NULL AND site_gdpr_status > 0")
def rtb_dlt_bids_gold():
    df = spark.sql(
        """SELECT *, round(rand()+0.1) as in_view
        FROM (
            SELECT * FROM rtb_demo.bids_device_silver A
            LEFT JOIN rtb_demo.bids_imp_silver B ON A.device_auction_id = B.imp_auction_id 
        ) D 
        LEFT JOIN rtb_demo.bids_site_silver C on D.device_auction_id = C.site_auction_id """
    )
    return df


# COMMAND ----------

# df.writeStream.option("mergeSchema", "true")
#     .option("checkpointLocation", "/mnt/demos/rtb/dbr-demo/checkpoint/")
#     .start("/mnt/demos/rtb/dbr-demo/bidstreams/out/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Sharing Demo (depending on time)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rtb_demo.bids_device_silver limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rtb_demo.bids_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rtb_demo.bids_site_silver limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rtb_demo.bids_imp_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rtb_demo.bids_bronze limit 10
