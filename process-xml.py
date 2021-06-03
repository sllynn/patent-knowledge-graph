# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


landing_zone_path = dbutils.widgets.get("landingZonePath")
bronze_table_name = dbutils.widgets.get("bronzeTableName")
bronze_table_path = dbutils.widgets.get("bronzeTablePath")
dbutils.fs.mkdirs(bronze_table_path)

# COMMAND ----------

full_text_description_schema = \
StructType([
  StructField("us-bibliographic-data-grant", StructType([
    StructField("application-reference", StructType([
      StructField("document-id", StructType([
        StructField("country", StringType()),
        StructField("date", LongType()),
        StructField("doc-number", LongType())
      ]))
    ]))
  ])),
  StructField("description", StructType([
    StructField("p", ArrayType(StringType()))
  ]))
])

df = (
  spark.read
  .format('xml')
  .schema(full_text_description_schema)
  .options(rowTag='us-patent-grant')
  .load(landing_zone_path)
)

df.cache()
df.count()

# COMMAND ----------

full_text_df = (
  df
  .where(size("description.p") > 0)
  .withColumn("text", array_join("description.p", "\n"))
  .select("us-bibliographic-data-grant.application-reference.document-id.*", "text")
  .withColumn("date", to_date(col("date").cast("string"), format="yyyyMMdd"))
  .withColumn("year", year("date"))
)

full_text_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists patents;

# COMMAND ----------

(
  full_text_df.write
  .format("delta")
  .mode("overwrite")
  .partitionBy("year")
  .saveAsTable(name=bronze_table_name, path=bronze_table_path)
)

# COMMAND ----------

dbutils.notebook.exit("0")

# COMMAND ----------

dbutils.widgets.text("landingZonePath", "/home/stuart@databricks.com/datasets/rolls-royce/patents/raw")
dbutils.widgets.text("bronzeTableName", "patents.text_bronze")
dbutils.widgets.text("bronzeTablePath", "/home/stuart@databricks.com/datasets/rolls-royce/patents/bronze")
