# Databricks notebook source
# MAGIC %pip install spacy

# COMMAND ----------

# MAGIC %pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.0.0/en_core_web_sm-3.0.0-py3-none-any.whl

# COMMAND ----------

import re
import string
import json
from pyspark.sql.functions import *
import spacy
import en_core_web_sm

spark.conf.set("spark.sql.adaptive.enabled", "false")

bronze_table_name = dbutils.widgets.get("bronzeTableName")
silver_table_name = dbutils.widgets.get("silverTableName")
silver_table_path = dbutils.widgets.get("silverTablePath")
gold_table_name = dbutils.widgets.get("goldTableName")
gold_table_path = dbutils.widgets.get("goldTablePath")

dbutils.fs.mkdirs(silver_table_path)
dbutils.fs.mkdirs(gold_table_path)

# COMMAND ----------

patent_text_sdf = spark.table(bronze_table_name)
patent_text_sdf.display()

# COMMAND ----------

def remove_non_ascii(text):
  printable = set(string.printable)
  return ''.join(x for x in text if x in printable)

def remove_punct(doc):
  return [t for t in doc if t.text not in string.punctuation]

def remove_tags(text):
    """Remove html tags from a string"""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def remove_stop_words(doc):
  return [t for t in doc if not t.is_stop]

def lemmatize(doc):
  return ' '.join([t.lemma_ for t in doc])

def extract_entities(doc):
  return [{"text": ent.text, "start": ent.start_char, "end": ent.end_char, "label": ent.label_} for ent in doc.ents]

# COMMAND ----------

def get_entities(nlp, text):
  """
  Extracting descriptions from raw text by removing junk, URLs, etc.
  We group consecutive lines into paragraphs and use spacy to parse sentences.
  """
  
  # remove non ASCII characters
  ascii_text = remove_non_ascii(text)
  removed_tags = remove_tags(ascii_text)
  
  doc = nlp(removed_tags)
  removed_punct = remove_punct(doc)
  removed_stop_words = remove_stop_words(removed_punct)
  lemmatized = lemmatize(removed_stop_words)
  return json.dumps(extract_entities(nlp(lemmatized)))

# COMMAND ----------

@pandas_udf('string', PandasUDFType.SCALAR_ITER)
def extract_entities_udf(content_series_iter):
  """
  as loading a spacy model takes time, we certainly do not want to load model for each record to process
  we load model only once and apply it to each batch of content this executor is responsible for
  """
  
  # load spacy model
  nlp = en_core_web_sm.load()
  
  # cleanse and tokenize a batch of PDF content 
  for content_series in content_series_iter:
    yield content_series.map(lambda x: get_entities(nlp, x))

# COMMAND ----------

# *****************************
# apply transformation at scale
# *****************************

descriptions_sdf = (
  patent_text_sdf
  .withColumn('entities', extract_entities_udf(col("text")))
  .select('country', 'date', 'doc-number', 'entities')
)

descriptions_sdf.cache()
descriptions_sdf.write.format("noop").mode("append").save()

display(descriptions_sdf)

# COMMAND ----------

entities_sdf = (
  descriptions_sdf
  .withColumn("entities", from_json(col("entities"), "ARRAY<MAP<STRING,STRING>>"))
  .withColumn("entities", explode("entities"))
  .where(col("entities.label").isin(["PRODUCT", "GPE", "ORG", "NORP"]))
  .where(~(col("entities.text").isin(["FIG", "FIGS"])))
  .select("country", "date", "doc-number", "entities.text", "entities.label")
  .dropDuplicates()
)

entities_sdf.display()

# COMMAND ----------

entities_sdf.count()

# COMMAND ----------

(
  entities_sdf.write
  .format("delta")
  .mode("append")
  .save(silver_table_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Merge against latest view of graph triplets
# MAGIC - We will use the change data feed to work out what needs to be updated in the graph DB

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS $goldTableName (
# MAGIC   srcId string
# MAGIC   , srcType string
# MAGIC   , dstId string
# MAGIC   , dstType string
# MAGIC   , count long
# MAGIC )
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)
# MAGIC LOCATION '$goldTablePath'

# COMMAND ----------

article_edges_sdf = (
  entities_sdf
  .select(
    upper(col("text")).alias("srcId"), col("LABEL").alias("srcType"), 
    col("doc-number").alias("dstId"), lit("patent").alias("dstType")
  )
  .groupBy("srcId", "srcType", "dstId", "dstType")
  .count()
)

article_edges_sdf.createOrReplaceTempView("new_article_edges")
article_edges_sdf.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO $goldTableName edges
# MAGIC USING new_article_edges new_edges
# MAGIC ON
# MAGIC   new_edges.srcId = edges.srcId
# MAGIC   and new_edges.srcType = edges.srcType
# MAGIC   and new_edges.dstId = edges.dstId
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

dst_sdf = (
  entities_sdf
  .select("doc-number", upper(col("text")).alias("dstId"), col("LABEL").alias("dstType"))
)

concept_edges_sdf = (
  entities_sdf
  .select("doc-number", upper(col("text")).alias("srcId"), col("LABEL").alias("srcType"))
  .join(dst_sdf, on="doc-number", how="inner")
  .where(~(col("srcId")==col("dstId")))
  .groupBy("srcId", "srcType", "dstId", "dstType")
  .count()
)

concept_edges_sdf.createOrReplaceTempView("new_concept_edges")
concept_edges_sdf.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO $goldTableName edges
# MAGIC USING new_concept_edges new_edges
# MAGIC ON
# MAGIC   new_edges.srcId = edges.srcId
# MAGIC   and new_edges.srcType = edges.srcType
# MAGIC   and new_edges.dstId = edges.dstId
# MAGIC   and new_edges.dstType = edges.dstType
# MAGIC WHEN MATCHED THEN UPDATE SET count = new_edges.count + edges.count
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

change_feed = (
  spark.read
  .format("delta")
  .option("readChangeFeed", "true")
#   .option("startingTimestamp", "2021-05-25 20:20:00")
  .option("startingVersion", 0)
  .load(gold_table_path)
)

change_feed.display()

# COMMAND ----------

dbutils.notebook.exit("0")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists patents.edges_gold

# COMMAND ----------

dbutils.fs.rm("/home/stuart@databricks.com/datasets/rolls-royce/patents/gold", True)

# COMMAND ----------

dbutils.widgets.text("bronzeTableName", "patents.text_bronze")
dbutils.widgets.text("silverTableName", "patents.entities_silver")
dbutils.widgets.text("silverTablePath", "/home/stuart@databricks.com/datasets/rolls-royce/patents/silver")
dbutils.widgets.text("goldTableName", "patents.edges_gold")
dbutils.widgets.text("goldTablePath", "/home/stuart@databricks.com/datasets/rolls-royce/patents/gold")
