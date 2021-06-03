# Databricks notebook source
import os
import requests
from zipfile import ZipFile
from io import BytesIO

landing_zone_path = dbutils.widgets.get("landingZonePath")
dbutils.fs.mkdirs(landing_zone_path)
week_id = dbutils.widgets.get("weekId")
url = f"https://bulkdata.uspto.gov/data/patent/grant/redbook/fulltext/2021/{week_id}.zip"

response = requests.get(url)

# COMMAND ----------

zipfile = ZipFile(BytesIO(response.content))
zip_names = zipfile.namelist()
if len(zip_names) == 1:
  zipfile.extract(member=zip_names.pop(), path=os.path.join("/dbfs", landing_zone_path[1:]))

# COMMAND ----------

display(dbutils.fs.ls(landing_zone_path))

# COMMAND ----------

dbutils.notebook.exit("0")

# COMMAND ----------

dbutils.widgets.text("weekId", "ipg210511")
dbutils.widgets.text("landingZonePath", "/home/stuart@databricks.com/datasets/rolls-royce/patents/raw")
