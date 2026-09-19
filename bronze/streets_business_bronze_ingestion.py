# Databricks notebook source
# DBTITLE 1,Bronze Layer — Raw Data Ingestion
# MAGIC %md
# MAGIC # Bronze Layer — Raw Data Ingestion
# MAGIC
# MAGIC **Purpose:** Download raw street closure and business data from public Tel Aviv municipal sources and persist them as Delta tables in the Bronze layer.
# MAGIC
# MAGIC **Inputs:**
# MAGIC - Street closures CSV from Google Cloud Storage
# MAGIC - Businesses JSON from Tel Aviv ArcGIS REST API
# MAGIC
# MAGIC **Main transformations:**
# MAGIC - Download source files to Unity Catalog Volumes
# MAGIC - Load into Spark DataFrames with schema inference (all strings)
# MAGIC - Persist as raw Delta tables
# MAGIC
# MAGIC **Output tables:**
# MAGIC - `workspace.bronze.street_bronze`
# MAGIC - `workspace.bronze.business_bronze`

# COMMAND ----------

# DBTITLE 1,Cell 1 — Define Street Closures Source Configuration
street_csv_url = "https://storage.googleapis.com/test_onedatai/rechov_sagur.csv"

# COMMAND ----------

# DBTITLE 1,Cell 2 — Import HTTP Request Library
import requests

# COMMAND ----------

# DBTITLE 1,Cell 3 — Download Street Closures CSV from Source URL
response = requests.get(street_csv_url)
response.raise_for_status()

# COMMAND ----------

# DBTITLE 1,Cell 4 — Define Databricks Storage Path for Raw Street Closures File
street_path = "/Volumes/workspace/default/home_assignment/rechov_sagur.csv"

# COMMAND ----------

# DBTITLE 1,Cell 5 — Persist Raw Street Closures CSV to Databricks File System
with open(street_path, "wb") as file:
    file.write(response.content)

# COMMAND ----------

# DBTITLE 1,Cell 6 — Load Raw Street Closures CSV into Bronze DataFrame
df_bronze_street = (
    spark.read
         .option("header", True)
         .option("inferSchema", False)
         .csv(street_path)
)

# COMMAND ----------

# DBTITLE 1,Business Data Ingestion Section
# MAGIC %md
# MAGIC ## Business Data Ingestion

# COMMAND ----------

# DBTITLE 1,Cell 7 — Preview Bronze Street Closures Data
display(df_bronze_street)

# COMMAND ----------

# DBTITLE 1,Cell 8 — Inspect Bronze Street Closures Schema
df_bronze_street.printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 9 — Validate Bronze Street Closures Record Count
print(df_bronze_street.count())

# COMMAND ----------

# DBTITLE 1,Cell 10 — Define Business API Source Configuration
business_api_url = (
    "https://gisn.tel-aviv.gov.il/arcgis/rest/services/"
    "IView2/MapServer/925/query?"
    "where=1%3D1&outFields=*&f=json"
)

# COMMAND ----------

# DBTITLE 1,Cell 11 — Download Raw Business Data from ArcGIS REST API
business_response = requests.get(business_api_url)
business_response.raise_for_status()

# COMMAND ----------

# DBTITLE 1,Cell 12 — Define Unity Catalog Volume Path for Raw Business JSON File
business_path = "/Volumes/workspace/default/home_assignment/businesses.json"

# COMMAND ----------

# DBTITLE 1,Cell 13 — Write Raw Business JSON to Unity Catalog Volume
with open(business_path, "wb") as file:
    file.write(business_response.content)

# COMMAND ----------

# DBTITLE 1,Cell 14 — Load Raw Business JSON into Bronze DataFrame
df_bronze_business = (
    spark.read
         .option("multiline", True)
         .json(business_path)
)

# COMMAND ----------

# DBTITLE 1,Cell 15 — Preview Raw Bronze Business Data
display(df_bronze_business)

# COMMAND ----------

# DBTITLE 1,Cell 16 — Inspect Bronze Business Schema
df_bronze_business.printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 17 — Validate Bronze Business Data Structure and Record Count
print(df_bronze_business.count())

# COMMAND ----------

# DBTITLE 1,Create bronze schema
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# COMMAND ----------

# DBTITLE 1,Create Street Bronze Table
df_bronze_street.write.mode("overwrite").saveAsTable("bronze.street_bronze")

# COMMAND ----------

# DBTITLE 1,Create Business Bronze Table
df_bronze_business.write.mode("overwrite").saveAsTable("bronze.business_bronze")