# Databricks notebook source
# DBTITLE 1,Business Silver Layer
# MAGIC %md
# MAGIC # Business Silver Layer — Business Data Cleaning
# MAGIC
# MAGIC **Purpose:** Clean, normalize, and deduplicate business records from the Bronze layer.
# MAGIC
# MAGIC **Input:** `workspace.bronze.business_bronze`
# MAGIC
# MAGIC **Main transformations:**
# MAGIC - Explode and flatten nested JSON features
# MAGIC - Rename columns to descriptive English names
# MAGIC - Cast data types (long, double, date, integer)
# MAGIC - Parse establishment date from Unix epoch
# MAGIC - Normalize and trim string columns
# MAGIC - Remove records with no meaningful data
# MAGIC - Deduplicate by BusinessId (keep latest record)
# MAGIC
# MAGIC **Output:** `workspace.silver.business_silver`

# COMMAND ----------

# DBTITLE 1,Cell 1 — Load Bronze Business Table
df_bronze_business = spark.table("workspace.bronze.business_bronze")

# COMMAND ----------

# DBTITLE 1,Cell 2 — Inspect Bronze Business Data Structure
display(df_bronze_business)
df_bronze_business.printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 3 — Explode Business Features into Individual Records
from pyspark.sql import functions as F

df_business_exploded = (
    df_bronze_business
    .select(
        F.explode("features").alias("feature")
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 4 — Inspect Exploded Business Records
display(df_business_exploded)

# COMMAND ----------

# DBTITLE 1,Cell 5 — Flatten Business Attributes and Geometry into Columns
df_business_flattened = (
    df_business_exploded
    .select(
        "feature.attributes.*",
        F.col("feature.geometry.x").alias("geometry_x"),
        F.col("feature.geometry.y").alias("geometry_y")
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 6 — Standardize Business Column Names and Data Types
df_business_typed = (
    df_business_flattened
    .select(
        F.col("OBJECTID")
            .cast("long")
            .alias("ObjectId"),

        F.to_timestamp(
            F.col("date_import"),
            "dd/MM/yyyy HH:mm:ss"
        ).alias("ImportDate"),

        F.col("id_esek")
            .cast("long")
            .alias("BusinessId"),

        F.col("ms_bayit")
            .cast("string")
            .alias("HouseNumber"),

        F.col("ms_mezahe_machzik_rashi")
            .cast("string")
            .alias("MainOwnerId"),

        F.col("shem_machzik_rashi")
            .cast("string")
            .alias("MainOwnerName"),

        F.col("shem_rechov")
            .cast("string")
            .alias("StreetName"),

        F.col("shetach")
            .cast("double")
            .alias("Area"),

        F.col("shimush")
            .cast("string")
            .alias("Usage"),

        F.col("sw_taun_rishui")
            .cast("integer")
            .alias("RequiresLicense"),

        F.when(
            F.col("tr_hakama") == 0,
            F.lit(None).cast("date")
        ).otherwise(
            F.to_date(
                F.from_unixtime(
                    F.col("tr_hakama") / 1000
                )
            )
        ).alias("EstablishmentDate"),

        F.col("x_coord")
            .cast("double")
            .alias("XCoordinate"),

        F.col("y_coord")
            .cast("double")
            .alias("YCoordinate"),

        F.col("geometry_x")
            .cast("double")
            .alias("GeometryX"),

        F.col("geometry_y")
            .cast("double")
            .alias("GeometryY")
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 7 — Normalize String Columns
string_columns = [
    "HouseNumber",
    "MainOwnerId",
    "MainOwnerName",
    "StreetName",
    "Usage"
]

df_business_normalized = df_business_typed

for column_name in string_columns:
    df_business_normalized = (
        df_business_normalized
        .withColumn(
            column_name,
            F.trim(F.col(column_name))
        )
    )

# COMMAND ----------

# DBTITLE 1,Cell 8 — Remove Records with No Meaningful Business Data
business_data_columns = [
    "ImportDate",
    "HouseNumber",
    "MainOwnerName",
    "StreetName",
    "Area",
    "Usage",
    "RequiresLicense",
    "EstablishmentDate",
    "XCoordinate",
    "YCoordinate",
    "GeometryX",
    "GeometryY"
]

meaningful_data_condition = F.lit(False)

for column_name in business_data_columns:
    meaningful_data_condition = (
        meaningful_data_condition
        | F.col(column_name).isNotNull()
    )

df_business_normalized = (
    df_business_normalized
    .filter(meaningful_data_condition)
)

# COMMAND ----------

# DBTITLE 1,Cell 9 — Deduplicate Businesses by Latest Record
from pyspark.sql.window import Window

business_window = (
    Window
    .partitionBy("BusinessId")
    .orderBy(
        F.col("ImportDate").desc_nulls_last(),
        F.col("ObjectId").desc()
    )
)
df_silver_business = (
    df_business_normalized
    .withColumn(
        "row_number",
        F.row_number().over(business_window)
    )
    .filter(
        F.col("row_number") == 1
    )
    .drop("row_number")
)

# COMMAND ----------

# DBTITLE 1,Cell 10-Create Silver schema and table
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

(
    df_silver_business
    .write
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("workspace.silver.business_silver")
)

# COMMAND ----------

# DBTITLE 1,Cell 11-display the table
display(df_silver_business)

# COMMAND ----------

# DBTITLE 1,Cell 12 — Validate Silver Business Table
df_silver_business_check = spark.table("workspace.silver.business_silver")

display(df_silver_business_check)

df_silver_business_check.printSchema()

print(df_silver_business_check.count())