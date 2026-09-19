# Databricks notebook source
# DBTITLE 1,Street Silver Layer
# MAGIC %md
# MAGIC # Street Silver Layer — Street Closure Cleaning
# MAGIC
# MAGIC **Purpose:** Clean, standardize, and validate street closure records from the Bronze layer.
# MAGIC
# MAGIC **Input:** `workspace.bronze.street_bronze`
# MAGIC
# MAGIC **Main transformations:**
# MAGIC - Rename Hebrew columns to descriptive English names
# MAGIC - Cast data types (integers, timestamps)
# MAGIC - Handle zero/placeholder values as nulls
# MAGIC - Normalize and trim string columns
# MAGIC - Fill missing closure end timestamps with start timestamps
# MAGIC - Remove records with missing street information
# MAGIC - Validate closure date ranges (end >= start)
# MAGIC
# MAGIC **Output:** `workspace.silver.street_closures_silver`

# COMMAND ----------

# DBTITLE 1,Cell 1 — Load Bronze Streets Table
df_bronze_street = spark.table("workspace.bronze.street_bronze")

# COMMAND ----------

# DBTITLE 1,Cell 3- Standardize Streets Column Names and Data Types


from pyspark.sql import functions as F

df_street_typed = (
    df_bronze_street
    .select(
        F.col("ID")
            .cast("integer")
            .alias("ObjectId"),

        F.col("id_rechov")
            .cast("integer")
            .alias("StreetClosureId"),

        F.when(
            F.col("me_k_rechov") == 0,
            F.lit(None).cast("integer")
        ).otherwise(
            F.col("me_k_rechov").cast("integer")
        ).alias("FromStreetCode"),

        F.when(
            F.trim(F.col("me_shem_rechov")) == "0",
            F.lit(None).cast("string")
        ).otherwise(
            F.col("me_shem_rechov").cast("string")
        ).alias("FromStreetName"),

        F.when(
            F.col("ad_k_rechov") == 0,
            F.lit(None).cast("integer")
        ).otherwise(
            F.col("ad_k_rechov").cast("integer")
        ).alias("ToStreetCode"),

        F.when(
            F.trim(F.col("ad_shem_rechov")) == "0",
            F.lit(None).cast("string")
        ).otherwise(
            F.col("ad_shem_rechov").cast("string")
        ).alias("ToStreetName"),

        F.to_timestamp(
            F.col("tr_from"),
            "dd/MM/yyyy H:mm"
        ).alias("ClosureStartTimestamp"),

        F.to_timestamp(
            F.col("tr_to"),
            "dd/MM/yyyy H:mm"
        ).alias("ClosureEndTimestamp"),

        F.col("k_sug")
            .cast("integer")
            .alias("ClosureTypeId"),

        F.col("t_sug")
            .cast("string")
            .alias("ClosureTypeName"),

        F.col("k_sgira")
            .cast("integer")
            .alias("ClosureCode"),

        F.col("shaot")
            .cast("string")
            .alias("ClosureDescription"),

        F.col("sw_laila")
            .cast("string")
            .alias("NightClosureRaw")
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 4 — Normalize Street Closure String Columns
from pyspark.sql.types import StringType

string_columns = [
    field.name
    for field in df_street_typed.schema.fields
    if isinstance(field.dataType, StringType)
]

df_street_normalized = df_street_typed

for column_name in string_columns:
    df_street_normalized = (
        df_street_normalized
        .withColumn(
            column_name,
            F.when(
                F.trim(F.col(column_name)) == "",
                F.lit(None)
            ).otherwise(
                F.trim(F.col(column_name))
            )
        )
    )

# COMMAND ----------

# DBTITLE 1,Cell 5 — Fill Missing Closure End Timestamp
df_street_normalized = (
    df_street_normalized
    .withColumn(
        "ClosureEndTimestamp",
        F.coalesce(
            F.col("ClosureEndTimestamp"),
            F.col("ClosureStartTimestamp")
        )
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 6 — Remove Records with Missing Street Information
df_street_normalized = (
    df_street_normalized
    .filter(
        F.col("FromStreetName").isNotNull()
        & F.col("FromStreetCode").isNotNull()
        & F.col("ToStreetName").isNotNull()
        & F.col("ToStreetCode").isNotNull()
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 7 — Validate Closure Date Range
df_invalid_dates = (
    df_street_normalized
    .filter(
        F.col("ClosureEndTimestamp") < F.col("ClosureStartTimestamp")
    )
)

display(df_invalid_dates)

# COMMAND ----------

# DBTITLE 1,Cell 8 -Create Silver table
(
    df_street_normalized
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.silver.street_closures_silver")
)

# COMMAND ----------

# DBTITLE 1,Cell 9 — Validate Silver Street Closures Table
df_silver_street_check = spark.table(
    "workspace.silver.street_closures_silver"
)

display(df_silver_street_check)