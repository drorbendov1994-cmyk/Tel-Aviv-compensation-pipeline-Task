# Databricks notebook source
# DBTITLE 1,Gold Layer — Business Compensation Model
# MAGIC %md
# MAGIC # Gold Layer — Business Compensation Model
# MAGIC
# MAGIC **Purpose:** Calculate 2023 annual compensation for businesses affected by street closures in Tel Aviv.
# MAGIC
# MAGIC **Inputs:**
# MAGIC - `workspace.silver.business_silver`
# MAGIC - `workspace.silver.street_closures_silver`
# MAGIC
# MAGIC **Main transformations:**
# MAGIC - Filter closures overlapping with 2023
# MAGIC - Normalize closure date ranges to 2023 boundaries
# MAGIC - Expand both affected streets (from/to) into individual records
# MAGIC - Expand closure date ranges into individual days
# MAGIC - Match businesses to closure days by street name
# MAGIC - Deduplicate to one compensation per BusinessId per day
# MAGIC - Calculate daily compensation: `min(Area * 100, 10000)`
# MAGIC - Aggregate to annual compensation per business
# MAGIC - Aggregate to annual compensation per street with CostPerArea
# MAGIC
# MAGIC **Output tables:**
# MAGIC - `workspace.gold.annual_business_compensation_2023`
# MAGIC - `workspace.gold.annual_street_compensation_2023`

# COMMAND ----------

# DBTITLE 1,Cell 1 — Load Silver Business and Street Closure Tables
df_business = spark.table("workspace.silver.business_silver")

df_street = spark.table("workspace.silver.street_closures_silver")

# COMMAND ----------

# DBTITLE 1,Step 1 Section
# MAGIC %md
# MAGIC ## Step 1: Filter and Expand Closures into Street-Days

# COMMAND ----------

# DBTITLE 1,Cell 2 — Filter Street Closures Overlapping with 2023
from pyspark.sql import functions as F

df_street_2023 = (
    df_street
    .filter(
        (F.col("ClosureEndTimestamp") >= F.lit("2023-01-01"))
        &
        (F.col("ClosureStartTimestamp") <= F.lit("2023-12-31"))
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 3 — Limit Closure Date Ranges to 2023
df_street_2023 = (
    df_street_2023
    .withColumn(
        "CompensationStartDate",
        F.greatest(
            F.to_date(F.col("ClosureStartTimestamp")),
            F.lit("2023-01-01").cast("date")
        )
    )
    .withColumn(
        "CompensationEndDate",
        F.least(
            F.to_date(F.col("ClosureEndTimestamp")),
            F.lit("2023-12-31").cast("date")
        )
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 4 — Create Independent Closed Street Records
df_closed_streets = (
    df_street_2023
    .select(
        "StreetClosureId",
        "CompensationStartDate",
        "CompensationEndDate",
        F.explode(
            F.array(
                F.col("FromStreetName"),
                F.col("ToStreetName")
            )
        ).alias("ClosedStreetName")
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 5 — Remove Duplicate Street Records per Closure
df_closed_streets = (
    df_closed_streets
    .dropDuplicates([
        "StreetClosureId",
        "ClosedStreetName"
    ])
)

# COMMAND ----------

# DBTITLE 1,Cell 6 — Expand Closure Date Ranges into Individual Days
df_closure_days = (
    df_closed_streets
    .withColumn(
        "ClosureDate",
        F.explode(
            F.sequence(
                F.col("CompensationStartDate"),
                F.col("CompensationEndDate")
            )
        )
    )
)

# COMMAND ----------

# DBTITLE 1,Step 2 Section
# MAGIC %md
# MAGIC ## Step 2: Match Businesses and Calculate Compensation

# COMMAND ----------

# DBTITLE 1,Cell 7 — Match Businesses to Closure Days
df_business_closure_days = (
    df_business.alias("b")
    .join(
        df_closure_days.alias("c"),
        F.col("b.StreetName") == F.col("c.ClosedStreetName"),
        "inner"
    )
    .select(
        F.col("b.BusinessId"),
        F.col("b.StreetName"),
        F.col("b.Area"),
        F.col("c.ClosureDate"),
        F.col("c.StreetClosureId")
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 9 — Remove Duplicate Compensation Days
df_business_compensation_days = (
    df_business_closure_days
    .dropDuplicates([
        "BusinessId",
        "ClosureDate"
    ])
)

# COMMAND ----------

# DBTITLE 1,Cell 10 — Calculate Daily Compensation
df_business_compensation_days = (
    df_business_compensation_days
    .withColumn(
        "DailyCompensation",
        F.least(
            F.col("Area") * F.lit(100),
            F.lit(10000)
        )
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 14 — Create Gold Schema
spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.gold
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 1 — Annual Compensation per Business

# COMMAND ----------

# DBTITLE 1,Cell 15 — Build Annual Business Compensation Gold Dataset
df_gold_business_compensation = (
    df_business_compensation_days
    .groupBy(
        "BusinessId",
        "StreetName"
    )
    .agg(
        F.first("Area").alias("Area"),
        F.count("ClosureDate").alias("CompensatedDays"),
        F.sum("DailyCompensation").alias("TotalCompensation")
    )
    .withColumn(
        "CostPerArea",
        F.when(
            F.col("Area") > 0,
            F.col("TotalCompensation") / F.col("Area")
        )
    )
    .withColumn(
        "AverageDailyCompensation",
        F.when(
            F.col("CompensatedDays") > 0,
            F.col("TotalCompensation") / F.col("CompensatedDays")
        )
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 16 — Save Annual Business Compensation Gold Table
(
    df_gold_business_compensation
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.gold.annual_business_compensation_2023"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 2 — Annual Compensation per Street

# COMMAND ----------

# DBTITLE 1,Cell 17 — Build Annual Street Compensation Gold Dataset
df_gold_street_compensation = (
    df_gold_business_compensation
    .groupBy("StreetName")
    .agg(
        F.countDistinct("BusinessId")
            .alias("AffectedBusinesses"),

        F.sum("Area")
            .alias("TotalBusinessArea"),

        F.sum("CompensatedDays")
            .alias("CompensatedBusinessDays"),

        F.sum("TotalCompensation")
            .alias("TotalCompensation")
    )
    .withColumn(
        "CostPerArea",
        F.when(
            F.col("TotalBusinessArea") > 0,
            F.col("TotalCompensation")
            / F.col("TotalBusinessArea")
        )
    )
    .withColumn(
        "AverageCompensationPerBusiness",
        F.when(
            F.col("AffectedBusinesses") > 0,
            F.col("TotalCompensation")
            / F.col("AffectedBusinesses")
        )
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 18 — Save Annual Street Compensation Gold Table
(
    df_gold_street_compensation
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.gold.annual_street_compensation_2023"
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 19 — Validate Gold Business Output
display(
    spark.table(
        "workspace.gold.annual_business_compensation_2023"
    )
    .orderBy(
        F.col("TotalCompensation").desc()
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 20 — Validate Gold Street Output
display(
    spark.table(
        "workspace.gold.annual_street_compensation_2023"
    )
    .orderBy(
        F.col("TotalCompensation").desc()
    )
)