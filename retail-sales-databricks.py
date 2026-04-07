# Databricks notebook source
import subprocess
result = subprocess.run(['find', '/Volumes', '-name', '*.csv'], capture_output=True, text=True)
print(result.stdout)

# COMMAND ----------

# ============================================
# BRONZE LAYER - Ingest raw CSV into Delta
# ============================================

# File path to your uploaded CSV
csv_path = "/Volumes/retail_pipeline_workspace/default/retail_data/online_retail_2010_2011.csv"

# Where we'll save the Bronze Delta table
bronze_path = "/Volumes/retail_pipeline_workspace/default/retail_data/bronze/raw_sales"

# Step 1: Read raw CSV with PySpark
df_raw = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .option("escape", '"')
    .csv(csv_path))

# Step 2: Check what we loaded
print(f"Total rows: {df_raw.count()}")
print(f"Columns: {df_raw.columns}")
df_raw.printSchema()

# Step 3: Show first 5 rows
df_raw.show(5)

# Step 4: Save as Delta table (Bronze layer)
df_raw = df_raw.withColumnRenamed("Customer ID", "Customer_ID")
(df_raw.write
    .format("delta")
    .mode("overwrite")
    .save(bronze_path))

print("✅ Bronze layer written successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# ============================================
# SILVER LAYER - Clean & transform the data
# ============================================

from pyspark.sql import functions as F

silver_path = "/Volumes/retail_pipeline_workspace/default/retail_data/silver/cleaned_sales"

# Step 1: Read from Bronze Delta table
df_bronze = spark.read.format("delta").load(
    "/Volumes/retail_pipeline_workspace/default/retail_data/bronze/raw_sales"
)

# Step 2: Clean the data
df_silver = (df_bronze
    # Rename columns to be consistent
    .withColumnRenamed("Customer_ID", "CustomerID")

    # Parse InvoiceDate string into proper timestamp
    .withColumn("InvoiceDate", F.to_timestamp("InvoiceDate", "dd/MM/yyyy HH:mm"))

    # Remove rows where key fields are null
    .filter(F.col("CustomerID").isNotNull())
    .filter(F.col("InvoiceDate").isNotNull())
    .filter(F.col("Quantity") > 0)
    .filter(F.col("Price") > 0)

    # Add a calculated column: total revenue per line
    .withColumn("LineRevenue", F.round(F.col("Quantity") * F.col("Price"), 2))

    # Add year and month columns for easier aggregation later
    .withColumn("Year", F.year("InvoiceDate"))
    .withColumn("Month", F.month("InvoiceDate"))
)

# Step 3: Check results
print(f"Bronze rows: {df_bronze.count()}")
print(f"Silver rows after cleaning: {df_silver.count()}")
print(f"Rows removed: {df_bronze.count() - df_silver.count()}")
df_silver.show(5)

# Step 4: Save as Delta table (Silver layer)
(df_silver.write
    .format("delta")
    .mode("overwrite")
    .save(silver_path))

print("✅ Silver layer written successfully!")

# COMMAND ----------

# ============================================
# GOLD LAYER - Business KPIs & Aggregations
# ============================================

silver_path = "/Volumes/retail_pipeline_workspace/default/retail_data/silver/cleaned_sales"
gold_base = "/Volumes/retail_pipeline_workspace/default/retail_data/gold"

df_silver = spark.read.format("delta").load(silver_path)

# --- KPI 1: Monthly Revenue Trend ---
df_monthly_revenue = (df_silver
    .groupBy("Year", "Month")
    .agg(
        F.round(F.sum("LineRevenue"), 2).alias("TotalRevenue"),
        F.countDistinct("Invoice").alias("TotalOrders"),
        F.countDistinct("CustomerID").alias("UniqueCustomers")
    )
    .orderBy("Year", "Month")
)

df_monthly_revenue.show()

(df_monthly_revenue.write
    .format("delta")
    .mode("overwrite")
    .save(f"{gold_base}/monthly_revenue"))

# --- KPI 2: Top 10 Products by Revenue ---
df_top_products = (df_silver
    .groupBy("StockCode", "Description")
    .agg(
        F.round(F.sum("LineRevenue"), 2).alias("TotalRevenue"),
        F.sum("Quantity").alias("TotalUnitsSold")
    )
    .orderBy(F.desc("TotalRevenue"))
    .limit(10)
)

df_top_products.show()

(df_top_products.write
    .format("delta")
    .mode("overwrite")
    .save(f"{gold_base}/top_products"))

# --- KPI 3: Revenue by Country ---
df_by_country = (df_silver
    .groupBy("Country")
    .agg(
        F.round(F.sum("LineRevenue"), 2).alias("TotalRevenue"),
        F.countDistinct("CustomerID").alias("UniqueCustomers")
    )
    .orderBy(F.desc("TotalRevenue"))
)

df_by_country.show()

(df_by_country.write
    .format("delta")
    .mode("overwrite")
    .save(f"{gold_base}/revenue_by_country"))

print("✅ Gold layer written successfully!")

# COMMAND ----------

# ============================================
# TIME TRAVEL - Query Delta table history
# ============================================

from delta.tables import DeltaTable

silver_path = "/Volumes/retail_pipeline_workspace/default/retail_data/silver/cleaned_sales"

# --- Show full history of the Silver table ---
delta_table = DeltaTable.forPath(spark, silver_path)
delta_table.history().select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

# --- Query version 0 (original write) ---
df_v0 = (spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(silver_path))

print(f"Version 0 row count: {df_v0.count()}")

# --- Simulate an update: overwrite with a filtered subset ---
df_silver = spark.read.format("delta").load(silver_path)

(df_silver
    .filter(F.col("Country") == "United Kingdom")
    .write
    .format("delta")
    .mode("overwrite")
    .save(silver_path))

print(f"Version 1 row count (UK only): {spark.read.format('delta').load(silver_path).count()}")

# --- Time travel back to version 0 ---
df_restored = (spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(silver_path))

print(f"Restored version 0 row count: {df_restored.count()}")
print("✅ Time travel works!")

# --- Restore back to full dataset ---
(df_restored.write
    .format("delta")
    .mode("overwrite")
    .save(silver_path))

print("✅ Table restored to full dataset!")

# COMMAND ----------

# ============================================
# VISUALIZATIONS - Display Gold layer KPIs
# ============================================

gold_base = "/Volumes/retail_pipeline_workspace/default/retail_data/gold"

# Load all three Gold tables
df_monthly = spark.read.format("delta").load(f"{gold_base}/monthly_revenue")
df_products = spark.read.format("delta").load(f"{gold_base}/top_products")
df_countries = spark.read.format("delta").load(f"{gold_base}/revenue_by_country")

# Display each one - Databricks renders these as interactive tables/charts
print("📊 Monthly Revenue Trend")
display(df_monthly.orderBy("Year", "Month"))

print("🏆 Top 10 Products by Revenue")
display(df_products)

print("🌍 Revenue by Country")
display(df_countries)