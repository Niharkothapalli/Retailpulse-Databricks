from pyspark.sql.functions import *


customers_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Volumes/workspace/bronze/raw_olist/olist_customers_dataset.csv")
)

display(customers_df)

# RetailPulse - Bronze Layer
# Olist Retail Data Ingestion


# Purpose:
# This notebook ingests the raw Olist CSV files from the
# Unity Catalog Volume and stores them as Delta tables
# in the Bronze layer.

# Bronze Layer Principles:
# - Preserve the source data as much as possible.
# - Do not perform business transformations.
# - Capture ingestion metadata.
# - Store the data in Delta format.

# Source:
# Olist Brazilian E-Commerce Dataset

# Target Catalog:
# workspace

# Target Schema:
# bronze



# 1. CHECK RAW OLIST FILES

# List all raw Olist files available in the Bronze volume.

%fs ls /Volumes/workspace/bronze/raw_olist/


# 2. READ THE CUSTOMERS DATASET

# Read the raw customers CSV file into a Spark DataFrame.

# header = true:
# The first row contains the column names.

# inferSchema = true:
# Spark automatically detects suitable data types.

customers_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(
        "/Volumes/workspace/bronze/raw_olist/"
        "olist_customers_dataset.csv"
    )
)

# Display the raw customers DataFrame for inspection.

display(customers_df)


# 3. INSPECT THE CUSTOMERS SCHEMA

# Display the schema of the customers DataFrame.

customers_df.printSchema()


# 4. ADD BRONZE INGESTION METADATA

from pyspark.sql.functions import current_timestamp, col, lit

# Add metadata columns required for data lineage.

# _ingestion_timestamp:
# Records when the data was loaded into the Bronze layer.

# _source_file:
# Records the original source file path.

# source_system:
# Identifies the system from which the data originated.

customers_bronze_df = (
    customers_df
    .withColumn(
        "_ingestion_timestamp",
        current_timestamp()
    )
    .withColumn(
        "_source_file",
        col("_metadata.file_path")
    )
    .withColumn(
        "source_system",
        lit("olist_ecommerce")
    )
)

# Display the Bronze DataFrame.

display(customers_bronze_df)


# 5. WRITE CUSTOMERS TO THE BRONZE DELTA TABLE

# Write the customers DataFrame as a Delta table.

# Target table:
# workspace.bronze.customers

(
    customers_bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.bronze.customers"
    )
)


# 6. VALIDATE THE CUSTOMERS BRONZE TABLE

# Verify the number of records loaded into the Bronze table.

spark.sql("""
SELECT COUNT(*) AS row_count
FROM workspace.bronze.customers
""").show()


# 7. CREATE A REUSABLE BRONZE INGESTION FUNCTION

# This reusable function:

# 1. Reads a raw CSV file.
# 2. Automatically detects the schema.
# 3. Adds ingestion metadata.
# 4. Identifies the source system.
# 5. Writes the data as a Delta table.

# This avoids repeating the same ingestion logic
# for every source dataset.

def ingest_to_bronze(file_name, table_name):

    # Read the raw CSV file.

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(
            f"/Volumes/workspace/bronze/raw_olist/{file_name}"
        )
    )

    # Add Bronze ingestion metadata.

    bronze_df = (
        df
        .withColumn(
            "_ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "_source_file",
            col("_metadata.file_path")
        )
        .withColumn(
            "source_system",
            lit("olist_ecommerce")
        )
    )

    # Write the DataFrame as a Delta table.

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            f"workspace.bronze.{table_name}"
        )
    )

    # Print a confirmation message.

    print(
        f"Loaded {file_name} "
        f"→ workspace.bronze.{table_name}"
    )


# 8. INGEST THE REMAINING OLIST DATASETS

# Ingest the Orders dataset.

ingest_to_bronze(
    "olist_orders_dataset.csv",
    "orders"
)

# Ingest the Order Items dataset.

ingest_to_bronze(
    "olist_order_items_dataset.csv",
    "order_items"
)

# Ingest the Order Payments dataset.

ingest_to_bronze(
    "olist_order_payments_dataset.csv",
    "order_payments"
)

# Ingest the Products dataset.

ingest_to_bronze(
    "olist_products_dataset.csv",
    "products"
)


# 9. VALIDATE ALL BRONZE TABLES

# Compare the number of records in all Bronze tables.

spark.sql("""

SELECT
    'customers' AS table_name,
    COUNT(*) AS row_count
FROM workspace.bronze.customers

UNION ALL

SELECT
    'orders',
    COUNT(*)
FROM workspace.bronze.orders

UNION ALL

SELECT
    'order_items',
    COUNT(*)
FROM workspace.bronze.order_items

UNION ALL

SELECT
    'order_payments',
    COUNT(*)
FROM workspace.bronze.order_payments

UNION ALL

SELECT
    'products',
    COUNT(*)
FROM workspace.bronze.products


""").show()


# 10. DISPLAY ALL BRONZE TABLES

# Display the tables available in the Bronze schema.

spark.sql("""


SHOW TABLES IN workspace.bronze

""").show()


# 11. PREVIEW A BRONZE TABLE

# Preview the customers Bronze table.

spark.sql("""


SELECT *
FROM workspace.bronze.customers
LIMIT 10


""").show()