# 🥉 Bronze Layer — Olist Data Ingestion

The Bronze Layer is the first stage of the RetailPulse data platform.

Its purpose is to ingest the raw Olist e-commerce datasets into Databricks and store them as Delta tables in the `workspace.bronze` schema.

The Bronze layer preserves the source data while adding basic ingestion metadata for data lineage and traceability.

---

## 🎯 Objectives

The Bronze layer is responsible for:

- Reading raw Olist CSV files from a Unity Catalog Volume
- Loading the datasets into Spark DataFrames
- Automatically inferring the source schema
- Adding ingestion metadata
- Storing the data as Delta tables
- Validating the number of records loaded
- Providing a reliable foundation for the Silver layer

---

## 🏗️ Bronze Layer Architecture

```text
Olist CSV Files
       │
       ▼
Unity Catalog Volume
/Volumes/workspace/bronze/raw_olist/
       │
       ▼
Databricks Spark
       │
       ├── Read CSV
       ├── Infer Schema
       ├── Add Metadata
       │
       ▼
Delta Tables
workspace.bronze
       │
       ├── customers
       ├── orders
       ├── order_items
       ├── order_payments
       └── products