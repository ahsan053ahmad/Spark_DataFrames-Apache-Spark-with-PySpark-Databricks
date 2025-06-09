# Spark_DataFrames-Apache-Spark-with-PySpark-Databricks

This repository contains my assignment submission for processing distributed data using  **Apache Spark DataFrames** in **Databricks**. The goal was to perform exploratory and summary analytics on real flight data using **PySpark**, highlighting the advantages of schema-aware, distributed data processing.

---

### Business Problem

Handling large volumes of semi-structured data efficiently requires scalable tools like Apache Spark. This assignment simulated a real-world big data pipeline using Spark to analyze air traffic between the U.S. and other countries (2010–2015).  
The goal was to extract insights such as traffic patterns, domestic vs. international flights, and yearly trends using a schema-aware, fault-tolerant distributed framework.

---

### Dataset Overview

The dataset consists of **CSV files (2010–2015)** available in Databricks File System:

- Columns: `DEST_COUNTRY_NAME`, `ORIGIN_COUNTRY_NAME`, `count`
- Schema: renamed to `dest`, `origin`, and `numflights`
- Location: `dbfs:/databricks-datasets/definitive-guide/data/flight-data/csv/`
- Read into Spark using `spark.read.csv()` and `StructType` schemas

---

### Project Objectives

The assignment focused on:

- Reading multiple distributed files and combining into a single DataFrame
- Exploring schemas via `printSchema()` and managing data types
- Performing transformations (e.g., renaming, adding year from filename)
- Filtering and grouping data for exploratory analysis
- Creating new columns using Spark functions like `input_file_name()`, `regexp_extract()`
- Performing aggregate operations: `sum()`, `avg()`, `max()`
- Filtering international vs. domestic flight records
- Creating visual summaries using Databricks’ `display()` chart function
- Joining DataFrames and adding Boolean columns based on conditional logic

---

### Solution Approach

**1. Environment Setup & Housekeeping**
- Imported PySpark modules: `pyspark.sql.functions` and `pyspark.sql.types`
- Initialized notebook and displayed file directory contents

**2. Data Loading & Schema Handling**
- Loaded 2014 and 2015 CSVs using `inferSchema=True`
- Viewed DataFrame schema pre- and post-inference
- Combined all CSVs into a single DataFrame using wildcard path
- Created a custom schema using `StructType` and re-read the files

**3. DataFrame Transformations**
- Renamed `count` to `numflights`, added `filename` column with `input_file_name()`
- Extracted `year` from filenames using `regexp_extract()` and cast to integer
- Dropped unnecessary columns, reduced duplicates, and validated schema

**4. Data Analysis & Aggregations**
- Sorted records by destination and flight count
- Calculated distinct destination/year combinations
- Filtered data for Japan, South Korea, and domestic flights
- Summarized yearly flight counts, both domestic and international
- Compared inbound vs. outbound international flights with Boolean logic
- Computed aggregates: sum, average, and max flights to the U.S. per year

**5. Visualization**
- Created a bar chart to visualize the number of domestic flights over time using `display()` in Databricks

---

### Business Value

This assignment simulates core data engineering and analytics workflows, demonstrating:

- **Scalable data processing** with schema validation
- **Insightful analytics** from flight records with minimal memory use
- **Automation of ETL pipelines** for multi-file input
- **Visual dashboards** for pattern recognition and trend analysis
- **International route planning** insights using grouped aggregates and Boolean logic

---

### Challenges Encountered

- Parsing file names to extract meaningful values (e.g., year)
- Dealing with inferred vs. custom schema compatibility
- Managing large output with `.collect()` vs. `.show()` safely
- Constructing regular expressions for robust year extraction
- Learning new PySpark syntax for chaining and column transformations

---
