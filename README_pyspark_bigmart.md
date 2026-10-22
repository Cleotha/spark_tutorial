# ⚡ PySpark Fundamentals: BigMart Sales

A hands-on Databricks notebook covering the core PySpark DataFrame operations every data engineer uses: reading data in different formats, defining schemas, selecting and filtering, transforming columns, sorting, and removing duplicates. All examples use the **BigMart Sales** dataset.

## Overview

This project is a structured walkthrough of the first things you do with a new dataset in Spark:

```
Read data  →  Define schema  →  Select / Filter  →  Transform columns  →  Sort / Limit  →  Drop / Deduplicate
```

Each section is a short, runnable example with scenarios that build from simple to more realistic.

## Dataset

| File | Format | Description |
|---|---|---|
| `BigMart Sales.csv` | CSV | retail sales records for items sold across outlets |
| `drivers.json` | JSON | sample JSON file used to demonstrate JSON reading |

**BigMart Sales schema**

| Column | Type | Description |
|---|---|---|
| `item_identifier` | string | unique item code |
| `item_weight` | double | weight of the item |
| `item_fat_content` | string | fat category (Regular, Low Fat) |
| `item_visibility` | double | share of display area given to the item |
| `item_type` | string | product category (e.g. Soft Drinks) |
| `item_MRP` | double | maximum retail price |
| `outlet_identifier` | string | unique outlet code |
| `outlet_establishment_year` | integer | year the outlet opened |
| `outlet_size` | string | outlet size category |
| `outlet_location_type` | string | city tier (Tier 1, 2, 3) |
| `outlet_type` | string | outlet category |
| `item_outlet_sales` | double | sales of the item at the outlet |

## Topics Covered

| Topic | What it demonstrates |
|---|---|
| Reading CSV and JSON | `spark.read.format(...)` with options |
| Schema inference | `inferSchema` and `header` options |
| DDL schema | defining a schema as a SQL-style string |
| `StructType` schema | defining a schema programmatically with `StructField` |
| `select` | choosing specific columns |
| `alias` | renaming a column inside a query |
| `filter` / `where` | single, combined (`&`) and null/`isin` conditions |
| `withColumnRenamed` | renaming a column in the DataFrame |
| `withColumn` | adding columns, calculating values, `regexp_replace` |
| Type casting | `cast()` to change a column's data type |
| `sort` / `orderBy` | ascending, descending and multi-column sorting |
| `limit` | restricting the number of rows |
| `drop` | removing one or several columns |
| `dropDuplicates` | removing duplicate rows, fully or by subset |

## Notebook Walkthrough

### 1. Reading data
```python
df = (spark.read.format("csv")
      .option("inferSchema", True)
      .option("header", True)
      .load("/Volumes/workspace/default/my_files/BigMart Sales.csv"))
```
Also covers reading JSON and listing files with `dbutils.fs.ls(...)`.

### 2. Defining a schema
Two approaches, so Spark doesn't have to guess types:

- **DDL string**: a compact, SQL-style definition
- **`StructType`**: a programmatic definition using `StructField(name, type, nullable)`

Defining the schema explicitly is faster (no extra pass over the data) and safer (types never change unexpectedly).

### 3. Selecting and renaming
```python
df.select(col("item_identifier"), col("item_weight")).display()
df.select(col("item_identifier").alias("item_ID")).display()
```

### 4. Filtering
```python
# single condition
df.filter(col("item_fat_content") == "Regular")

# multiple conditions
df.filter((col("item_type") == "Soft Drinks") & (col("item_weight") < 10))

# null check combined with isin
df.filter(col("outlet_size").isNull() & col("outlet_location_type").isin(["Tier 1", "Tier 2"]))
```

### 5. Transforming columns
```python
df = df.withColumn("flag", lit("new"))                                  # constant column
df.withColumn("multiply", col("item_weight") * col("item_MRP"))          # calculated column
df.withColumn("item_fat_content", regexp_replace("item_fat_content", "Regular", "Reg"))
```

### 6. Type casting
```python
df = df.withColumn("item_weight", col("item_weight").cast(StringType()))
```

### 7. Sorting and limiting
```python
df.sort(col("item_weight").desc())
df.sort(["item_weight", "item_visibility"], ascending=[1, 0])
df.limit(10)
```

### 8. Dropping columns and duplicates
```python
df.drop("item_visibility", "item_type")
df.dropDuplicates()
df.dropDuplicates(subset=["item_type"])
```

## Requirements

- **Databricks** workspace (the Free Edition works)
- A Unity Catalog **Volume** containing the data files, referenced as `/Volumes/workspace/default/my_files/`
- PySpark (preinstalled on Databricks)

## How to Run

1. Create a Volume in your Databricks workspace (for example `workspace.default.my_files`).
2. Upload `BigMart Sales.csv` and `drivers.json` to that Volume.
3. Import the notebook into your workspace and attach it to a compute cluster.
4. Update the file paths if your Volume has a different name.
5. Run the cells from top to bottom.

> `display()` and `dbutils` are Databricks-specific. To run the code in plain PySpark, replace `.display()` with `.show()` and use the standard file system for paths.

## Notes and Things to Watch

- **Option name:** the option is spelled `inferSchema`, not `interSchema`. Misspelled options are silently ignored by Spark, so the JSON read would not infer types. JSON also does not need `header`, since JSON carries its own field names.
- **`display()` returns nothing.** `df_sel = df.select(...).display()` stores `None` in `df_sel`. Assign the DataFrame first and call `display()` on a separate line.
- **Reassigning `df` changes later results.** After `df = df.withColumn("Item_weight", col("Item_weight").cast(StringType()))`, the weight column is a string, so any later sort on it is alphabetical (`"10.5"` sorts before `"9.2"`). Cast back to `DoubleType()` or use a different variable name.
- **DataFrames are immutable.** `withColumn`, `drop`, `filter` and similar methods return a new DataFrame. If you don't assign the result, the original is unchanged.
- **`dropDuplicates(subset=[...])`** keeps one arbitrary row per group, not necessarily the first.
- **Column names are case-insensitive by default** in Spark, but consistent casing is still good practice.

## What I Learned

- How Spark reads different file formats and why explicit schemas are better than inferred ones
- The difference between DDL and `StructType` schemas
- How to filter with multiple conditions using `&`, `|` and `isin`
- How lazy, immutable DataFrames behave when chaining and reassigning
- How to clean values with `regexp_replace` and change types with `cast`

## Next Steps

- Aggregations: `groupBy`, `agg`, `count`, `sum`, `avg`
- Joins between DataFrames
- Window functions: `row_number`, `rank`, `lag`
- Handling nulls: `fillna`, `dropna`
- Writing results to Parquet or Delta tables
- Spark SQL with temporary views

## Tech Stack

- **Apache Spark / PySpark**
- **Databricks**
- **Python**
