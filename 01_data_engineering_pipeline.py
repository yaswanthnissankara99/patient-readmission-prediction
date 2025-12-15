# Databricks notebook source
# MAGIC %md
# MAGIC # Patient Hospital Readmission Prediction System
# MAGIC ## End-to-End Data Engineering Pipeline on Databricks
# MAGIC
# MAGIC **Project Goal**: Build a production-ready data pipeline to identify high-risk hospital readmission patients
# MAGIC
# MAGIC **Architecture**: Medallion Pattern (Bronze → Silver → Gold)
# MAGIC
# MAGIC **Technology**: SQL,PySpark, Delta Lake, Databricks Community Edition
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. SETUP & CONFIGURATION

# COMMAND ----------

#As Databricks Unity Catalog doesnt allow us to create the database in the Pyspark code, 
#If we try create a database using Pyspark code it will create a Database but then the remaining pyspark code cant read the database even the database is created, 
#So the traiditinal approach should be creating a database using the SQL Commands below and
#then use the below pyspark code for the remaining pipeline 


#%sql
#CREATE CATALOG IF NOT EXISTS healthcare_readmission;
#CREATE SCHEMA IF NOT EXISTS healthcare_readmission.default;
#CREATE VOLUME IF NOT EXISTS healthcare_readmission.default.readmission_prediction_project;

# Import libraries
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.window import Window
import datetime

# Set database name
#database_name = "healthcare_readmission"
#spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
#spark.sql(f"USE healthcare_readmission")

# Create paths
bronze_path = "/Volumes/healthcare_readmission/default/readmission_prediction_project/bronze/"
silver_path = "/Volumes/healthcare_readmission/default/readmission_prediction_project/silver/"
gold_path   = "/Volumes/healthcare_readmission/default/readmission_prediction_project/gold/"


# Create directories
#import os
#for path in [bronze_path, silver_path, gold_path]:
#    dbutils.fs.mkdirs(path)

print("✓ Environment configured")
print(f"✓ Database: healthcare_readmission")
print(f"✓ Paths set: Bronze, Silver, Gold (Unity Catalog Volumes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. BRONZE LAYER: Ingest Raw Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Load Patients Data (Raw)

# COMMAND ----------

# For Databricks Community Edition: Upload CSV files to workspace first
# Then load them

# Define schema for patients
patients_schema = StructType([
    StructField("patient_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("admission_date", StringType(), True),
    StructField("discharge_date", StringType(), True),
    StructField("length_of_stay", IntegerType(), True),
    StructField("readmitted_30_days", IntegerType(), True)
])

# LOAD DATA - Option 1: From uploaded CSV file
# Replace with actual path where you uploaded patients.csv
df_patients_raw = spark.read.schema(patients_schema).csv(
    "/Volumes/workspace/healthcare1_workspace/healthcare1_volume/patients.csv",  # UPDATE THIS PATH
    header=True
).withColumn("ingest_date", F.current_timestamp()) \
 .withColumn("source_file", F.lit("patients.csv")) \
 .withColumn("data_quality_check", F.lit("raw_load"))

# Save to Bronze
df_patients_raw.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
    f"{bronze_path}patients"
)
print(f"✓ Bronze Patients: {df_patients_raw.count()} records loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Load Diagnoses Data (Raw)

# COMMAND ----------

diagnoses_schema = StructType([
    StructField("diagnosis_id", IntegerType(), True),
    StructField("patient_id", IntegerType(), True),
    StructField("diagnosis_code", StringType(), True),
    StructField("diagnosis_description", StringType(), True),
    StructField("primary_diagnosis", IntegerType(), True)
])

df_diagnoses_raw = spark.read.schema(diagnoses_schema).csv(
    "/Volumes/workspace/healthcare1_workspace/healthcare1_volume/diagnoses.csv",  # UPDATE THIS PATH
    header=True
).withColumn("ingest_date", F.current_timestamp()) \
 .withColumn("source_file", F.lit("diagnoses.csv"))

df_diagnoses_raw.write.format("delta").mode("overwrite").save(
    f"{bronze_path}diagnoses"
)
print(f"✓ Bronze Diagnoses: {df_diagnoses_raw.count()} records loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Load Lab Results Data (Raw)

# COMMAND ----------

lab_schema = StructType([
    StructField("lab_id", IntegerType(), True),
    StructField("patient_id", IntegerType(), True),
    StructField("test_name", StringType(), True),
    StructField("test_value", DoubleType(), True),
    StructField("test_date", StringType(), True),
    StructField("reference_range", StringType(), True)
])

df_labs_raw = spark.read.schema(lab_schema).csv(
    "/Volumes/workspace/healthcare1_workspace/healthcare1_volume/lab_results.csv",  # UPDATE THIS PATH
    header=True
).withColumn("ingest_date", F.current_timestamp()) \
 .withColumn("source_file", F.lit("lab_results.csv"))

df_labs_raw.write.format("delta").mode("overwrite").save(
    f"{bronze_path}lab_results"
)
print(f"✓ Bronze Lab Results: {df_labs_raw.count()} records loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Load Medications Data (Raw)

# COMMAND ----------

meds_schema = StructType([
    StructField("medication_id", IntegerType(), True),
    StructField("patient_id", IntegerType(), True),
    StructField("medication_name", StringType(), True),
    StructField("dosage", StringType(), True),
    StructField("frequency", StringType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True)
])

df_meds_raw = spark.read.schema(meds_schema).csv(
    "/Volumes/workspace/healthcare1_workspace/healthcare1_volume/medications.csv",  # UPDATE THIS PATH
    header=True
).withColumn("ingest_date", F.current_timestamp()) \
 .withColumn("source_file", F.lit("medications.csv"))

df_meds_raw.write.format("delta").mode("overwrite").save(
    f"{bronze_path}medications"
)
print(f"✓ Bronze Medications: {df_meds_raw.count()} records loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SILVER LAYER: Clean, Validate, Deduplicate

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Silver Layer: Clean Patients (Deduplication)

# COMMAND ----------

# Read Bronze
df_patients_bronze = spark.read.format("delta").load(f"{bronze_path}patients")

# STEP 1: Standardize data types and formats
df_patients_clean = df_patients_bronze.select(
    F.col("patient_id"),
    F.col("first_name"),
    F.col("last_name"),
    # Parse DOB in multiple formats
    F.when(
        F.col("date_of_birth").rlike(r"^\d{2}/\d{2}/\d{4}$"),
        F.to_date(F.col("date_of_birth"), "MM/dd/yyyy")
    ).otherwise(
        F.to_date(F.col("date_of_birth"), "yyyy-MM-dd")
    ).alias("date_of_birth"),
    F.col("age"),
    F.col("gender"),
    F.to_date(F.col("admission_date"), "yyyy-MM-dd").alias("admission_date"),
    F.to_date(F.col("discharge_date"), "yyyy-MM-dd").alias("discharge_date"),
    F.col("length_of_stay"),
    F.col("readmitted_30_days"),
    F.current_timestamp().alias("processed_date"),
    F.lit(None).alias("data_quality_issue")  # Will populate below
)

# STEP 2: Data Quality Checks
df_patients_quality = df_patients_clean.withColumn(
    "data_quality_issue",
    F.when(F.col("first_name").isNull(), F.lit("null_first_name"))
     .when(F.col("last_name").isNull(), F.lit("null_last_name"))
     .when(F.col("age") < 0, F.lit("invalid_age"))
     .when(F.col("age") > 120, F.lit("invalid_age"))
     .when(F.col("admission_date").isNull(), F.lit("null_admission_date"))
     .when(F.col("length_of_stay") <= 0, F.lit("invalid_los"))
     .otherwise(F.lit(None))
)

# STEP 3: Deduplication using fuzzy matching
# Create a fingerprint for matching: FIRST_NAME + LAST_NAME + DOB
df_for_dedup = df_patients_quality.withColumn(
    "patient_fingerprint",
    F.concat_ws("|",
        F.upper(F.col("first_name")),
        F.upper(F.col("last_name")),
        F.col("date_of_birth")
    )
)

# Assign unique patient_key to deduplicated records
# Keep the first occurrence, flag duplicates
window_spec = Window.partitionBy("patient_fingerprint").orderBy(F.col("patient_id"))
df_deduped = df_for_dedup.withColumn(
    "row_num", F.row_number().over(window_spec)
).withColumn(
    "patient_key", F.dense_rank().over(Window.orderBy("patient_fingerprint"))
).withColumn(
    "is_duplicate", F.when(F.col("row_num") > 1, F.lit(1)).otherwise(F.lit(0))
)

# Select final silver schema
df_patients_silver = df_deduped.select(
    F.col("patient_key"),
    F.col("patient_id").alias("original_patient_id"),
    F.col("first_name"),
    F.col("last_name"),
    F.col("date_of_birth"),
    F.col("age"),
    F.col("gender"),
    F.col("admission_date"),
    F.col("discharge_date"),
    F.col("length_of_stay"),
    F.col("readmitted_30_days"),
    F.col("is_duplicate"),
    F.col("data_quality_issue"),
    F.col("processed_date")
)

# Save to Silver
df_patients_silver.write.format("delta").mode("overwrite").save(
    f"{silver_path}patients"
)

print(f"✓ Silver Patients: {df_patients_silver.count()} records processed")
print(f"  - Unique patients (patient_key): {df_patients_silver.select('patient_key').distinct().count()}")
print(f"  - Duplicates detected: {df_patients_silver.filter(F.col('is_duplicate') == 1).count()}")
print(f"  - Records with quality issues: {df_patients_silver.filter(F.col('data_quality_issue').isNotNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Silver Layer: Clean Diagnoses (Standardize Codes)

# COMMAND ----------

df_diagnoses_bronze = spark.read.format("delta").load(f"{bronze_path}diagnoses")

# Standardize diagnosis codes to uppercase and remove special characters
df_diagnoses_clean = df_diagnoses_bronze.select(
    F.col("diagnosis_id"),
    F.col("patient_id"),
    # Standardize diagnosis code (uppercase, remove dashes, remove dots)
    F.regexp_replace(
        F.upper(F.col("diagnosis_code")),
        r"[-.]",
        ""
    ).alias("diagnosis_code_standard"),
    F.col("diagnosis_code").alias("diagnosis_code_raw"),
    F.col("diagnosis_description"),
    F.col("primary_diagnosis"),
    F.current_timestamp().alias("processed_date"),
    F.lit(None).alias("data_quality_issue")
).withColumn(
    "data_quality_issue",
    F.when(F.col("diagnosis_code_standard") == "", F.lit("empty_code"))
     .when(F.col("diagnosis_description").isNull(), F.lit("null_description"))
     .otherwise(F.lit(None))
)

df_diagnoses_silver = df_diagnoses_clean.select(
    F.col("diagnosis_id"),
    F.col("patient_id"),
    F.col("diagnosis_code_standard").alias("diagnosis_code"),
    F.col("diagnosis_code_raw"),
    F.col("diagnosis_description"),
    F.col("primary_diagnosis"),
    F.col("data_quality_issue"),
    F.col("processed_date")
)

df_diagnoses_silver.write.format("delta").mode("overwrite").save(
    f"{silver_path}diagnoses"
)

print(f"✓ Silver Diagnoses: {df_diagnoses_silver.count()} records processed")
print(f"  - Records with quality issues: {df_diagnoses_silver.filter(F.col('data_quality_issue').isNotNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Silver Layer: Clean Lab Results (Outlier Detection)

# COMMAND ----------

df_labs_bronze = spark.read.format("delta").load(f"{bronze_path}lab_results")

# Detect outliers using IQR (Interquartile Range) method
# Calculate quartiles grouped by test_name
window_by_test = Window.partitionBy("test_name")

df_labs_stats = df_labs_bronze.withColumn(
    "q1", F.percentile_approx(F.col("test_value"), 0.25).over(window_by_test)
).withColumn(
    "q3", F.percentile_approx(F.col("test_value"), 0.75).over(window_by_test)
).withColumn(
    "iqr", F.col("q3") - F.col("q1")
).withColumn(
    "lower_bound", F.col("q1") - (1.5 * F.col("iqr"))
).withColumn(
    "upper_bound", F.col("q3") + (1.5 * F.col("iqr"))
).withColumn(
    "is_outlier",
    F.when(
        (F.col("test_value") < F.col("lower_bound")) |
        (F.col("test_value") > F.col("upper_bound")),
        F.lit(1)
    ).otherwise(F.lit(0))
).withColumn(
    "test_date_parsed", F.to_date(F.col("test_date"), "yyyy-MM-dd")
).withColumn(
    "data_quality_issue",
    F.when(F.col("is_outlier") == 1, F.lit("outlier_detected"))
     .when(F.col("test_value") < 0, F.lit("negative_value"))
     .when(F.col("test_date_parsed").isNull(), F.lit("invalid_date"))
     .otherwise(F.lit(None))
)

df_labs_silver = df_labs_stats.select(
    F.col("lab_id"),
    F.col("patient_id"),
    F.col("test_name"),
    F.col("test_value"),
    F.col("test_date_parsed").alias("test_date"),
    F.col("reference_range"),
    F.col("is_outlier"),
    F.col("data_quality_issue"),
    F.current_timestamp().alias("processed_date")
)

df_labs_silver.write.format("delta").mode("overwrite").save(
    f"{silver_path}lab_results"
)

print(f"✓ Silver Lab Results: {df_labs_silver.count()} records processed")
print(f"  - Outliers detected: {df_labs_silver.filter(F.col('is_outlier') == 1).count()}")
print(f"  - Records with quality issues: {df_labs_silver.filter(F.col('data_quality_issue').isNotNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Silver Layer: Clean Medications (Standardization)

# COMMAND ----------

df_meds_bronze = spark.read.format("delta").load(f"{bronze_path}medications")

# Standardize medication names
# Map common misspellings to standard names
medication_mapping = {
    "METFORMIN": ["METFORMIN", "METFORMINE"],
    "LISINOPRIL": ["LISINOPRIL", "LISINOPREL"],
    "ATORVASTATIN": ["ATORVASTATIN", "ATORVASTINE"],
    "OMEPRAZOLE": ["OMEPRAZOLE", "OMEPRAZOL"],
    "ALBUTEROL": ["ALBUTEROL", "ALBUTEROL INHALER"],
    "ASPIRIN": ["ASPIRIN", "ASA"]
}

# Create standardization logic
df_meds_standardized = df_meds_bronze.withColumn(
    "medication_name_standard",
    F.when(F.upper(F.col("medication_name")).rlike("METFORMIN"), F.lit("Metformin"))
     .when(F.upper(F.col("medication_name")).rlike("LISINOPRIL"), F.lit("Lisinopril"))
     .when(F.upper(F.col("medication_name")).rlike("ATORVASTATIN"), F.lit("Atorvastatin"))
     .when(F.upper(F.col("medication_name")).rlike("OMEPRAZOLE"), F.lit("Omeprazole"))
     .when(F.upper(F.col("medication_name")).rlike("ALBUTEROL"), F.lit("Albuterol"))
     .when(F.upper(F.col("medication_name")).rlike("ASPIRIN|ASA"), F.lit("Aspirin"))
     .otherwise(F.col("medication_name"))
).withColumn(
    "start_date_parsed", F.to_date(F.col("start_date"), "yyyy-MM-dd")
).withColumn(
    "end_date_parsed", F.to_date(F.col("end_date"), "yyyy-MM-dd")
).withColumn(
    "data_quality_issue",
    F.when(F.col("medication_name_standard") == F.col("medication_name"), F.lit(None))
     .otherwise(F.lit("standardized_name"))
)

df_meds_silver = df_meds_standardized.select(
    F.col("medication_id"),
    F.col("patient_id"),
    F.col("medication_name_standard").alias("medication_name"),
    F.col("medication_name").alias("medication_name_raw"),
    F.col("dosage"),
    F.col("frequency"),
    F.col("start_date_parsed").alias("start_date"),
    F.col("end_date_parsed").alias("end_date"),
    F.col("data_quality_issue"),
    F.current_timestamp().alias("processed_date")
)

df_meds_silver.write.format("delta").mode("overwrite").save(
    f"{silver_path}medications"
)

print(f"✓ Silver Medications: {df_meds_silver.count()} records processed")
print(f"  - Standardized names: {df_meds_silver.filter(F.col('data_quality_issue').isNotNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. GOLD LAYER: Feature Engineering

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Create Patient Master Table (Unified View)

# COMMAND ----------

# Read Silver tables
df_patients_silver = spark.read.format("delta").load(f"{silver_path}patients")
df_diagnoses_silver = spark.read.format("delta").load(f"{silver_path}diagnoses")
df_labs_silver = spark.read.format("delta").load(f"{silver_path}lab_results")
df_meds_silver = spark.read.format("delta").load(f"{silver_path}medications")

# Create patient master: use patient_key from deduped patients
df_patient_master = df_patients_silver.select(
    F.col("patient_key"),
    F.col("original_patient_id"),
    F.col("first_name"),
    F.col("last_name"),
    F.col("age"),
    F.col("gender"),
    F.col("admission_date"),
    F.col("discharge_date"),
    F.col("length_of_stay"),
    F.col("readmitted_30_days").alias("target_readmitted_30_days")
)

print(f"✓ Patient Master: {df_patient_master.count()} unique patients")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Engineer Features from Diagnoses

# COMMAND ----------

# Create diagnosis features
df_diagnosis_features = df_diagnoses_silver.filter(
    F.col("data_quality_issue").isNull()  # Exclude bad records
).groupBy("patient_id").agg(
    F.count("diagnosis_id").alias("num_diagnoses"),
    F.sum(F.when(F.col("diagnosis_code").rlike("E11"), 1).otherwise(0)).alias("has_diabetes"),
    F.sum(F.when(F.col("diagnosis_code").rlike("I10|I5"), 1).otherwise(0)).alias("has_heart_disease"),
    F.sum(F.when(F.col("diagnosis_code").rlike("J44"), 1).otherwise(0)).alias("has_copd"),
    F.sum(F.when(F.col("diagnosis_code").rlike("N18"), 1).otherwise(0)).alias("has_ckd"),
    F.sum(F.when(F.col("diagnosis_code").rlike("F41"), 1).otherwise(0)).alias("has_anxiety"),
    F.collect_set("diagnosis_code").alias("all_diagnosis_codes")
)

# Convert indicators to binary (0/1)
df_diagnosis_features = df_diagnosis_features.select(
    "*",
    F.when(F.col("has_diabetes") > 0, 1).otherwise(0).alias("has_diabetes_flag")
).drop("has_diabetes").withColumnRenamed("has_diabetes_flag", "has_diabetes")

for col_name in ["has_heart_disease", "has_copd", "has_ckd", "has_anxiety"]:
    df_diagnosis_features = df_diagnosis_features.withColumn(
        col_name,
        F.when(F.col(col_name) > 0, 1).otherwise(0)
    )

# Count chronic conditions
df_diagnosis_features = df_diagnosis_features.withColumn(
    "num_chronic_conditions",
    F.col("has_diabetes") + F.col("has_heart_disease") + F.col("has_copd") + F.col("has_ckd")
)

print(f"✓ Diagnosis Features: {df_diagnosis_features.count()} patients")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Engineer Features from Lab Results

# COMMAND ----------

# Create lab result features
df_lab_features = df_labs_silver.filter(
    (F.col("data_quality_issue").isNull()) &  # Exclude bad records
    (F.col("is_outlier") == 0)  # Exclude outliers
).groupBy("patient_id").agg(
    F.avg(F.when(F.col("test_name") == "Hemoglobin", F.col("test_value"))).alias("avg_hemoglobin"),
    F.avg(F.when(F.col("test_name") == "Glucose", F.col("test_value"))).alias("avg_glucose"),
    F.avg(F.when(F.col("test_name") == "WBC", F.col("test_value"))).alias("avg_wbc"),
    F.avg(F.when(F.col("test_name") == "Creatinine", F.col("test_value"))).alias("avg_creatinine"),
    F.avg(F.when(F.col("test_name") == "BUN", F.col("test_value"))).alias("avg_bun"),
    F.count("lab_id").alias("num_lab_tests")
)

# Round to 2 decimals
for col_name in ["avg_hemoglobin", "avg_glucose", "avg_wbc", "avg_creatinine", "avg_bun"]:
    df_lab_features = df_lab_features.withColumn(
        col_name, F.round(F.col(col_name), 2)
    )

print(f"✓ Lab Features: {df_lab_features.count()} patients")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Engineer Features from Medications

# COMMAND ----------

# Create medication features
df_med_features = df_meds_silver.filter(
    F.col("data_quality_issue").isNull()
).groupBy("patient_id").agg(
    F.count("medication_id").alias("num_medications"),
    F.sum(F.when(F.col("medication_name").rlike("Metformin"), 1).otherwise(0)).alias("on_metformin"),
    F.sum(F.when(F.col("medication_name").rlike("Lisinopril"), 1).otherwise(0)).alias("on_ace_inhibitor"),
    F.sum(F.when(F.col("medication_name").rlike("Atorvastatin"), 1).otherwise(0)).alias("on_statin"),
)

# Convert to binary
for col_name in ["on_metformin", "on_ace_inhibitor", "on_statin"]:
    df_med_features = df_med_features.withColumn(
        col_name, F.when(F.col(col_name) > 0, 1).otherwise(0)
    )

print(f"✓ Medication Features: {df_med_features.count()} patients")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Combine All Features into Gold Layer

# COMMAND ----------

# Join all features together
# First, join patient master with diagnosis features
df_gold = df_patient_master.join(
    df_diagnosis_features,
    df_patient_master.original_patient_id == df_diagnosis_features.patient_id,
    "left"
).drop(df_diagnosis_features.patient_id)

# Join with lab features
df_gold = df_gold.join(
    df_lab_features,
    df_gold.original_patient_id == df_lab_features.patient_id,
    "left"
).drop(df_lab_features.patient_id)

# Join with medication features
df_gold = df_gold.join(
    df_med_features,
    df_gold.original_patient_id == df_med_features.patient_id,
    "left"
).drop(df_med_features.patient_id)

# Fill null values with 0 for count columns
count_cols = ["num_diagnoses", "num_chronic_conditions", "num_lab_tests", "num_medications"]
for col_name in count_cols:
    df_gold = df_gold.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))

# Fill null values with 0 for binary columns
binary_cols = ["has_diabetes", "has_heart_disease", "has_copd", "has_ckd", "has_anxiety",
               "on_metformin", "on_ace_inhibitor", "on_statin"]
for col_name in binary_cols:
    df_gold = df_gold.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0)))

# Fill null values with 0.0 for numeric columns
numeric_cols = ["avg_hemoglobin", "avg_glucose", "avg_wbc", "avg_creatinine", "avg_bun"]
for col_name in numeric_cols:
    df_gold = df_gold.withColumn(col_name, F.coalesce(F.col(col_name), F.lit(0.0)))

# Add feature generation timestamp
df_gold = df_gold.withColumn("feature_generation_date", F.current_timestamp())

# Select and order final columns
final_columns = [
    "patient_key", "original_patient_id", "age", "gender",
    "length_of_stay",
    "num_diagnoses", "num_chronic_conditions",
    "has_diabetes", "has_heart_disease", "has_copd", "has_ckd", "has_anxiety",
    "num_medications", "on_metformin", "on_ace_inhibitor", "on_statin",
    "num_lab_tests",
    "avg_hemoglobin", "avg_glucose", "avg_wbc", "avg_creatinine", "avg_bun",
    "target_readmitted_30_days",
    "feature_generation_date"
]

df_gold = df_gold.select(*final_columns)

# Save Gold layer
df_gold.write.format("delta").mode("overwrite").save(
    f"{gold_path}patient_readmission_features"
)

print(f"✓ Gold Layer (Features): {df_gold.count()} patient records")
print(f"\nFeature Engineering Complete!")
print(f"Total Unique Patients: {df_gold.select('patient_key').distinct().count()}")
print(f"Readmission Rate: {df_gold.filter(F.col('target_readmitted_30_days') == 1).count() / df_gold.count() * 100:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. DATA QUALITY METRICS

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Generate Data Quality Report

# COMMAND ----------

print("="*70)
print("DATA QUALITY REPORT")
print("="*70)

# Read Silver tables for quality metrics
df_patients_silver = spark.read.format("delta").load(f"{silver_path}patients")
df_diagnoses_silver = spark.read.format("delta").load(f"{silver_path}diagnoses")
df_labs_silver = spark.read.format("delta").load(f"{silver_path}lab_results")
df_meds_silver = spark.read.format("delta").load(f"{silver_path}medications")

print("\n📊 PATIENTS TABLE")
print(f"  Total Records: {df_patients_silver.count()}")
print(f"  Unique Patients: {df_patients_silver.select('patient_key').distinct().count()}")
print(f"  Duplicates Detected & Flagged: {df_patients_silver.filter(F.col('is_duplicate') == 1).count()}")
print(f"  Deduplication Rate: {(df_patients_silver.count() - df_patients_silver.select('patient_key').distinct().count()) / df_patients_silver.count() * 100:.2f}%")
quality_issues = df_patients_silver.filter(F.col("data_quality_issue").isNotNull()).count()
print(f"  Records with Quality Issues: {quality_issues}")

print("\n📋 DIAGNOSES TABLE")
print(f"  Total Records: {df_diagnoses_silver.count()}")
print(f"  Valid Records (no quality issues): {df_diagnoses_silver.filter(F.col('data_quality_issue').isNull()).count()}")
print(f"  Records with Standardized Codes: {df_diagnoses_silver.filter(F.col('diagnosis_code_raw') != F.col('diagnosis_code')).count()}")

print("\n🧪 LAB RESULTS TABLE")
print(f"  Total Records: {df_labs_silver.count()}")
print(f"  Outliers Detected: {df_labs_silver.filter(F.col('is_outlier') == 1).count()}")
print(f"  Outlier Percentage: {df_labs_silver.filter(F.col('is_outlier') == 1).count() / df_labs_silver.count() * 100:.2f}%")
print(f"  Valid Records: {df_labs_silver.filter((F.col('is_outlier') == 0) & (F.col('data_quality_issue').isNull())).count()}")

print("\n💊 MEDICATIONS TABLE")
print(f"  Total Records: {df_meds_silver.count()}")
print(f"  Standardized Medication Names: {df_meds_silver.filter(F.col('medication_name_raw') != F.col('medication_name')).count()}")
print(f"  Valid Records: {df_meds_silver.filter(F.col('data_quality_issue').isNull()).count()}")

print("\n" + "="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Save Quality Metrics Table

# COMMAND ----------

# Create a metrics DataFrame for future reference
metrics_data = [
    ("patients_total", df_patients_silver.count()),
    ("patients_unique", df_patients_silver.select('patient_key').distinct().count()),
    ("patients_duplicates", df_patients_silver.filter(F.col('is_duplicate') == 1).count()),
    ("diagnoses_total", df_diagnoses_silver.count()),
    ("labs_total", df_labs_silver.count()),
    ("labs_outliers", df_labs_silver.filter(F.col('is_outlier') == 1).count()),
    ("medications_total", df_meds_silver.count()),
]

df_metrics = spark.createDataFrame(metrics_data, ["metric", "value"]).withColumn(
    "generated_at", F.current_timestamp()
)

df_metrics.write.format("delta").mode("overwrite").save(
    f"{gold_path}data_quality_metrics"
)

print("✓ Quality metrics saved to Gold layer")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. VERIFY GOLD LAYER OUTPUT

# COMMAND ----------

# Read the final Gold layer table
df_gold_final = spark.read.format("delta").load(f"{gold_path}patient_readmission_features")

print("\n" + "="*70)
print("GOLD LAYER - FINAL FEATURES TABLE")
print("="*70)
print(f"\nShape: {df_gold_final.count()} patients × {len(df_gold_final.columns)} features")
print(f"\nColumns:")
for col in df_gold_final.columns:
    print(f"  - {col}")

print(f"\nTarget Variable Distribution:")
print(f"  - Readmitted (1): {df_gold_final.filter(F.col('target_readmitted_30_days') == 1).count()}")
print(f"  - Not Readmitted (0): {df_gold_final.filter(F.col('target_readmitted_30_days') == 0).count()}")

print(f"\nFeature Statistics:")
df_gold_final.describe().show()

print("\n✅ PHASE 1: DATA ENGINEERING COMPLETE!")
print("\nNext Steps:")
print("  1. Review the deduplication results")
print("  2. Analyze feature distributions")
print("  3. Prepare data for ML model (Phase 2)")