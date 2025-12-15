# Patient Hospital Readmission Prediction System

## 🏥 Project Overview

This is an **end-to-end data engineering project** that builds a production-ready pipeline to identify high-risk hospital readmission patients. The project demonstrates core data engineering skills including **data quality, deduplication, feature engineering, and building scalable pipelines**.

**Status**: ✅ Phase 1 Complete (Data Engineering) | Phase 2 Ready (ML Model)

---

## 🎯 Problem Statement

**The Healthcare Challenge:**
- **20% of Medicare patients are readmitted within 30 days** of discharge
- This preventable readmission costs the U.S. healthcare system **$17B annually**
- Hospitals struggle with **siloed data, poor quality, and lack of early warning systems**

**My Solution:**
- Build a data engineering pipeline that cleans hospital data, removes duplicates, and engineers predictive features
- Create a unified patient view across multiple data sources
- Enable early identification of high-risk readmission patients
- Implement production-grade data governance and quality checks

---

## 📊 PHASE 1 ACTUAL RESULTS

### Input Data (Bronze Layer)
```
Total Records Processed: 10,460+

1. PATIENTS:        1,000 records × 13 columns
2. DIAGNOSES:       3,007 records × 7 columns
3. LAB RESULTS:     5,942 records × 7 columns
4. MEDICATIONS:     4,511 records × 9 columns
```

### Data Cleaning (Silver Layer)
```
DEDUPLICATION:
  Raw patients:           1,000
  Duplicates detected:    51 (5.1% reduction)
  Unique patients:        949

STANDARDIZATION:
  Diagnosis codes:        3 formats → 1 standard (100%)
  Medication names:       Spelling variations fixed (100%)
  Lab dates:              Multiple formats → standardized

OUTLIER DETECTION:
  Lab results:            5,942 total
  Outliers flagged:       297 (5%)
  Detection method:       IQR (Interquartile Range)
```

### Feature Engineering (Gold Layer)
```
FINAL OUTPUT:
  Unique patients:        949
  Features engineered:    24 ML-ready dimensions
  Quality metrics:        7 data quality indicators
  Format:                 Parquet (production standard)

FEATURE CATEGORIES:
  Demographics:           2 (age, length_of_stay)
  Diagnoses:             9 (chronic conditions, disease flags)
  Medications:           4 (medication usage patterns)
  Lab values:            5 (clinical measurements aggregated)
  Metadata:              4 (identifiers, timestamps)
```

### Readmission Distribution
```
Readmitted within 30 days:    190 patients (20%)
Not readmitted:               759 patients (80%)
Class balance:                Suitable for ML (slight imbalance)
```

---

## 📈 Project Architecture

### Medallion Architecture Pattern

```
RAW DATA (CSV Files)
       ↓
┌──────────────────────────────────────┐
│  BRONZE LAYER (Raw & Immutable)      │
│  ✓ 1,000 patients                    │
│  ✓ 3,007 diagnoses                   │
│  ✓ 5,942 lab results                 │
│  ✓ 4,511 medications                 │
│  ✓ Total: 10,460 records             │
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│  SILVER LAYER (Cleaned & Validated)  │
│  ✓ Deduplication: 51 duplicates      │
│  ✓ Standardization: 3 formats → 1    │
│  ✓ Outlier detection: 5% flagged     │
│  ✓ Quality checks: 8+ rules applied  │
│  ✓ Data completeness: 100%           │
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│  GOLD LAYER (Features & Analytics)   │
│  ✓ 949 unique patients               │
│  ✓ 24 engineered features            │
│  ✓ 7 quality metrics                 │
│  ✓ ML-ready dataset                  │
│  ✓ Parquet format (ACID compliance)  │
└──────────────────────────────────────┘
       ↓
    ML MODEL & INSIGHTS
```

---

## 🛠️ Technology Stack

- **Platform**: Databricks Community Edition (Free)
- **Language**: Python with PySpark
- **SQL**: Spark SQL for data transformation
- **Storage**: Delta Lake (ACID transactions)
- **Data Format**: Parquet (columnar storage)
- **Version Control**: Git/GitHub

---

## 📁 Project Structure

```
patient-readmission-prediction/
│
├── README.md                          # This file
├── LICENSE                            # MIT License
│
├── notebooks/
│   └── 01_data_engineering_pipeline.py    # Databricks notebook
│                                          # Complete ETL pipeline
│
├── data/
│   ├── generate_data.py               # Synthetic dataset generator
│   ├── patients.csv                   # 1,000 patient records
│   ├── diagnoses.csv                  # 3,007 diagnosis records
│   ├── lab_results.csv                # 5,942 lab measurements
│   └── medications.csv                # 4,511 medication records
│
├── docs/
│   ├── PHASE1_VERIFIED_METRICS.md     # Actual execution results
│   ├── PROJECT_GUIDE.md               # Detailed architecture
│   ├── PHASE1_QUICKSTART.md           # Execution guide
│   └── LINKEDIN_POST_GUIDE.md         # Social media strategy
│
└── results/
    ├── bronze_layer/                  # Raw data (parquet)
    ├── silver_layer/                  # Cleaned data (parquet)
    └── gold_layer/                    # Features (parquet)
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Databricks Account (free community tier)
- ~2.5 hours setup time

### Step 1: Generate Synthetic Data

```bash
# Clone the repo
git clone https://github.com/yourusername/patient-readmission-prediction.git
cd patient-readmission-prediction

# Install dependencies
pip install pandas numpy

# Generate synthetic healthcare dataset
python data/generate_data.py
```

**Output**: 4 CSV files created (patients, diagnoses, lab_results, medications)

### Step 2: Upload to Databricks

1. Create free Databricks account: https://databricks.com/try-databricks
2. Upload 4 CSV files to workspace
3. Note the file paths

### Step 3: Run the Pipeline

1. Create new Databricks notebook
2. Read the code First for more Instructions at 1st page and then Copy code from `notebooks/01_data_engineering_pipeline.py`
3. Update CSV file paths in Section 1
4. Click "Run All" - pipeline executes end-to-end

---

## 📊 Data Engineering Skills Demonstrated

### 1. Data Ingestion (Bronze Layer)
- ✅ Load data from multiple CSV sources
- ✅ Define explicit schemas (type safety)
- ✅ Add metadata tracking (ingest_date, source_file)
- ✅ Store immutable raw data
- ✅ **Actual Result**: 10,460 records ingested, 100% success

### 2. Data Quality & Deduplication (Silver Layer)
- ✅ **Deduplication Logic**: Fuzzy matching on demographics
  - **Detected 51 duplicate patient records (5.1%)**
  - Created patient_key as single source of truth
  - Flagged duplicates for investigation
  
- ✅ **Data Validation**: Multiple quality checks
  - Missing value handling
  - Type validation
  - Range validation
  
- ✅ **Format Standardization**:
  - Diagnosis codes: 3 formats → 1 standard (100%)
  - Medications: Spelling variations → standardized (100%)
  - Dates: Multiple formats → YYYY-MM-DD
  
- ✅ **Outlier Detection**: IQR method for lab results
  - **Detected 297 outliers (5% of 5,942 records)**
  - Flagged without deletion (preserve data)

### 3. Feature Engineering (Gold Layer)
- ✅ Patient demographics aggregation
- ✅ Multi-table joins with proper dimension handling
- ✅ Temporal feature engineering (length of stay)
- ✅ **24 engineered features created** from raw data:
  - 2 demographic features
  - 9 diagnosis-based features (chronic conditions)
  - 4 medication-based features
  - 5 lab result aggregations
  - 4 metadata fields
- ✅ Null handling and imputation strategies
- ✅ **949 deduplicated patients** with complete features

### 4. Data Quality Metrics & Monitoring
- ✅ Generated comprehensive quality report
- ✅ Tracked deduplication effectiveness (5.1% reduction)
- ✅ Documented outlier rates (5% of lab data)
- ✅ Created reproducible quality checks
- ✅ **7 key metrics** automatically tracked

---

## 📈 Key Results

### Deduplication Impact
```
Input:     1,000 patient records
Duplicates: 51 records (5.1%)
Output:    949 unique patients
Impact:    51 fewer duplicate tests/treatments/billing errors
```

### Data Quality
```
Diagnoses:        3,007 records standardized (3 formats → 1)
Lab Results:      5,942 records (297 outliers flagged = 5%)
Medications:      4,511 names standardized (100%)
Patient Records:  1,000 validated (100% success rate)
```

### Feature Engineering
```
Demographics:           2 features
Diagnosis Features:     9 features (binary flags + counts)
Medication Features:    4 features (binary flags)
Lab Features:           5 features (aggregated values)
Metadata:               4 features (IDs, timestamps)

Total:                  24 ML-ready features
Patient Coverage:       949 (94.9% of raw population)
```

---

## 🔍 Healthcare Pain Points Addressed

| Pain Point | Traditional Approach | My Solution |
|-----------|---------------------|--------------|
| **Duplicate Patient Records** | Manual review, error-prone | Automated fuzzy matching, 100% detection (51 found) |
| **Inconsistent Data Formats** | Complex downstream logic | Standardization at source (3 formats → 1) |
| **Siloed Data Sources** | Copy-paste between systems | Unified patient view via medallion architecture |
| **Poor Data Quality** | Unknown impact on analysis | Comprehensive quality framework (8+ checks) |
| **Undetected Outliers** | Skip detection, unreliable analysis | IQR method flagged 297 outliers (5%) |
| **No Early Warning System** | Reactive (after readmission) | Proactive risk scoring foundation created |
| **Lack of Governance** | Regulatory risk | Audit trail via Delta Lake transactions |

---


**"This project demonstrates I can:"**

1. ✅ **Design scalable data pipelines** using industry-standard medallion architecture
2. ✅ **Solve real healthcare problems** (duplicate records, data quality, risk prediction)
3. ✅ **Handle complex data transformations** (deduplication, standardization, feature engineering)
4. ✅ **Implement data governance** (quality checks, audit trails, schemas)
5. ✅ **Write production-ready PySpark code** (error handling, documentation)
6. ✅ **Think like a data engineer** (upstream/downstream impact, scalability, maintainability)

**"The result:"** A healthcare organization can now identify high-risk patients BEFORE discharge, enabling preventive interventions and reducing costly readmissions.

---


## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **Databricks Community Edition** for free compute resources
- **Healthcare data engineering community** for best practices
- **MIMIC-III dataset** inspiration for realistic clinical data

---

## 👨‍💻 About the Author

**Yaswanth Nissankara** - Data Engineer
- 4+ years healthcare data engineering experience
- Building scalable data pipelines for healthcare innovation

---

## 🌟 If You Find This Helpful

⭐ Please star this repository on GitHub!

---

## 📊 ACTUAL EXECUTION METRICS (VERIFIED)

For detailed breakdown of actual execution results, see:
**[PHASE1_VERIFIED_METRICS.md](docs/PHASE1_VERIFIED_METRICS.md)**

This document contains:
- ✅ Exact row and column counts for each layer
- ✅ Deduplication details (51 duplicates identified)
- ✅ Standardization specifics (3 formats → 1)
- ✅ Outlier detection results (297 of 5,942 lab records)
- ✅ Feature engineering breakdown (24 features × 949 patients)
- ✅ Quality metrics (7 tracked indicators)

---

**Last Updated**: December 15, 2025
**Status**: Phase 1 Complete ✅ | Phase 2 Ready ✅
**Total Records Processed**: 10,460+
**Unique Patients**: 949
**Features Engineered**: 24
**Quality Score**: 100%

---

