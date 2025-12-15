"""
Generate Synthetic Healthcare Dataset for Readmission Prediction Project
========================================================================

This script creates realistic, HIPAA-safe synthetic patient data that includes:
- Patient demographics with intentional duplicates (to show deduplication skill)
- Diagnoses with inconsistent formatting (to show data quality handling)
- Lab results with outliers (to show outlier detection)
- Medications with spelling variations (to show standardization)
- Readmission outcomes for ML training

Usage:
    python generate_healthcare_data.py

Output Files:
    - patients.csv (1000 records with 10% intentional duplicates)
    - diagnoses.csv (3000+ diagnosis records)
    - lab_results.csv (5000+ lab result records)
    - medications.csv (2000+ medication records)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

print("Generating synthetic healthcare dataset...\n")

# ============================================================================
# 1. GENERATE PATIENTS TABLE (WITH INTENTIONAL DUPLICATES)
# ============================================================================
print("1. Generating patients data...")

n_patients = 900  # We'll add 100 duplicates to reach 1000

first_names = ['John', 'Mary', 'Robert', 'Patricia', 'Michael', 'Jennifer', 
               'William', 'Linda', 'David', 'Barbara', 'Richard', 'Susan',
               'Joseph', 'Jessica', 'James', 'Sarah', 'Charles', 'Karen']

last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia',
              'Miller', 'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez',
              'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson']

patients_data = []

for i in range(1, n_patients + 1):
    dob = datetime(1930, 1, 1) + timedelta(days=random.randint(0, 365*80))
    admission_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
    length_of_stay = random.randint(1, 30)
    discharge_date = admission_date + timedelta(days=length_of_stay)
    
    # 20% readmission rate (realistic)
    readmitted = 1 if random.random() < 0.20 else 0
    
    patients_data.append({
        'patient_id': i,
        'first_name': random.choice(first_names),
        'last_name': random.choice(last_names),
        'date_of_birth': dob.strftime('%m/%d/%Y'),  # Intentional format inconsistency
        'age': (admission_date.year - dob.year),
        'gender': random.choice(['M', 'F', 'U']),
        'admission_date': admission_date.strftime('%Y-%m-%d'),
        'discharge_date': discharge_date.strftime('%Y-%m-%d'),
        'length_of_stay': length_of_stay,
        'readmitted_30_days': readmitted
    })

# Add intentional duplicates (10% of dataset) - shows deduplication need
print(f"   - Created {n_patients} unique patient records")
print(f"   - Adding intentional duplicates to demonstrate deduplication skills...")

duplicate_count = 100
for _ in range(duplicate_count):
    original = random.choice(patients_data[:-100])  # Don't duplicate duplicates
    duplicate = original.copy()
    duplicate['patient_id'] = n_patients + _ + 1
    # Intentionally introduce small variations to test fuzzy matching
    if random.random() < 0.5:
        duplicate['first_name'] = original['first_name'][:-1]  # Typo
    patients_data.append(duplicate)

df_patients = pd.DataFrame(patients_data)
df_patients.to_csv('patients.csv', index=False)
print(f"   ✓ Saved: patients.csv ({len(df_patients)} total records)")

# ============================================================================
# 2. GENERATE DIAGNOSES TABLE (WITH INCONSISTENT ICD-10 FORMATTING)
# ============================================================================
print("\n2. Generating diagnoses data...")

# Real ICD-10 codes with variations
diagnosis_codes = {
    'E11.9': ['E11.9', 'e11.9', 'E11', 'Diabetes Type 2'],  # Intentional variations
    'I10': ['I10', 'i10', 'I-10', 'Hypertension'],
    'J44.9': ['J44.9', 'j44.9', 'COPD'],
    'I50.9': ['I50.9', 'i50.9', 'Heart Failure'],
    'F41.1': ['F41.1', 'f41.1', 'Anxiety'],
    'K21.9': ['K21.9', 'k21.9', 'GERD'],
    'N18.3': ['N18.3', 'n18.3', 'Chronic Kidney Disease'],
    'M79.3': ['M79.3', 'm79.3', 'Myalgia'],
}

diagnoses_descriptions = {
    'E11.9': 'Type 2 diabetes mellitus without complications',
    'I10': 'Essential hypertension',
    'J44.9': 'Chronic obstructive pulmonary disease',
    'I50.9': 'Heart failure, unspecified',
    'F41.1': 'Generalized anxiety disorder',
    'K21.9': 'Unspecified reflux esophagitis',
    'N18.3': 'Chronic kidney disease, stage 3b',
    'M79.3': 'Myalgia',
}

diagnoses_data = []
diagnosis_id = 1

for _, patient in df_patients.iterrows():
    # Each patient has 2-4 diagnoses
    num_diagnoses = random.randint(2, 4)
    selected_codes = random.sample(list(diagnosis_codes.keys()), num_diagnoses)
    
    for idx, code in enumerate(selected_codes):
        # Use variations to show data quality issues
        selected_variation = random.choice(diagnosis_codes[code])
        
        diagnoses_data.append({
            'diagnosis_id': diagnosis_id,
            'patient_id': patient['patient_id'],
            'diagnosis_code': selected_variation,  # Inconsistent formatting
            'diagnosis_description': diagnoses_descriptions[code],
            'primary_diagnosis': 1 if idx == 0 else 0
        })
        diagnosis_id += 1

df_diagnoses = pd.DataFrame(diagnoses_data)
df_diagnoses.to_csv('diagnoses.csv', index=False)
print(f"   ✓ Saved: diagnoses.csv ({len(df_diagnoses)} records)")

# ============================================================================
# 3. GENERATE LAB RESULTS TABLE (WITH OUTLIERS)
# ============================================================================
print("\n3. Generating lab results data...")

lab_tests = {
    'Hemoglobin': {'normal_range': '12-17 g/dL', 'mean': 14.5, 'std': 1.5},
    'WBC': {'normal_range': '4.5-11.0 K/uL', 'mean': 7.5, 'std': 2.0},
    'Glucose': {'normal_range': '70-100 mg/dL', 'mean': 85, 'std': 15},
    'Creatinine': {'normal_range': '0.7-1.3 mg/dL', 'mean': 1.0, 'std': 0.3},
    'BUN': {'normal_range': '7-20 mg/dL', 'mean': 15, 'std': 5},
}

lab_data = []
lab_id = 1

for _, patient in df_patients.iterrows():
    # Each patient has 4-8 lab tests during stay
    num_tests = random.randint(4, 8)
    selected_tests = random.choices(list(lab_tests.keys()), k=num_tests)
    
    admission = datetime.strptime(patient['admission_date'], '%Y-%m-%d')
    discharge = datetime.strptime(patient['discharge_date'], '%Y-%m-%d')
    
    for test in selected_tests:
        test_info = lab_tests[test]
        
        # Generate realistic values with occasional outliers
        if random.random() < 0.05:  # 5% outliers
            value = test_info['mean'] * random.uniform(0.5, 2.0)
        else:
            value = np.random.normal(test_info['mean'], test_info['std'])
        
        test_date = admission + timedelta(days=random.randint(0, (discharge - admission).days))
        
        lab_data.append({
            'lab_id': lab_id,
            'patient_id': patient['patient_id'],
            'test_name': test,
            'test_value': round(max(0.1, value), 2),  # No negative values
            'test_date': test_date.strftime('%Y-%m-%d'),
            'reference_range': test_info['normal_range']
        })
        lab_id += 1

df_labs = pd.DataFrame(lab_data)
df_labs.to_csv('lab_results.csv', index=False)
print(f"   ✓ Saved: lab_results.csv ({len(df_labs)} records)")

# ============================================================================
# 4. GENERATE MEDICATIONS TABLE (WITH SPELLING INCONSISTENCIES)
# ============================================================================
print("\n4. Generating medications data...")

medication_variations = {
    'Metformin': ['Metformin', 'metformin', 'Metformine', 'METFORMIN'],
    'Lisinopril': ['Lisinopril', 'Lisinoprel', 'lisinopril', 'LISINOPRIL'],
    'Atorvastatin': ['Atorvastatin', 'atorvastatin', 'Atorvastine', 'ATORVASTATIN'],
    'Omeprazole': ['Omeprazole', 'omeprazole', 'Omeprazol', 'OMEPRAZOLE'],
    'Albuterol': ['Albuterol', 'albuterol', 'Albuterol Inhaler', 'ALBUTEROL'],
    'Aspirin': ['Aspirin', 'aspirin', 'ASA', 'ASPIRIN'],
}

frequencies = ['Once daily', 'Twice daily', 'Three times daily', 'As needed', 'Every 8 hours']
dosages = ['500 mg', '1000 mg', '10 mg', '20 mg', '50 mg', '100 mg', '2 tabs', '1 tab']

medications_data = []
med_id = 1

for _, patient in df_patients.iterrows():
    # Each patient takes 3-6 medications
    num_meds = random.randint(3, 6)
    selected_meds = random.choices(list(medication_variations.keys()), k=num_meds)
    
    admission = datetime.strptime(patient['admission_date'], '%Y-%m-%d')
    discharge = datetime.strptime(patient['discharge_date'], '%Y-%m-%d')
    
    for med in selected_meds:
        # Use variations to show standardization need
        med_name = random.choice(medication_variations[med])
        
        start_date = admission - timedelta(days=random.randint(0, 30))
        end_date = discharge + timedelta(days=random.randint(0, 90))
        
        medications_data.append({
            'medication_id': med_id,
            'patient_id': patient['patient_id'],
            'medication_name': med_name,  # Inconsistent spelling
            'dosage': random.choice(dosages),
            'frequency': random.choice(frequencies),
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d')
        })
        med_id += 1

df_medications = pd.DataFrame(medications_data)
df_medications.to_csv('medications.csv', index=False)
print(f"   ✓ Saved: medications.csv ({len(df_medications)} records)")

# ============================================================================
# 5. SUMMARY STATISTICS
# ============================================================================
print("\n" + "="*70)
print("DATASET SUMMARY")
print("="*70)

print(f"\n📊 Patients: {len(df_patients)} records")
print(f"   - Unique patients: {len(df_patients) - duplicate_count}")
print(f"   - Intentional duplicates: {duplicate_count} (to show deduplication)")
print(f"   - Readmission rate: {df_patients['readmitted_30_days'].mean()*100:.1f}%")
print(f"   - Avg length of stay: {df_patients['length_of_stay'].mean():.1f} days")

print(f"\n📋 Diagnoses: {len(df_diagnoses)} records")
print(f"   - Avg diagnoses per patient: {len(df_diagnoses)/len(df_patients):.1f}")

print(f"\n🧪 Lab Results: {len(df_labs)} records")
print(f"   - Avg tests per patient: {len(df_labs)/len(df_patients):.1f}")

print(f"\n💊 Medications: {len(df_medications)} records")
print(f"   - Avg medications per patient: {len(df_medications)/len(df_patients):.1f}")

print("\n✅ All data files created successfully!")
print("   Ready to upload to Databricks and start the pipeline.\n")

print("Next steps:")
print("1. Upload CSV files to Databricks workspace")
print("2. Create a new notebook for the ETL pipeline")
print("3. Implement Bronze → Silver → Gold transformations")
