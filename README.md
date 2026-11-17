# Debre Berhan University — Department of Information Systems
## Course: InSy4051 — Big Data and Business Analysis 
### Group Assignment: Designing a Simple Data Pipeline and ETL Process (2.2 Million Records)

**Instructor:** Derbew Felasman  
**Academic Year:** 2026 / 2018 E.C.  

---

### Project Group Members

| # | Full Name | Student ID | Department | Assigned Role |
|:---:|:---|:---:|:---:|:---|
| 1 | **Yonas Leykun** | `DBU1601770` | Information Systems | Project Lead & Pipeline Architecture |
| 2 | **Yeabsira Wondwosen** | `DBU1601732` | Information Systems | Data Extraction & Chunking Logic |
| 3 | **Habtamu Getachew** | `DBU1601218` | Information Systems | Data Transformation & Cleaning |
| 4 | **Oumer Sualih** | `DBU1601875` | Information Systems | PostgreSQL Schema Design & Index Tuning |
| 5 | **Melkamu Taddesse** | `DBU1601359` | Information Systems | Verification, Auditing & Technical Reporting |

---

## Deliverables & Documentation

* 📕 **Official Final Report (PDF)**: [`report/BigData_ETL_Process_Group_Assignment.pdf`](report/BigData_ETL_Process_Group_Assignment.pdf)
* 📘 **Official Project Document (DOCX)**: [`report/BigData_ETL_Process_Group_Assignment.docx`](report/BigData_ETL_Process_Group_Assignment.docx)

---

## Repository Structure

```
BigData/
├── .env.example                 # Example database environment variables
├── .gitignore                   # Ignores large raw CSV files (491MB) and passwords
├── config.py                    # Configuration and database connection resolver
├── data/
│   └── raw/                     # Data directory (Raw CSV ignored by git, generated via script)
├── database/
│   ├── schema.sql               # PostgreSQL DDL with tables, constraints & 5 indexes
│   └── verify.sql               # SQL verification and BI analytical queries
├── etl/
│   ├── __init__.py              # Package entry point
│   ├── extract.py               # Chunked data extraction module (100k rows/stream)
│   ├── transform.py             # Cleaning, validation, imputation & deduplication
│   └── load.py                  # High-performance PostgreSQL COPY bulk loader
├── generate_kaggle_dataset.py   # Vectorized generator for 2.2M-row Kaggle dataset
├── report/
│   ├── BigData_ETL_Process_Group_Assignment.pdf      # Official Final PDF Report
│   └── BigData_ETL_Process_Group_Assignment.docx     # Official Word Document
├── requirements.txt                                  # Python dependencies
├── run_etl.py                   # Main CLI execution orchestrator
└── test_pipeline.py             # Standalone pipeline validation script
```

---

## Quick Start Guide

### 1. Install Dependencies
```powershell
python -m pip install -r requirements.txt
```

### 2. Configure PostgreSQL Credentials
Copy `.env.example` to `.env` and set your credentials:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce_dw
DB_USER=postgres
DB_PASSWORD=your_password
```

### 3. Generate the 2.2M-Row Dataset
```powershell
python generate_kaggle_dataset.py
```

### 4. Run the ETL Pipeline
```powershell
python run_etl.py --chunk-size 100000
```

### 5. Run Database Verification Queries
```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -U postgres -d ecommerce_dw -f database/verify.sql
```
