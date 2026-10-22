# DataStack Setup & Execution Guide

## 1. Prerequisites

- **Python:** 3.12 (Recommended for Apache Spark 3.5.x compatibility)
- **Java:** OpenJDK 11 (LTS)
- **Database:** PostgreSQL 15+ (Local native service or Docker Compose)
- **Docker & Docker Compose:** Optional for containerized services

---

## 2. Environment Setup

### Option A: Native Windows (Using Conda - Recommended)

```powershell
# 1. Create and activate dedicated environment
conda create -n datastack-env python=3.12 openjdk=11 -y
conda activate datastack-env

# 2. Clone repository & install dependencies
git clone https://github.com/VaishnavSreekumar/Datastack.git
cd Datastack
pip install -r requirements.txt
pip install -e .
```

### Option B: Linux / WSL2

```bash
# 1. Update and install prerequisites
sudo apt update && sudo apt install -y openjdk-11-jdk python3.12 python3.12-venv

# 2. Setup virtual environment
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

---

## 3. Configuration

Copy the sample environment file and configure connection settings:

```powershell
cp .env.example .env
```

Ensure `.env` matches your target PostgreSQL instance:
```ini
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=olist_dw
POSTGRES_USER=olist
POSTGRES_PASSWORD=your_password
```

---

## 4. PostgreSQL Warehouse Setup

### Using Docker Compose:
```bash
docker compose up -d
# Check health
docker compose ps
```

### Using Native PostgreSQL:
```sql
CREATE USER olist WITH PASSWORD 'olist_secure_dev_password';
CREATE DATABASE olist_dw OWNER olist;
GRANT ALL PRIVILEGES ON DATABASE olist_dw TO olist;
\c olist_dw
CREATE SCHEMA IF NOT EXISTS dev;
CREATE SCHEMA IF NOT EXISTS public;
GRANT ALL ON SCHEMA dev TO olist;
GRANT ALL ON SCHEMA public TO olist;
```

---

## 5. Running the Pipeline

### Step 1: Run Ingestion (CSV &rarr; Parquet Bronze)
```powershell
python -m datastack.ingest.bronze
```

### Step 2: Run Silver Transformations (Enrichment &rarr; Parquet &amp; Warehouse)
```powershell
python -m datastack.transform.silver
```

### Step 3: Run dbt Gold Models & Data Tests
```powershell
cd olist_dbt
dbt run
dbt test
```

### Step 4: Run Test Suite
```powershell
pytest -v
```

### Step 5: Run Engine Benchmark
```powershell
python scripts/run_benchmark.py
```
