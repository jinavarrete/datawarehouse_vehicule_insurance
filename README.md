# Data Warehouse - Vehicle Insurance

This is a project that aims to simulates the construction of a modern **Data Structure** using a medallion architecture Data Lake (Bronze, Silver, Gold) to process insurance-related data (clients, vehicles, policies, claims, payments) for analytics and business intelligence.

---

## 🗂️ Project Structure

```
├── data_sources/             # Simulated raw CSV files (generated with Faker)
├── scripts/
│   ├── bronze/               # Bronze layer: loads raw CSVs to S3 as Parquet
│   ├── silver/               # Silver layer: cleans and standardizes data
│   ├── gold/                 # Gold layer: creates dimensional and fact tables
│   ├── config/               # AWS credential loader
├── .env                      # Environment variables (bucket, region, AWS keys)
├── Arquitectura.drawio       # Architecture diagram in Drawio
├── requirements.txt          # Python dependencies
└── README.md                 # You're here 👋
```

---

## 🏗️ Architecture

Follows a Medallion Pattern:

- **Bronze**:   Raw CSVs → Parquet (stored in S3)
- **Silver**:   Cleaned & validated (e.g., remove nulls, trim, normalize values)
- **Gold**:     Dimensional and fact tables, ready for BI / consumption

Each layer is saved in a dedicated path in S3:
```
s3://wd-insurance-datalake/bronze/
s3://wd-insurance-datalake/silver/
s3://wd-insurance-datalake/gold/
```

---

## ✅ Implemented Gold Tables

| Table                 | Type       | Description                                |
|----------------------|------------|--------------------------------------------|
| `dim_clients`        | Dimension  | Enriched clients with CRM attributes       |
| `dim_vehicles`       | Dimension  | Vehicles with cleaned data                 |
| `fact_payments`      | Fact       | Payments linked to clients and policies    |
| `fact_client_summary`| Fact       | Aggregated KPIs by client (premium, claims)|


---

## ⚙️ How to Run

1. Clone the repo and create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up your `.env` file:
```dotenv
AWS_ACCESS_KEY_ID=key
AWS_SECRET_ACCESS_KEY=secret
AWS_REGION=region
S3_BUCKET=wd-insurance-datalake
```

4. Generate the raw data:
```bash
python scripts/data_sources/generate_raw_data.py
```

5. Load Bronze layer:
```bash
python -m scripts.bronze.load_bronze
```

6. Load Silver layer:
```bash
python -m scripts.silver.load_silver
```

7. Load Gold layer:
```bash
python -m scripts.gold.load_gold
```

---

## 📊 Consume Layer

Once the Gold layer is complete, data can be consumed via:
- **Amazon Athena** (direct S3 queries)
- **Amazon Redshift** (COPY from S3)
- **Power BI** consumig the Redshift layer

---

## 📌 Status
✅ Bronze, Silver, Gold scripts working with test data.  
📦 S3-ready Parquet layers generated.  
📘 Still evolving: 
    **more fact tables and modeling features to come.**
    **lambda function to COPY from Gold layer to Redshift**
    **modeling and implementation of the Datawarehouse in Redshift**
    **develop a DataViz project to consume the data**

---

## 📫 Contact
Made by Juan Ignacio — Data Engineer

---

> Feel free to fork, improve, or adapt this as a personal portfolio project!
