# Workshop 3 — Streaming ETL with Apache Kafka and Machine Learning

**Course:** ETL (G01) — Data Engineering and Artificial Intelligence  
**Universidad Autónoma de Occidente**

---

## Project Description

This project implements a **streaming ETL pipeline** that:
1. Trains a regression model on World Happiness data (2015–2019)
2. Streams records through Apache Kafka
3. Performs real-time inference using a pre-trained ML model
4. Stores raw events and predictions in PostgreSQL
5. Visualizes results in a dashboard

---

## Architecture

```
[Offline Process]
CSV Files → EDA → Cleaning → Feature Engineering → Train Model → model.pkl

[Streaming Process]
CSV Files → Kafka Producer → Kafka Topic → Kafka Consumer
                                                ↓
                                        Store Raw Event
                                                ↓
                                        Validate Schema
                                                ↓
                                        Load model.pkl
                                                ↓
                                       Generate Prediction
                                                ↓
                                       Store in PostgreSQL
                                                ↓
                                         Dashboard & KPIs
```

---

## Folder Structure

```
project/
├── data/
│   ├── raw/           ← Original CSV files (2015–2019)
│   ├── processed/     ← unified.csv after cleaning
│   └── streaming/
├── notebooks/
│   ├── eda.ipynb
│   └── model_training.ipynb
├── kafka/
│   ├── producer.py
│   └── consumer.py
├── models/
│   └── model.pkl
├── sql/
│   ├── create_tables.sql
│   └── kpis.sql
├── dashboards/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Data Cleaning Decisions

> *(Complete this section after your EDA)*

- Column renaming decisions: ...
- Missing value handling: ...
- Outlier treatment: ...
- Schema harmonization: ...

---

## Feature Engineering

Selected features: `gdp`, `family`, `health`, `freedom`, `generosity`, `corruption`

Justification:
- These variables directly capture socioeconomic and social factors that the literature identifies as drivers of happiness.
- `actual_happiness_score` is excluded from features to avoid target leakage.

---

## Kafka Pipeline

- **Topic:** `happiness-predictions`
- **Producer:** reads `unified.csv`, sends one JSON event per row with a 0.5s delay
- **Consumer:** validates schema → stores raw event → runs inference → stores prediction

### Event statuses
| Status | Meaning |
|---|---|
| VALID | Event passed all validations |
| INVALID_SCHEMA | Missing required fields |
| INVALID_VALUES | Non-numeric or negative values |
| PREDICTION_ERROR | Model inference failed |

---

## Database Schema

- `raw_happiness_events` — every Kafka message stored verbatim
- `dim_country` — country dimension
- `dim_date` — year dimension
- `fact_predictions` — prediction results linked to raw events

---

## Dashboard KPIs

1. Average prediction error
2. Predictions by country
3. Predicted vs actual score
4. Prediction trends over time

---

## Execution Instructions

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Place CSV files
Copy `2015.csv` … `2019.csv` into `data/raw/`

### 3. Run EDA and training
```bash
jupyter notebook notebooks/eda.ipynb
jupyter notebook notebooks/model_training.ipynb
```

### 4. Start infrastructure
```bash
docker-compose up -d
```

### 5. Run consumer (background)
```bash
python kafka/consumer.py
```

### 6. Run producer
```bash
python kafka/producer.py
```

### 7. Verify data
```bash
docker exec -it postgres psql -U etl_user -d happiness_db -c "SELECT COUNT(*) FROM fact_predictions;"
```
