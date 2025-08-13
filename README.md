# Health ETL Pipeline
Data Extraction (E)

An end-to-end data engineering and analytics project using synthetic healthcare data and public health API data.
The pipeline extracts data, transforms it into a clean star schema, loads it into a database, and visualizes key health metrics in interactive dashboards.

Example Dashboard Screenshot
<img width="702" height="287" alt="Screenshot 2025-08-13 at 7 26 18 PM" src="https://github.com/user-attachments/assets/e462a6d1-cbb0-4d2d-a06c-1032e47b0833" />

Tech Stack

Python — pandas, SQLAlchemy, requests

Databases — SQLite (local), PostgreSQL (via Docker Compose)

Data Visualization — Metabase

Containerization — Docker & Docker Compose

Public Data API — CDC Chronic Disease Indicators (Socrata API)

Synthetic Data — Synthea

Interactive dashboards in Metabase: http://probable-garbanzo-qgww4r9gj6534977-3000.app.github.dev/public/dashboard/d73da2ff-e478-4573-9b40-1043e6e2bc72


Data Extraction (E)

Synthetic patient, encounter, and condition data from Synthea.

CDC Chronic Disease Indicators (Maryland subset) via Socrata API.

Data Transformation (T)

Star schema with fact & dimension tables.

Precomputed SQL views for:

Monthly 30-day readmission rates

Top conditions by patient count

Chronic disease trends

Data Loading (L)

Loads into SQLite (local testing) or PostgreSQL (production-like via Docker Compose).

Visualization

Interactive Metabase dashboards for:

Monthly readmission rate trends

Top health conditions

Chronic disease prevalence


To run this code:

Start services:
docker compose up -d

Run ETL Scripts:
python etl/extract.py
python etl/load.py
python etl/transform.py

To View Dashboards
View dashboards

Open Metabase at http://localhost:3000

Connect the database 

Dashboards live in the Health Dashboard collection.

Skills used:
API data extraction (Socrata API)

Data wrangling with pandas

SQL schema design & indexing

Dockerized development

BI dashboard creation

Healthcare analytics domain knowledge




