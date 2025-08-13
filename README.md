# Health ETL Pipeline
Data Extraction (E)

This project extracts synthetic healthcare data from Synthea and the CDC Chronic Disease Indicators (CDI) API, loads it into a database, transforms it into a star schema, and visualizes key metrics with Metabase.


Data Extraction (E)

Synthetic patient, encounter, and condition data from Synthea.

Maryland slice of CDC Chronic Disease Indicators via Socrata API.

Data Transformation (T)

Star schema with dimension & fact tables.

Precomputed SQL views for analytics.

Data Loading (L)

SQLite or Postgres (via Docker Compose).

Visualization

Interactive dashboards in Metabase: http://probable-garbanzo-qgww4r9gj6534977-3000.app.github.dev/public/dashboard/d73da2ff-e478-4573-9b40-1043e6e2bc72


Monthly 30-day readmission rates

Top conditions by unique patient count

Chronic disease trends



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
