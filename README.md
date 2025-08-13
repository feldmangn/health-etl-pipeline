# Health ETL Pipeline

This project extracts synthetic healthcare data (Synthea), loads it into a Postgres database, transforms it into a star schema, and visualizes key metrics.

## Tech Stack
- Python (pandas, SQLAlchemy)
- Postgres
- Docker Compose
- Metabase

## Steps
1. Run `docker compose up -d` to start Postgres & Metabase.
2. Run ETL scripts in `etl/` to load data.
3. Explore dashboards in `viz/`.

## Data
Uses synthetic data generated with [Synthea](https://synthetichealth.github.io/synthea/).
