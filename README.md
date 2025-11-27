# Clash royale ELT pipeline

End-to-end data pipeline designed to extract, process, and model data from the clash royale api. The system tracks player performance, card economy, and meta trends using a hybrid spark and dbt architecture.

## Overview

This project implements an elt pipeline that ingests semi-structured json data into a data lake, processes it using spark for schema enforcement and partitioning, and transforms it into a dimensional model (star schema) using dbt for analytics.

The entire stack is containerized with docker for consistent deployment.

## Architecture

Data flow follows a standard lakehouse pattern:

1. **ingestion**: python script fetches data using a "clan-first" discovery strategy to maximize player coverage.
2. **data lake**: raw json is stored in minio (s3 compatible storage).
3. **processing**: apache spark reads raw data, handles nested arrays, and loads cleaned data into postgres.
4. **transformation**: dbt builds a star schema (facts and dimensions) and enforces data quality tests.
5. **orchestration**: apache airflow manages the dependency graph and scheduling.

## Tech stack

* **language:** python
* **storage:** minio (s3), postgresql
* **processing:** apache spark (pyspark)
* **transformation:** dbt core
* **orchestration:** apache airflow
* **infrastructure:** docker compose

## Setup & usage

### Prerequisites

* docker desktop installed and running.
* clash royale developer account (to obtain an api key).

### Quick start

1.  **Clone the repository**
    ```bash
    git clone [https://github.com/your-username/clash-royale-elt.git](https://github.com/your-username/elt_cr.git)
    cd elt_cr
    ```

2.  **Configure environment**
    
    ```bash
    cp env.example.txt .env
    ```
    Open `.env` and paste your specific `CLASH_API_KEY`. Connections for the database and storage are pre-configured for the docker network.
    **Dynamic Tracking List (Optional)**
    To create a custom list of VIP players/clans, create a JSON variable in .env named `cr_tracking_config`, then put the tags you want to track.

    If this variable is not set, the pipeline will only process the top global clans and their members automatically discovered.

3.  **Start services**
    ```bash
    docker compose up -d
    ```

4.  **Run the pipeline**
    * access airflow at `http://localhost:8080` (credentials: `airflow` / `airflow`).
    * trigger the `ingest_script` dag.

    This will execute the following flow:
    * fetch top global clans and discover players.
    * dump raw json data to minio.
    * process data with spark and load to postgres.
    * run dbt models and tests.

## Data modeling

The warehouse is modeled using a kimball star schema approach.

### Core models
* **`fct_battles`**: transactional granularity per battle. contains metrics like crowns, elixir leaked, and tower damage.
* **`fct_cards_usage`**: union of player and opponent cards to analyze meta trends.
* **`fct_player_daily_stats`**: periodic snapshot calculating daily flow (delta of trophies, wins) using window functions.
* **`dim_cards`**, **`dim_players`**, **`dim_clans`**: standard dimensions for filtering and grouping.

### Data quality
configured in `schema.yml`. the pipeline enforces:
* **referential integrity:** validates relationships between battles and players/cards.
* **uniqueness:** uses surrogate keys to prevent duplicates.
* **validity:** checks for logical ranges