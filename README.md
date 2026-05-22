# Clash Royale Analytics Platform

> End-to-end ELT pipeline que extrae datos de la Clash Royale API, los procesa con Apache Spark y construye un data warehouse analítico con dbt.

![CI](https://github.com/GasparSaenzValiente/elt_cr/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Airflow-3.0-red)
![dbt](https://img.shields.io/badge/dbt-core-orange)

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Apache Airflow 3                             │
│  ┌─────────────┐   ┌──────────────┐   ┌────────────┐   ┌────────┐  │
│  │  Ingestion  │──▶│  Transform   │──▶│  dbt run   │──▶│dbt test│  │
│  │  (Python)   │   │  (PySpark)   │   │            │   │        │  │
│  └──────┬──────┘   └──────┬───────┘   └────────────┘   └────────┘  │
└─────────│────────────────│────────────────────────────────────────-─┘
          │                │
          ▼                ▼
  ┌───────────────┐  ┌─────────────────┐
  │  MinIO (S3)   │  │   PostgreSQL    │
  │  Data Lake    │  │   (landing →    │
  │  raw/JSON     │  │   star schema)  │
  └───────────────┘  └─────────────────┘
```

**Flujo de datos (patrón Lakehouse):**

1. **Ingesta** — script Python con estrategia "clan-first": descubre jugadores a partir de los top clanes globales. Los datos raw (JSON) se guardan particionados por fecha en MinIO.
2. **Data Lake** — MinIO como almacenamiento S3-compatible. Particionado por `year/month/day` para optimizar las lecturas de Spark.
3. **Procesamiento** — Apache Spark lee el JSON raw, resuelve arrays anidados (cartas, miembros) y carga las tablas `landing_*` en PostgreSQL.
4. **Transformación** — dbt construye un star schema Kimball con tests de calidad de datos.
5. **Orquestación** — Airflow gestiona el grafo de dependencias y el scheduling diario.

## Stack tecnológico

| Capa | Tecnología |
|------|-----------|
| Lenguaje | Python 3.12 |
| Orquestación | Apache Airflow 3.0 (CeleryExecutor) |
| Procesamiento | Apache Spark 3.5 (PySpark) |
| Data Lake | MinIO (S3-compatible) |
| Data Warehouse | PostgreSQL 16 |
| Transformación | dbt Core |
| Infraestructura | Docker Compose |
| CI | GitHub Actions |

## Modelo de datos

Star schema siguiendo la metodología Kimball:

```
                    ┌─────────────┐
                    │  dim_date   │
                    └──────┬──────┘
                           │
 ┌─────────────┐    ┌──────┴──────┐    ┌──────────────────┐
 │ dim_players │───▶│ fct_battles │◀───│  dim_game_modes  │
 └─────────────┘    └──────┬──────┘    └──────────────────┘
                           │
 ┌─────────────┐    ┌──────┴──────────┐
 │  dim_cards  │───▶│ fct_cards_usage │
 └──────┬──────┘    └─────────────────┘
        │
        │           ┌──────────────────────┐
        └──────────▶│ fct_player_card_     │
                    │ holdings             │
                    └──────────────────────┘
 ┌─────────────┐    ┌──────────────────────┐
 │  dim_clans  │───▶│ fct_player_daily_    │
 └─────────────┘    │ stats                │
                    └──────────────────────┘
```

### Tablas de hechos

| Tabla | Granularidad | Descripción |
|-------|-------------|-------------|
| `fct_battles` | 1 fila por (battle_id, player_tag) | Métricas por batalla: coronas, elixir, torres, resultado |
| `fct_cards_usage` | 1 fila por (battle_id, player_tag, card_id) | Cartas usadas por batalla para análisis de meta |
| `fct_player_daily_stats` | 1 fila por (player_tag, snapshot_date) | Snapshot diario con deltas de trofeos, victorias y donaciones |
| `fct_player_card_holdings` | 1 fila por (player_tag, card_id, snapshot_date) | Colección diaria de cartas por jugador |

## Setup

### Prerequisitos

- Docker Desktop instalado y corriendo
- Cuenta de desarrollador de Clash Royale (para obtener API key)

### Inicio rápido

**1. Clonar el repositorio**
```bash
git clone https://github.com/GasparSaenzValiente/elt_cr.git
cd elt_cr
```

**2. Configurar el entorno**
```bash
cp .env.example .env
```

Editar `.env` y completar los valores marcados con `<...>`:
- `CLASH_API_KEY` — obtener en https://developer.clashroyale.com/
- `AIRFLOW_FERNET_KEY` — generar con:
  ```bash
  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  ```
- Contraseñas para PostgreSQL, MinIO y la UI de Airflow

**3. Levantar los servicios**
```bash
docker compose up -d
```

**4. Ejecutar el pipeline**
- Acceder a Airflow en `http://localhost:8080`
- Activar el DAG `clash_royale_pipeline`
- El pipeline ejecuta automáticamente a medianoche o puede dispararse manualmente

**Servicios disponibles:**

| Servicio | URL | Credenciales |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | Ver `.env` → `AIRFLOW_WWW_USER_*` |
| MinIO Console | http://localhost:9001 | Ver `.env` → `MINIO_ROOT_*` |
| PostgreSQL | localhost:5432 | Ver `.env` → `CR_POSTGRES_*` |

## Calidad de datos

Los tests de dbt en `models/schema.yml` verifican:
- **Unicidad**: claves surrogate únicas en todos los modelos de hechos
- **Integridad referencial**: FK válidas entre hechos y dimensiones
- **Rangos válidos**: coronas entre 0-3, nivel de exp >= 1, nivel de carta >= 1
- **Valores aceptados**: rareza, tipo de carta, resultado de batalla

## Decisiones de ingeniería

**Spark para extracción + dbt para transformación**
Spark maneja la complejidad del JSON anidado (arrays de cartas, miembros de clan) de forma distribuida. dbt se ocupa de la lógica de negocio, los tests y el linaje, donde brilla más que Spark.

**Identificador de batalla generado localmente**
La API de Clash Royale no expone un ID de batalla. Se genera un `battle_id` determinístico como SHA-256 de `(battleTime, min(player_tag, opp_tag), max(player_tag, opp_tag))`, lo que garantiza idempotencia en re-ejecuciones.

**Snapshots vs. slowly changing dimensions**
Los datos de jugadores y clanes se modelan como snapshots diarios en staging, y las dimensiones exponen solo el último estado conocido. Esto permite análisis histórico sin la complejidad de SCD Tipo 2.

**Estrategia de deduplicación**
`DISTINCT ON (key_columns) ... ORDER BY snapshot_date DESC` en los modelos staging garantiza una sola fila por entidad sin depender de `ROW_NUMBER()`, que es más costoso en PostgreSQL.

## Estructura del repositorio

```
elt_cr/
├── dags/
│   └── clash_royale_pipeline_dag.py   # DAG principal de Airflow
├── scripts/
│   ├── api_wrapper.py                 # Cliente tipado para la API de Clash Royale
│   ├── extract_data.py                # Extracción y carga en MinIO
│   └── transform_data.py             # Transformación Spark → PostgreSQL
├── dbt/clash_royale_analytics/
│   └── models/
│       ├── stg/                       # Staging: limpieza y tipado
│       ├── dim/                       # Dimensiones del star schema
│       ├── fct/                       # Tablas de hechos
│       ├── schema.yml                 # Tests y documentación dbt
│       └── sources.yml                # Definición de fuentes (landing_*)
├── analysis/
│   └── sample_queries.sql            # Queries de ejemplo sobre el warehouse
├── config/
│   └── airflow.cfg                   # Solo los overrides de configuración
├── .github/workflows/
│   └── ci.yml                        # Pipeline de CI (lint + dbt compile + tests)
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── .env.example                      # Plantilla de configuración
```
