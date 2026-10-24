# Fleet Telematics Lakehouse

SIC 8 Big Data graduation project, domain 3: an end-to-end pipeline on **Microsoft Fabric** for a
simulated fleet of 5,000 delivery vehicles - generator, streaming ingestion, Spark processing,
Delta lakehouse with a star schema, Airflow orchestration and a Power BI dashboard.

```
generator -> Eventstream -> bronze -> (Spark) silver -> (warehouse) gold -> Power BI
```

## Repository layout

| Folder | Owner | Contents |
|---|---|---|
| `generator/` | Person 1 | Python generator (config + seed), reference data, sample, stream sender |
| `ingestion/` | Person 1 | Fabric notebooks for loading reference data and checking bronze |
| `spark/` | Person 2 | Bronze -> silver jobs, anomaly detection, evaluation |
| `lakehouse/` | Person 3 | Silver -> gold star schema, SCD2, quality checks, KPIs |
| `orchestration/` | Person 4 | Airflow DAG |
| `dashboard/` | Person 4 | Power BI |
| `docs/` | All | Per-stage documentation, architecture, decisions |
| `tests/` | All | Tests |
| `evidence/` | All | Screenshots, logs and outputs for grading |

## Quick start (generator)

```bash
cd generator
python3 -m venv .venv && .venv/bin/pip install pyyaml azure-eventhub
.venv/bin/python generate.py all
```

Secrets (the Eventstream connection string) go in `generator/.env`, which is git-ignored. Never commit them.
