# Notebook Workspace

This folder is mounted into the `jupyter` service from `docker-compose.yml`.

## Start

```bash
docker compose up -d jupyter
```

Open [http://localhost:8888](http://localhost:8888).

If you set `JUPYTER_TOKEN` in `.env`, use:

```text
http://localhost:8888/lab?token=<your-token>
```

## Data Access

The container gets these credentials automatically from compose:

- MinIO: `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`
- Warehouse Postgres: `WAREHOUSE_HOST`, `WAREHOUSE_PORT`, `WAREHOUSE_DB`, `WAREHOUSE_USER`, `WAREHOUSE_PASSWORD`

Helper utilities live in `notebooks/helpers/platform_io.py`.

## Quick Example

```python
from helpers.platform_io import list_minio_objects, query_postgres

list_minio_objects("gold", prefix="job_market_nl/")
query_postgres("select table_schema, table_name from information_schema.tables limit 5")
```
