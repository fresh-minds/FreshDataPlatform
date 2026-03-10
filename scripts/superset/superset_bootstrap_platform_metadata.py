import json
import os
import time
from typing import Any, Dict, List, Optional

import psycopg2
import requests

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
ADMIN_USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")

WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "warehouse")
WAREHOUSE_PORT = os.getenv("WAREHOUSE_PORT", "5432")
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "odp_dw")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER", "admin")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "admin")

DATABASE_NAME = os.getenv("SUPERSET_WAREHOUSE_NAME", "Platform Warehouse")
SCHEMA_NAME = os.getenv("SUPERSET_PLATFORM_METADATA_SCHEMA", "platform_metadata")
DASHBOARD_TITLE = os.getenv("SUPERSET_PLATFORM_METADATA_DASHBOARD", "Platform Metadata Operations")

PLATFORM_TABLES = (
    "pipeline_runs",
    "pipeline_task_runs",
    "data_quality_results",
    "policy_evaluation_results",
)


def _log(message: str) -> None:
    print(f"[Platform Metadata Dashboard] {message}")


def _request_with_fallback(session: requests.Session, method: str, url: str, **kwargs):
    response = session.request(method, url, **kwargs)
    if response.status_code == 200:
        return response

    if method.lower() == "get" and "page_size" in (kwargs.get("params") or {}):
        params = kwargs.get("params", {})
        page_size = params.get("page_size", 1000)
        response = session.request(
            method,
            url,
            params={"q": f"(page_size:{page_size})"},
            **{k: v for k, v in kwargs.items() if k != "params"},
        )
    return response


def get_access_token(session: requests.Session) -> str:
    payload = {
        "username": ADMIN_USER,
        "password": ADMIN_PASSWORD,
        "provider": "db",
        "refresh": True,
    }
    resp = session.post(f"{SUPERSET_URL}/api/v1/security/login", json=payload)
    resp.raise_for_status()
    data = resp.json()
    return data.get("access_token")


def get_csrf_token(session: requests.Session, access_token: str) -> str:
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", headers=headers)
    resp.raise_for_status()
    return resp.json().get("result")


def list_databases(session: requests.Session, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    resp = _request_with_fallback(
        session,
        "GET",
        f"{SUPERSET_URL}/api/v1/database/",
        headers=headers,
        params={"page_size": 1000, "page": 0},
    )
    resp.raise_for_status()
    return resp.json().get("result", [])


def create_database(session: requests.Session, headers: Dict[str, str]) -> int:
    sqlalchemy_uri = (
        f"postgresql+psycopg2://{WAREHOUSE_USER}:{WAREHOUSE_PASSWORD}"
        f"@{WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB}"
    )
    payload = {
        "database_name": DATABASE_NAME,
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_ctas": True,
        "allow_cvas": True,
        "allow_dml": True,
        "allow_run_async": False,
        "extra": json.dumps({"engine_params": {}}),
    }
    resp = session.post(f"{SUPERSET_URL}/api/v1/database/", headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json().get("id")


def list_datasets(session: requests.Session, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    resp = _request_with_fallback(
        session,
        "GET",
        f"{SUPERSET_URL}/api/v1/dataset/",
        headers=headers,
        params={"page_size": 1000, "page": 0},
    )
    resp.raise_for_status()
    return resp.json().get("result", [])


def _dataset_database_id(dataset: Dict[str, Any]) -> Optional[int]:
    database = dataset.get("database")
    if isinstance(database, dict):
        return database.get("id")
    if isinstance(database, int):
        return database
    return None


def create_dataset(
    session: requests.Session,
    headers: Dict[str, str],
    database_id: int,
    table_name: str,
) -> int:
    payload = {
        "database": database_id,
        "schema": SCHEMA_NAME,
        "table_name": table_name,
        "is_managed_externally": True,
        "external_url": "",
        "owners": [],
    }
    resp = session.post(f"{SUPERSET_URL}/api/v1/dataset/", headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json().get("id")


def create_virtual_dataset(
    session: requests.Session,
    headers: Dict[str, str],
    database_id: int,
    name: str,
    sql: str,
) -> int:
    payload = {
        "database": database_id,
        "schema": SCHEMA_NAME,
        "table_name": name,
        "sql": sql,
        "is_managed_externally": True,
        "external_url": "",
        "owners": [],
    }
    resp = session.post(f"{SUPERSET_URL}/api/v1/dataset/", headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json().get("id")


def list_dashboards(session: requests.Session, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    resp = _request_with_fallback(
        session,
        "GET",
        f"{SUPERSET_URL}/api/v1/dashboard/",
        headers=headers,
        params={"page_size": 1000, "page": 0},
    )
    resp.raise_for_status()
    return resp.json().get("result", [])


def create_dashboard(session: requests.Session, headers: Dict[str, str]) -> int:
    payload = {
        "dashboard_title": DASHBOARD_TITLE,
        "published": True,
        "owners": [],
    }
    resp = session.post(f"{SUPERSET_URL}/api/v1/dashboard/", headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json().get("id")


def list_charts(session: requests.Session, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    resp = _request_with_fallback(
        session,
        "GET",
        f"{SUPERSET_URL}/api/v1/chart/",
        headers=headers,
        params={"page_size": 1000, "page": 0},
    )
    resp.raise_for_status()
    return resp.json().get("result", [])


def create_chart(
    session: requests.Session,
    headers: Dict[str, str],
    dataset_id: int,
    dashboard_id: int,
    spec: Dict[str, Any],
) -> int:
    form_data = spec["form_data"].copy()
    form_data["datasource"] = f"{dataset_id}__table"
    payload = {
        "slice_name": spec["name"],
        "viz_type": spec["viz_type"],
        "datasource_id": dataset_id,
        "datasource_type": "table",
        "dashboards": [dashboard_id],
        "params": json.dumps(form_data),
    }
    resp = session.post(f"{SUPERSET_URL}/api/v1/chart/", headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json().get("id")


def _query_columns_from_form_data(form_data: Dict[str, Any]) -> List[Any]:
    groupby = form_data.get("groupby")
    if isinstance(groupby, list) and groupby:
        return groupby

    columns = form_data.get("columns")
    if isinstance(columns, list) and columns:
        return columns

    all_columns = form_data.get("all_columns")
    if isinstance(all_columns, list) and all_columns:
        return all_columns

    return []


def _query_metrics_from_form_data(form_data: Dict[str, Any]) -> List[Any]:
    metric = form_data.get("metric")
    if metric:
        return [metric]

    metrics = form_data.get("metrics")
    if isinstance(metrics, list):
        return metrics

    return []


def _extract_where_from_adhoc(form_data: Dict[str, Any]) -> str:
    adhoc = form_data.get("adhoc_filters", [])
    clauses = []
    for item in adhoc:
        if item.get("expressionType") == "SQL" and item.get("clause") == "WHERE":
            sql_expression = item.get("sqlExpression", "").strip()
            if sql_expression:
                clauses.append(sql_expression)

    existing_where = form_data.get("where", "")
    if existing_where:
        clauses.insert(0, existing_where)

    return " AND ".join(clauses)


def build_query_context(form_data: Dict[str, Any], dataset_id: int, chart_id: int) -> Dict[str, Any]:
    query_columns = _query_columns_from_form_data(form_data)
    query_metrics = _query_metrics_from_form_data(form_data)
    row_limit = int(form_data.get("row_limit") or 1000)
    order_desc = bool(form_data.get("sort_desc", form_data.get("order_desc", True)))
    where_clause = _extract_where_from_adhoc(form_data)

    context_form_data = form_data.copy()
    context_form_data.update(
        {
            "datasource": f"{dataset_id}__table",
            "slice_id": chart_id,
            "force": False,
            "result_format": "json",
            "result_type": "full",
        }
    )

    return {
        "datasource": {"id": dataset_id, "type": "table"},
        "force": False,
        "queries": [
            {
                "filters": [],
                "extras": {
                    "having": form_data.get("having", ""),
                    "where": where_clause,
                },
                "applied_time_extras": {},
                "columns": query_columns,
                "metrics": query_metrics,
                "annotation_layers": [],
                "row_limit": row_limit,
                "series_limit": int(form_data.get("timeseries_limit") or 0),
                "order_desc": order_desc,
                "url_params": {},
                "custom_params": {},
                "custom_form_data": {},
            }
        ],
        "form_data": context_form_data,
        "result_format": "json",
        "result_type": "full",
    }


def update_chart_query_context(
    session: requests.Session,
    headers: Dict[str, str],
    chart_id: int,
    dataset_id: int,
    form_data: Dict[str, Any],
) -> None:
    query_context = build_query_context(form_data, dataset_id, chart_id)
    payload = {
        "query_context": json.dumps(query_context),
        "query_context_generation": True,
    }
    resp = session.put(f"{SUPERSET_URL}/api/v1/chart/{chart_id}", headers=headers, json=payload)
    resp.raise_for_status()


def validate_chart_data(
    session: requests.Session,
    headers: Dict[str, str],
    chart_id: int,
    chart_name: str,
) -> None:
    resp = session.get(f"{SUPERSET_URL}/api/v1/chart/{chart_id}/data/", headers=headers)
    if resp.status_code == 200:
        _log(f"  ✓ Chart '{chart_name}' validated OK.")
        return
    snippet = (resp.text or "")[:500]
    _log(
        f"  ✗ Chart validation failed for '{chart_name}' "
        f"(id={chart_id}, status={resp.status_code}): {snippet}"
    )


def update_dashboard_layout(
    session: requests.Session,
    headers: Dict[str, str],
    dashboard_id: int,
    charts: List[Dict[str, Any]],
) -> None:
    layout_rows = [
        {"row_id": "ROW-1", "height": 16, "charts_width": [4, 4, 4]},
        {"row_id": "ROW-2", "height": 26, "charts_width": [6, 6]},
        {"row_id": "ROW-3", "height": 26, "charts_width": [6, 6]},
    ]

    row_ids = [row["row_id"] for row in layout_rows]
    position: Dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "parents": ["ROOT_ID"], "children": row_ids},
    }

    for row in layout_rows:
        position[row["row_id"]] = {
            "id": row["row_id"],
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": [],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }

    chart_idx = 0
    for row in layout_rows:
        for col_idx, width in enumerate(row["charts_width"]):
            if chart_idx >= len(charts):
                break
            chart = charts[chart_idx]
            row_label = row["row_id"].replace("-", "")
            node_id = f"CHART-{row_label}-{col_idx + 1}"
            position[row["row_id"]]["children"].append(node_id)
            position[node_id] = {
                "id": node_id,
                "type": "CHART",
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row["row_id"]],
                "meta": {
                    "chartId": chart["id"],
                    "height": row["height"],
                    "width": width,
                },
            }
            chart_idx += 1

    payload = {
        "position_json": json.dumps(position),
        "published": True,
    }
    resp = session.put(f"{SUPERSET_URL}/api/v1/dashboard/{dashboard_id}", headers=headers, json=payload)
    if resp.status_code in (200, 201):
        _log("Dashboard layout updated successfully.")
    else:
        _log(f"Dashboard layout update returned status {resp.status_code}: {(resp.text or '')[:500]}")


def discover_platform_metadata_tables() -> set[str]:
    dsn = (
        f"host={WAREHOUSE_HOST} port={WAREHOUSE_PORT} dbname={WAREHOUSE_DB} "
        f"user={WAREHOUSE_USER} password={WAREHOUSE_PASSWORD}"
    )
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                """,
                (SCHEMA_NAME,),
            )
            return {row[0] for row in cur.fetchall()}


def build_virtual_datasets(available_tables: set[str]) -> List[Dict[str, str]]:
    virtual_datasets: List[Dict[str, str]] = []

    if "pipeline_runs" in available_tables:
        virtual_datasets.extend(
            [
                {
                    "name": "vw_pm_pipeline_run_kpis",
                    "sql": (
                        "SELECT\n"
                        "    COUNT(*)::bigint AS total_runs,\n"
                        "    SUM(CASE WHEN COALESCE(status, 'UNKNOWN') <> 'SUCCESS' THEN 1 ELSE 0 END)::bigint AS failed_runs,\n"
                        "    COALESCE(AVG(duration_ms), 0)::numeric(18,2) AS avg_duration_ms\n"
                        f"FROM {SCHEMA_NAME}.pipeline_runs\n"
                        "WHERE COALESCE(started_at_utc, finished_at_utc) >= now() - interval '30 days'"
                    ),
                },
                {
                    "name": "vw_pm_pipeline_runs_by_day",
                    "sql": (
                        "SELECT\n"
                        "    TO_CHAR(DATE_TRUNC('day', COALESCE(finished_at_utc, started_at_utc)), 'YYYY-MM-DD') AS run_day,\n"
                        "    COUNT(*)::bigint AS total_runs,\n"
                        "    SUM(CASE WHEN COALESCE(status, 'UNKNOWN') = 'SUCCESS' THEN 1 ELSE 0 END)::bigint AS success_runs,\n"
                        "    SUM(CASE WHEN COALESCE(status, 'UNKNOWN') <> 'SUCCESS' THEN 1 ELSE 0 END)::bigint AS failed_runs\n"
                        f"FROM {SCHEMA_NAME}.pipeline_runs\n"
                        "WHERE COALESCE(started_at_utc, finished_at_utc) >= now() - interval '30 days'\n"
                        "GROUP BY 1\n"
                        "ORDER BY 1"
                    ),
                },
            ]
        )

    if "pipeline_task_runs" in available_tables:
        virtual_datasets.append(
            {
                "name": "vw_pm_task_status_7d",
                "sql": (
                    "SELECT\n"
                    "    COALESCE(status, 'UNKNOWN') AS status,\n"
                    "    COUNT(*)::bigint AS task_count\n"
                    f"FROM {SCHEMA_NAME}.pipeline_task_runs\n"
                    "WHERE COALESCE(started_at_utc, finished_at_utc, logged_at_utc) >= now() - interval '7 days'\n"
                    "GROUP BY 1\n"
                    "ORDER BY 2 DESC"
                ),
            }
        )

    if "data_quality_results" in available_tables:
        virtual_datasets.append(
            {
                "name": "vw_pm_quality_status_7d",
                "sql": (
                    "SELECT\n"
                    "    COALESCE(status, 'UNKNOWN') AS status,\n"
                    "    COUNT(*)::bigint AS result_count\n"
                    f"FROM {SCHEMA_NAME}.data_quality_results\n"
                    "WHERE evaluated_at_utc >= now() - interval '7 days'\n"
                    "GROUP BY 1\n"
                    "ORDER BY 2 DESC"
                ),
            }
        )

    if "policy_evaluation_results" in available_tables:
        virtual_datasets.append(
            {
                "name": "vw_pm_policy_status_7d",
                "sql": (
                    "SELECT\n"
                    "    COALESCE(status, 'UNKNOWN') AS status,\n"
                    "    COUNT(*)::bigint AS result_count\n"
                    f"FROM {SCHEMA_NAME}.policy_evaluation_results\n"
                    "WHERE evaluated_at_utc >= now() - interval '7 days'\n"
                    "GROUP BY 1\n"
                    "ORDER BY 2 DESC"
                ),
            }
        )

    return virtual_datasets


def build_chart_specs(dataset_ids: Dict[str, int]) -> List[Dict[str, Any]]:
    chart_specs: List[Dict[str, Any]] = []

    if "vw_pm_pipeline_run_kpis" in dataset_ids:
        chart_specs.extend(
            [
                {
                    "name": "Pipeline Runs (30d)",
                    "dataset": "vw_pm_pipeline_run_kpis",
                    "viz_type": "big_number_total",
                    "form_data": {
                        "viz_type": "big_number_total",
                        "metric": {
                            "expressionType": "SIMPLE",
                            "column": {"column_name": "total_runs"},
                            "aggregate": "SUM",
                            "label": "SUM(total_runs)",
                            "hasCustomLabel": False,
                            "optionName": "metric_total_runs",
                        },
                        "adhoc_filters": [],
                        "row_limit": 1000,
                    },
                },
                {
                    "name": "Pipeline Failures (30d)",
                    "dataset": "vw_pm_pipeline_run_kpis",
                    "viz_type": "big_number_total",
                    "form_data": {
                        "viz_type": "big_number_total",
                        "metric": {
                            "expressionType": "SIMPLE",
                            "column": {"column_name": "failed_runs"},
                            "aggregate": "SUM",
                            "label": "SUM(failed_runs)",
                            "hasCustomLabel": False,
                            "optionName": "metric_failed_runs",
                        },
                        "adhoc_filters": [],
                        "row_limit": 1000,
                    },
                },
                {
                    "name": "Avg Pipeline Duration ms (30d)",
                    "dataset": "vw_pm_pipeline_run_kpis",
                    "viz_type": "big_number_total",
                    "form_data": {
                        "viz_type": "big_number_total",
                        "metric": {
                            "expressionType": "SIMPLE",
                            "column": {"column_name": "avg_duration_ms"},
                            "aggregate": "AVG",
                            "label": "AVG(avg_duration_ms)",
                            "hasCustomLabel": False,
                            "optionName": "metric_avg_duration",
                        },
                        "adhoc_filters": [],
                        "row_limit": 1000,
                    },
                },
            ]
        )

    if "vw_pm_pipeline_runs_by_day" in dataset_ids:
        chart_specs.extend(
            [
                {
                    "name": "Pipeline Runs by Day (30d)",
                    "dataset": "vw_pm_pipeline_runs_by_day",
                    "viz_type": "dist_bar",
                    "form_data": {
                        "viz_type": "dist_bar",
                        "metrics": [
                            {
                                "expressionType": "SIMPLE",
                                "column": {"column_name": "total_runs"},
                                "aggregate": "SUM",
                                "label": "Runs",
                                "hasCustomLabel": True,
                                "optionName": "metric_runs_by_day",
                            }
                        ],
                        "groupby": ["run_day"],
                        "adhoc_filters": [],
                        "row_limit": 60,
                        "sort_desc": False,
                        "order_desc": False,
                        "show_legend": False,
                    },
                },
                {
                    "name": "Pipeline Failures by Day (30d)",
                    "dataset": "vw_pm_pipeline_runs_by_day",
                    "viz_type": "dist_bar",
                    "form_data": {
                        "viz_type": "dist_bar",
                        "metrics": [
                            {
                                "expressionType": "SIMPLE",
                                "column": {"column_name": "failed_runs"},
                                "aggregate": "SUM",
                                "label": "Failures",
                                "hasCustomLabel": True,
                                "optionName": "metric_failures_by_day",
                            }
                        ],
                        "groupby": ["run_day"],
                        "adhoc_filters": [],
                        "row_limit": 60,
                        "sort_desc": False,
                        "order_desc": False,
                        "show_legend": False,
                    },
                },
            ]
        )

    if "vw_pm_quality_status_7d" in dataset_ids:
        chart_specs.append(
            {
                "name": "Data Quality Status (7d)",
                "dataset": "vw_pm_quality_status_7d",
                "viz_type": "pie",
                "form_data": {
                    "viz_type": "pie",
                    "metrics": [
                        {
                            "expressionType": "SIMPLE",
                            "column": {"column_name": "result_count"},
                            "aggregate": "SUM",
                            "label": "SUM(result_count)",
                            "hasCustomLabel": False,
                            "optionName": "metric_quality_results",
                        }
                    ],
                    "groupby": ["status"],
                    "adhoc_filters": [],
                    "row_limit": 20,
                    "donut": True,
                    "show_labels": True,
                    "show_legend": True,
                },
            }
        )

    if "vw_pm_policy_status_7d" in dataset_ids:
        chart_specs.append(
            {
                "name": "Policy Evaluation Status (7d)",
                "dataset": "vw_pm_policy_status_7d",
                "viz_type": "pie",
                "form_data": {
                    "viz_type": "pie",
                    "metrics": [
                        {
                            "expressionType": "SIMPLE",
                            "column": {"column_name": "result_count"},
                            "aggregate": "SUM",
                            "label": "SUM(result_count)",
                            "hasCustomLabel": False,
                            "optionName": "metric_policy_results",
                        }
                    ],
                    "groupby": ["status"],
                    "adhoc_filters": [],
                    "row_limit": 20,
                    "donut": True,
                    "show_labels": True,
                    "show_legend": True,
                },
            }
        )

    if "vw_pm_task_status_7d" in dataset_ids:
        chart_specs.append(
            {
                "name": "Pipeline Task Status (7d)",
                "dataset": "vw_pm_task_status_7d",
                "viz_type": "dist_bar",
                "form_data": {
                    "viz_type": "dist_bar",
                    "metrics": [
                        {
                            "expressionType": "SIMPLE",
                            "column": {"column_name": "task_count"},
                            "aggregate": "SUM",
                            "label": "Tasks",
                            "hasCustomLabel": True,
                            "optionName": "metric_task_status",
                        }
                    ],
                    "groupby": ["status"],
                    "adhoc_filters": [],
                    "row_limit": 20,
                    "sort_desc": True,
                    "order_desc": True,
                    "show_legend": False,
                },
            }
        )

    return chart_specs


def main() -> None:
    _log("Starting platform metadata dashboard bootstrap...")

    available_tables = discover_platform_metadata_tables()
    existing_tables = sorted([table for table in PLATFORM_TABLES if table in available_tables])
    if not existing_tables:
        _log(
            f"No required tables found in schema {SCHEMA_NAME}; "
            "skipping dashboard bootstrap."
        )
        return

    _log(f"Discovered metadata tables: {', '.join(existing_tables)}")

    virtual_datasets = build_virtual_datasets(available_tables)

    session = requests.Session()

    access_token = None
    for attempt in range(1, 11):
        try:
            access_token = get_access_token(session)
            break
        except Exception as exc:
            if attempt == 10:
                raise
            _log(f"Superset not ready yet (attempt {attempt}/10): {exc}")
            time.sleep(5)
    if not access_token:
        return

    csrf_token = get_csrf_token(session, access_token)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-CSRFToken": csrf_token,
        "Content-Type": "application/json",
    }

    dbs = list_databases(session, headers)
    db_match = next((db for db in dbs if db.get("database_name") == DATABASE_NAME), None)
    if db_match:
        database_id = db_match.get("id")
        _log(f"Reusing existing database '{DATABASE_NAME}' (id={database_id}).")
    else:
        database_id = create_database(session, headers)
        _log(f"Created database '{DATABASE_NAME}' (id={database_id}).")

    existing_datasets = list_datasets(session, headers)
    dataset_ids: Dict[str, int] = {}

    for table_name in existing_tables:
        match = next(
            (
                ds
                for ds in existing_datasets
                if ds.get("table_name") == table_name
                and ds.get("schema") == SCHEMA_NAME
                and _dataset_database_id(ds) == database_id
            ),
            None,
        )
        if match:
            dataset_ids[table_name] = match.get("id")
            _log(f"Dataset already exists: {SCHEMA_NAME}.{table_name}")
        else:
            ds_id = create_dataset(session, headers, database_id, table_name)
            dataset_ids[table_name] = ds_id
            _log(f"Created dataset: {SCHEMA_NAME}.{table_name}")

    for dataset in virtual_datasets:
        name = dataset["name"]
        match = next(
            (
                ds
                for ds in existing_datasets
                if ds.get("table_name") == name and _dataset_database_id(ds) == database_id
            ),
            None,
        )
        if match:
            dataset_ids[name] = match.get("id")
            _log(f"Virtual dataset already exists: {name}")
        else:
            ds_id = create_virtual_dataset(session, headers, database_id, name, dataset["sql"])
            dataset_ids[name] = ds_id
            _log(f"Created virtual dataset: {name}")

    chart_specs = build_chart_specs(dataset_ids)
    if not chart_specs:
        _log("No chart specs generated from available metadata tables; skipping dashboard creation.")
        return

    dashboards = list_dashboards(session, headers)
    dashboard_match = next((dash for dash in dashboards if dash.get("dashboard_title") == DASHBOARD_TITLE), None)
    if dashboard_match:
        dashboard_id = dashboard_match.get("id")
        _log(f"Dashboard already exists (id={dashboard_id}).")
    else:
        dashboard_id = create_dashboard(session, headers)
        _log(f"Created dashboard '{DASHBOARD_TITLE}' (id={dashboard_id}).")

    existing_charts = list_charts(session, headers)
    configured_charts: List[Dict[str, Any]] = []

    for spec in chart_specs:
        dataset_id = dataset_ids.get(spec["dataset"])
        if not dataset_id:
            _log(f"Dataset missing for chart '{spec['name']}', skipping.")
            continue

        chart_match = next((chart for chart in existing_charts if chart.get("slice_name") == spec["name"]), None)
        if chart_match:
            chart_id = chart_match.get("id")
            _log(f"Chart already exists: '{spec['name']}' (id={chart_id})")
        else:
            chart_id = create_chart(session, headers, dataset_id, dashboard_id, spec)
            _log(f"Created chart: '{spec['name']}' (id={chart_id})")

        configured_charts.append(
            {
                "id": chart_id,
                "name": spec["name"],
                "dataset_id": dataset_id,
                "form_data": spec["form_data"],
            }
        )

    _log("Setting query context for each chart...")
    for chart in configured_charts:
        chart_id = chart["id"]
        if not chart_id:
            continue
        update_chart_query_context(
            session,
            headers,
            chart_id=chart_id,
            dataset_id=chart["dataset_id"],
            form_data=chart["form_data"],
        )
        validate_chart_data(session, headers, chart_id=chart_id, chart_name=chart["name"])

    if configured_charts:
        update_dashboard_layout(session, headers, dashboard_id, configured_charts)

    _log(
        f"Platform metadata dashboard bootstrap completed. "
        f"Configured {len(configured_charts)} charts."
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        _log(f"Bootstrap failed: {exc}")
        raise