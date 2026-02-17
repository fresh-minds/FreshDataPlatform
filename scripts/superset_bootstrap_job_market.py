import json
import os
import time
from typing import Any, Dict, List, Optional

import requests

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
ADMIN_USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")

WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "warehouse")
WAREHOUSE_PORT = os.getenv("WAREHOUSE_PORT", "5432")
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "freshminds_dw")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER", "admin")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "admin")

DATABASE_NAME = os.getenv("SUPERSET_WAREHOUSE_NAME", "Platform Warehouse")
DASHBOARD_TITLE = os.getenv("SUPERSET_JOB_MARKET_DASHBOARD", "NL IT Job Market")
SCHEMA_NAME = os.getenv("JOB_MARKET_SCHEMA", "job_market_nl")

DATASETS = [
    "it_market_snapshot",
    "it_market_top_skills",
    "it_market_region_distribution",
    "it_market_job_ads_geo",
]

CHART_SPECS = [
    {
        "name": "NL IT Vacancies (CBS)",
        "dataset": "it_market_snapshot",
        "viz_type": "big_number_total",
        "form_data": {
            "viz_type": "big_number_total",
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "vacancies"},
                "aggregate": "SUM",
                "label": "SUM(vacancies)",
                "hasCustomLabel": False,
                "optionName": "metric_vacancies_sum",
            },
            "adhoc_filters": [],
            "row_limit": 1000,
        },
    },
    {
        "name": "NL IT Vacancy Rate (CBS)",
        "dataset": "it_market_snapshot",
        "viz_type": "big_number_total",
        "form_data": {
            "viz_type": "big_number_total",
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "vacancy_rate"},
                "aggregate": "AVG",
                "label": "AVG(vacancy_rate)",
                "hasCustomLabel": False,
                "optionName": "metric_vacancy_rate_avg",
            },
            "adhoc_filters": [],
            "row_limit": 1000,
        },
    },
    {
        "name": "Top IT Skills (Adzuna)",
        "dataset": "it_market_top_skills",
        "viz_type": "bar",
        "form_data": {
            "viz_type": "bar",
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "count"},
                    "aggregate": "SUM",
                    "label": "SUM(count)",
                    "hasCustomLabel": False,
                    "optionName": "metric_count_sum",
                }
            ],
            "groupby": ["skill"],
            "adhoc_filters": [],
            "row_limit": 50,
            "sort_desc": True,
        },
    },
    {
        "name": "NL Job Market Heat Map (Adzuna)",
        "dataset": "it_market_job_ads_geo",
        "viz_type": "deck_gl_heatmap",
        "form_data": {
            "viz_type": "deck_gl_heatmap",
            "spatial": {
                "type": "latlong",
                "latCol": "latitude",
                "lonCol": "longitude",
            },
            "adhoc_filters": [],
            "row_limit": 5000,
            "mapbox_style": "mapbox://styles/mapbox/light-v10",
            "linear_color_scheme": "deck_gl_heatmap_gradient",
            "viewport": {
                "latitude": 52.1326,
                "longitude": 5.2913,
                "zoom": 6,
                "bearing": 0,
                "pitch": 0,
            },
        },
    },
]


def _log(message: str) -> None:
    print(f"[Superset Bootstrap] {message}")


def _request_with_fallback(session: requests.Session, method: str, url: str, **kwargs):
    response = session.request(method, url, **kwargs)
    if response.status_code == 200:
        return response

    # Try alternative pagination method if list calls fail
    if method.lower() == "get" and "page_size" in (kwargs.get("params") or {}):
        params = kwargs.get("params", {})
        page_size = params.get("page_size", 1000)
        response = session.request(method, url, params={"q": f"(page_size:{page_size})"}, **{k: v for k, v in kwargs.items() if k != "params"})
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
    resp = _request_with_fallback(session, "GET", f"{SUPERSET_URL}/api/v1/database/", headers=headers, params={"page_size": 1000, "page": 0})
    resp.raise_for_status()
    return resp.json().get("result", [])


def create_database(session: requests.Session, headers: Dict[str, str]) -> int:
    sqlalchemy_uri = f"postgresql+psycopg2://{WAREHOUSE_USER}:{WAREHOUSE_PASSWORD}@{WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB}"

    payload = {
        "database_name": DATABASE_NAME,
        "sqlalchemy_uri": sqlalchemy_uri,
        "expose_in_sqllab": True,
        "allow_ctas": True,
        "allow_cvas": True,
        "allow_dml": True,
        "allow_run_async": True,
        "extra": json.dumps({"engine_params": {}}),
    }
    resp = session.post(f"{SUPERSET_URL}/api/v1/database/", headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json().get("id")


def sync_database(session: requests.Session, headers: Dict[str, str], database_id: int) -> None:
    session.post(
        f"{SUPERSET_URL}/api/v1/database/{database_id}/sync_permissions/",
        headers=headers,
    )


def list_datasets(session: requests.Session, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    resp = _request_with_fallback(session, "GET", f"{SUPERSET_URL}/api/v1/dataset/", headers=headers, params={"page_size": 1000, "page": 0})
    resp.raise_for_status()
    return resp.json().get("result", [])


def _dataset_database_id(dataset: Dict[str, Any]) -> Optional[int]:
    database = dataset.get("database")
    if isinstance(database, dict):
        return database.get("id")
    if isinstance(database, int):
        return database
    return None


def create_dataset(session: requests.Session, headers: Dict[str, str], database_id: int, table_name: str) -> int:
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


def list_dashboards(session: requests.Session, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    resp = _request_with_fallback(session, "GET", f"{SUPERSET_URL}/api/v1/dashboard/", headers=headers, params={"page_size": 1000, "page": 0})
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
    resp = _request_with_fallback(session, "GET", f"{SUPERSET_URL}/api/v1/chart/", headers=headers, params={"page_size": 1000, "page": 0})
    resp.raise_for_status()
    return resp.json().get("result", [])


def create_chart(session: requests.Session, headers: Dict[str, str], dataset_id: int, dashboard_id: int, spec: Dict[str, Any]) -> int:
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

    spatial = form_data.get("spatial")
    if isinstance(spatial, dict):
        spatial_type = spatial.get("type")
        if spatial_type == "latlong":
            lon_col = spatial.get("lonCol")
            lat_col = spatial.get("latCol")
            return [col for col in [lon_col, lat_col] if col]
        if spatial_type == "delimited":
            lonlat_col = spatial.get("lonlatCol")
            return [lonlat_col] if lonlat_col else []
        if spatial_type == "geohash":
            geohash_col = spatial.get("geohashCol")
            return [geohash_col] if geohash_col else []

    return []


def _query_metrics_from_form_data(form_data: Dict[str, Any]) -> List[Any]:
    metric = form_data.get("metric")
    if metric:
        return [metric]

    metrics = form_data.get("metrics")
    if isinstance(metrics, list):
        return metrics

    return []


def build_query_context(form_data: Dict[str, Any], dataset_id: int, chart_id: int) -> Dict[str, Any]:
    query_columns = _query_columns_from_form_data(form_data)
    query_metrics = _query_metrics_from_form_data(form_data)
    row_limit = int(form_data.get("row_limit") or 1000)
    order_desc = bool(form_data.get("sort_desc", form_data.get("order_desc", True)))

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
                "extras": {"having": form_data.get("having", ""), "where": form_data.get("where", "")},
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


def validate_chart_data(session: requests.Session, headers: Dict[str, str], chart_id: int, chart_name: str) -> None:
    resp = session.get(f"{SUPERSET_URL}/api/v1/chart/{chart_id}/data/", headers=headers)
    if resp.status_code == 200:
        return
    snippet = (resp.text or "")[:500]
    _log(f"Chart validation failed for '{chart_name}' (id={chart_id}, status={resp.status_code}): {snippet}")


def update_dashboard_layout(session: requests.Session, headers: Dict[str, str], dashboard_id: int, charts: List[Dict[str, Any]]) -> None:
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {"id": "GRID_ID", "type": "GRID", "parents": ["ROOT_ID"], "children": ["ROW-1", "ROW-2"]},
        "ROW-1": {"id": "ROW-1", "type": "ROW", "parents": ["ROOT_ID", "GRID_ID"], "children": []},
        "ROW-2": {"id": "ROW-2", "type": "ROW", "parents": ["ROOT_ID", "GRID_ID"], "children": []},
    }

    # First row for big numbers, second row for skill bar + heat map.
    row1 = "ROW-1"
    row2 = "ROW-2"
    for idx, chart in enumerate(charts):
        chart_id = chart["id"]
        if idx < 2:
            node_id = f"CHART-ROW1-{idx + 1}"
            position[row1]["children"].append(node_id)
            position[node_id] = {
                "id": node_id,
                "type": "CHART",
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row1],
                "meta": {"chartId": chart_id, "height": 16, "width": 6},
            }
        else:
            node_id = f"CHART-ROW2-{idx + 1}"
            position[row2]["children"].append(node_id)
            position[node_id] = {
                "id": node_id,
                "type": "CHART",
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row2],
                "meta": {"chartId": chart_id, "height": 32, "width": 6},
            }

    payload = {
        "position_json": json.dumps(position),
        "published": True,
    }
    session.put(f"{SUPERSET_URL}/api/v1/dashboard/{dashboard_id}", headers=headers, json=payload)


def main() -> None:
    _log("Starting Superset bootstrap for job market dashboard...")

    session = requests.Session()

    # Superset can take a moment to be ready; retry a few times.
    for attempt in range(1, 11):
        try:
            access_token = get_access_token(session)
            break
        except Exception as exc:
            if attempt == 10:
                raise
            _log(f"Superset not ready yet (attempt {attempt}/10): {exc}")
            time.sleep(5)
    else:
        return

    csrf_token = get_csrf_token(session, access_token)
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-CSRFToken": csrf_token,
        "Content-Type": "application/json",
    }

    # Database
    dbs = list_databases(session, headers)
    db_match = next((db for db in dbs if db.get("database_name") == DATABASE_NAME), None)
    if db_match:
        database_id = db_match.get("id")
        _log(f"Database already exists (id={database_id}).")
    else:
        database_id = create_database(session, headers)
        _log(f"Created database '{DATABASE_NAME}' (id={database_id}).")

    # Datasets
    existing_datasets = list_datasets(session, headers)
    dataset_ids: Dict[str, int] = {}
    for table_name in DATASETS:
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
            dataset_id = create_dataset(session, headers, database_id, table_name)
            dataset_ids[table_name] = dataset_id
            _log(f"Created dataset: {SCHEMA_NAME}.{table_name}")

    # Dashboard
    dashboards = list_dashboards(session, headers)
    dash_match = next((dash for dash in dashboards if dash.get("dashboard_title") == DASHBOARD_TITLE), None)
    if dash_match:
        _log(f"Dashboard already exists (id={dash_match.get('id')}).")
        dashboard_id = dash_match.get("id")
    else:
        dashboard_id = create_dashboard(session, headers)
        _log(f"Created dashboard '{DASHBOARD_TITLE}' (id={dashboard_id}).")

    # Charts
    existing_charts = list_charts(session, headers)
    dataset_map = {name: dataset_id for name, dataset_id in dataset_ids.items()}
    configured_charts: List[Dict[str, Any]] = []
    for spec in CHART_SPECS:
        match = next((ch for ch in existing_charts if ch.get("slice_name") == spec["name"]), None)
        dataset_id = dataset_map.get(spec["dataset"])
        if not dataset_id:
            _log(f"Dataset missing for chart '{spec['name']}', skipping.")
            continue

        if match:
            _log(f"Chart already exists: {spec['name']} (id={match.get('id')})")
            configured_charts.append(
                {
                    "id": match.get("id"),
                    "name": spec["name"],
                    "dataset_id": dataset_id,
                    "form_data": spec["form_data"],
                }
            )
            continue

        chart_id = create_chart(session, headers, dataset_id, dashboard_id, spec)
        _log(f"Created chart '{spec['name']}' (id={chart_id}).")
        configured_charts.append(
            {
                "id": chart_id,
                "name": spec["name"],
                "dataset_id": dataset_id,
                "form_data": spec["form_data"],
            }
        )

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
        _log("Dashboard layout updated with charts.")

    _log("Superset bootstrap completed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        _log(f"Bootstrap failed: {exc}")
        raise
