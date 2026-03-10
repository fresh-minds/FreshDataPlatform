"""
superset_bootstrap_gold_dashboards.py
Bootstrap a comprehensive Superset dashboard from the gold star schema.

Creates:
    - 3 physical datasets  (fact_it_market_snapshot,
                           fact_it_market_top_skills, fact_job_postings)
  - 2 virtual SQL datasets (vw_aanvragen_monthly, vw_aanvragen_unit_monthly)
  - 10 charts across 5 rows
  - 1 dashboard: "ODP Staffing Demand"

Reuses the "Platform Warehouse" database connection created by the existing
superset_bootstrap_job_market.py script.

Usage:
    SUPERSET_URL=http://localhost:8088 \
    WAREHOUSE_HOST=localhost \
    WAREHOUSE_PORT=5433 \
      python scripts/superset/superset_bootstrap_gold_dashboards.py
"""

import json
import os
import time
from typing import Any, Dict, List, Optional

import requests

# ── Configuration ────────────────────────────────────────────────────────────

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
ADMIN_USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")

WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "warehouse")
WAREHOUSE_PORT = os.getenv("WAREHOUSE_PORT", "5432")
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "odp_dw")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER", "admin")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "admin")

DATABASE_NAME = os.getenv("SUPERSET_WAREHOUSE_NAME", "Platform Warehouse")
DASHBOARD_TITLE = "ODP Staffing Demand"
SCHEMA_NAME = "gold"

# ── Physical Datasets ────────────────────────────────────────────────────────

PHYSICAL_DATASETS = [
    "fact_it_market_snapshot",
    "fact_it_market_top_skills",
    "fact_job_postings",
]

# ── Virtual SQL Datasets ─────────────────────────────────────────────────────

VIRTUAL_DATASETS = [
    {
        "name": "vw_aanvragen_monthly",
        "sql": (
            "SELECT\n"
            "    TO_CHAR(DATE_TRUNC('month', date_received), 'YYYY-MM') AS month,\n"
            "    COUNT(*) AS request_count\n"
            "FROM gold.fact_job_postings\n"
            "WHERE posting_type = 'internal_request'\n"
            "GROUP BY 1\n"
            "ORDER BY 1"
        ),
    },
    {
        "name": "vw_aanvragen_unit_monthly",
        "sql": (
            "SELECT\n"
            "    TO_CHAR(DATE_TRUNC('month', date_received), 'YYYY-MM') AS month,\n"
            "    unit_name AS unit,\n"
            "    COUNT(*) AS request_count\n"
            "FROM gold.fact_job_postings\n"
            "WHERE posting_type = 'internal_request'\n"
            "  AND unit_name IS NOT NULL\n"
            "GROUP BY 1, 2\n"
            "ORDER BY 1, 2"
        ),
    },
]

# ── Chart Specifications ─────────────────────────────────────────────────────

CHART_SPECS: List[Dict[str, Any]] = [
    # ── Row 1: KPI cards ──────────────────────────────────────────────
    {
        "name": "Total Staffing Requests",
        "dataset": "fact_job_postings",
        "viz_type": "big_number_total",
        "form_data": {
            "viz_type": "big_number_total",
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "posting_id"},
                "aggregate": "COUNT",
                "label": "COUNT(posting_id)",
                "hasCustomLabel": True,
                "optionName": "metric_total_requests",
            },
            "subheader": "Staffing requests since Sep 2020",
            "adhoc_filters": [
                {
                    "clause": "WHERE",
                    "comparator": "",
                    "expressionType": "SQL",
                    "operator": "",
                    "sqlExpression": "posting_type = 'internal_request'",
                    "subject": "",
                    "filterOptionName": "filter_internal_requests_total",
                }
            ],
            "row_limit": 1000,
        },
    },
    {
        "name": "Unique Companies",
        "dataset": "fact_job_postings",
        "viz_type": "big_number_total",
        "form_data": {
            "viz_type": "big_number_total",
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "company_name"},
                "aggregate": "COUNT_DISTINCT",
                "label": "COUNT_DISTINCT(company_name)",
                "hasCustomLabel": True,
                "optionName": "metric_unique_companies",
            },
            "subheader": "Distinct hiring organisations",
            "adhoc_filters": [
                {
                    "clause": "WHERE",
                    "comparator": "",
                    "expressionType": "SQL",
                    "operator": "",
                    "sqlExpression": "posting_type = 'internal_request'",
                    "subject": "",
                    "filterOptionName": "filter_internal_requests_companies",
                }
            ],
            "row_limit": 1000,
        },
    },
    {
        "name": "CBS IT Vacancies (NL)",
        "dataset": "fact_it_market_snapshot",
        "viz_type": "big_number_total",
        "form_data": {
            "viz_type": "big_number_total",
            "metric": {
                "expressionType": "SIMPLE",
                "column": {"column_name": "vacancies"},
                "aggregate": "SUM",
                "label": "SUM(vacancies)",
                "hasCustomLabel": False,
                "optionName": "metric_cbs_vacancies",
            },
            "subheader": "CBS national IT vacancy count",
            "adhoc_filters": [],
            "row_limit": 1000,
        },
    },
    # ── Row 2: Monthly trend ──────────────────────────────────────────
    # Use dist_bar (not bar) — "bar" is Superset's time-series bar chart and
    # requires a configured datetime column on the dataset, which vw_aanvragen_monthly
    # does not have (month is a formatted string, not a timestamp column).
    {
        "name": "Monthly Request Volume",
        "dataset": "vw_aanvragen_monthly",
        "viz_type": "dist_bar",
        "form_data": {
            "viz_type": "dist_bar",
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "request_count"},
                    "aggregate": "SUM",
                    "label": "Requests",
                    "hasCustomLabel": True,
                    "optionName": "metric_monthly_count",
                }
            ],
            "groupby": ["month"],
            "adhoc_filters": [],
            "row_limit": 200,
            "sort_desc": False,
            "order_desc": False,
            "x_axis_label": "Month",
            "y_axis_label": "Requests",
            "bottom_margin": "auto",
            "color_scheme": "supersetColors",
            "show_legend": False,
        },
    },
    # ── Row 3: Pie + Bar ──────────────────────────────────────────────
    {
        "name": "Request Source Breakdown",
        "dataset": "fact_job_postings",
        "viz_type": "pie",
        "form_data": {
            "viz_type": "pie",
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "posting_id"},
                    "aggregate": "COUNT",
                    "label": "COUNT(posting_id)",
                    "hasCustomLabel": False,
                    "optionName": "metric_source_count",
                }
            ],
            "groupby": ["source"],
            "adhoc_filters": [
                {
                    "clause": "WHERE",
                    "comparator": "",
                    "expressionType": "SQL",
                    "operator": "",
                    "sqlExpression": "posting_type = 'internal_request'",
                    "subject": "",
                    "filterOptionName": "filter_internal_requests_source",
                }
            ],
            "row_limit": 10,
            "donut": True,
            "show_labels": True,
            "labels_outside": True,
            "show_legend": True,
            "color_scheme": "supersetColors",
        },
    },
    {
        "name": "Business Unit Distribution",
        "dataset": "fact_job_postings",
        "viz_type": "pie",
        "form_data": {
            "viz_type": "pie",
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "posting_id"},
                    "aggregate": "COUNT",
                    "label": "COUNT(posting_id)",
                    "hasCustomLabel": False,
                    "optionName": "metric_unit_count",
                }
            ],
            "groupby": ["unit_name"],
            "adhoc_filters": [
                {
                    "clause": "WHERE",
                    "comparator": "",
                    "expressionType": "SQL",
                    "operator": "",
                    "sqlExpression": "unit_name IS NOT NULL",
                    "subject": "",
                    "filterOptionName": "filter_unit_not_null",
                },
                {
                    "clause": "WHERE",
                    "comparator": "",
                    "expressionType": "SQL",
                    "operator": "",
                    "sqlExpression": "posting_type = 'internal_request'",
                    "subject": "",
                    "filterOptionName": "filter_internal_requests_unit",
                }
            ],
            "row_limit": 20,
            "donut": True,
            "show_labels": True,
            "labels_outside": True,
            "show_legend": True,
            "color_scheme": "supersetColors",
        },
    },
    {
        "name": "Top 15 Locations",
        "dataset": "fact_job_postings",
        "viz_type": "dist_bar",
        "form_data": {
            "viz_type": "dist_bar",
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "posting_id"},
                    "aggregate": "COUNT",
                    "label": "Requests",
                    "hasCustomLabel": True,
                    "optionName": "metric_location_count",
                }
            ],
            "groupby": ["location_name"],
            "adhoc_filters": [
                {
                    "clause": "WHERE",
                    "comparator": "",
                    "expressionType": "SQL",
                    "operator": "",
                    "sqlExpression": "location_name IS NOT NULL",
                    "subject": "",
                    "filterOptionName": "filter_location_not_null",
                },
                {
                    "clause": "WHERE",
                    "comparator": "",
                    "expressionType": "SQL",
                    "operator": "",
                    "sqlExpression": "posting_type = 'internal_request'",
                    "subject": "",
                    "filterOptionName": "filter_internal_requests_location",
                }
            ],
            "row_limit": 15,
            "sort_desc": True,
            "order_desc": True,
            "contribution": False,
            "show_legend": False,
            "color_scheme": "supersetColors",
            "x_axis_label": "Location",
            "y_axis_label": "Requests",
            "bottom_margin": "auto",
        },
    },
    # ── Row 4: Stacked bar + table ────────────────────────────────────
    {
        "name": "Unit Trend Over Time",
        "dataset": "vw_aanvragen_unit_monthly",
        "viz_type": "dist_bar",
        "form_data": {
            "viz_type": "dist_bar",
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "request_count"},
                    "aggregate": "SUM",
                    "label": "Requests",
                    "hasCustomLabel": True,
                    "optionName": "metric_unit_monthly_count",
                }
            ],
            "groupby": ["month"],
            "columns": ["unit"],
            "adhoc_filters": [],
            "row_limit": 5000,
            "sort_desc": False,
            "order_desc": False,
            "contribution": False,
            "show_bar_value": False,
            "bar_stacked": True,
            "show_legend": True,
            "color_scheme": "supersetColors",
            "x_axis_label": "Month",
            "y_axis_label": "Requests",
            "bottom_margin": "auto",
        },
    },
    {
        "name": "Top 20 Companies by Requests",
        "dataset": "fact_job_postings",
        "viz_type": "table",
        "form_data": {
            "viz_type": "table",
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "posting_id"},
                    "aggregate": "COUNT",
                    "label": "Requests",
                    "hasCustomLabel": True,
                    "optionName": "metric_company_count",
                }
            ],
            "groupby": ["company_name"],
            "all_columns": [],
            "adhoc_filters": [
                {
                    "clause": "WHERE",
                    "comparator": "",
                    "expressionType": "SQL",
                    "operator": "",
                    "sqlExpression": "company_name IS NOT NULL",
                    "subject": "",
                    "filterOptionName": "filter_company_not_null",
                },
                {
                    "clause": "WHERE",
                    "comparator": "",
                    "expressionType": "SQL",
                    "operator": "",
                    "sqlExpression": "posting_type = 'internal_request'",
                    "subject": "",
                    "filterOptionName": "filter_internal_requests_company",
                }
            ],
            "row_limit": 20,
            "sort_desc": True,
            "order_desc": True,
            "include_search": True,
            "table_timestamp_format": "smart_date",
            "page_length": 20,
        },
    },
    # ── Row 5: Skills ─────────────────────────────────────────────────
    # dist_bar (not bar) — same reason as Monthly Request Volume above.
    {
        "name": "Top IT Skills (Market)",
        "dataset": "fact_it_market_top_skills",
        "viz_type": "dist_bar",
        "form_data": {
            "viz_type": "dist_bar",
            "metrics": [
                {
                    "expressionType": "SIMPLE",
                    "column": {"column_name": "mention_count"},
                    "aggregate": "SUM",
                    "label": "Mentions",
                    "hasCustomLabel": True,
                    "optionName": "metric_skill_mentions",
                }
            ],
            "groupby": ["skill"],
            "adhoc_filters": [],
            "row_limit": 50,
            "sort_desc": True,
            "order_desc": True,
            "show_legend": False,
            "color_scheme": "supersetColors",
            "x_axis_label": "Skill",
            "y_axis_label": "Mentions",
            "bottom_margin": "auto",
        },
    },
]

# ── Dashboard Grid Layout ────────────────────────────────────────────────────
#
# ROW-1  (h=12):  KPI Total (w=4)  |  KPI Companies (w=4)  |  KPI CBS (w=4)
# ROW-2  (h=28):  Monthly Trend (w=12)
# ROW-3  (h=28):  Source Pie (w=4)  |  Unit Pie (w=4)  |  Top Locations (w=4)
# ROW-4  (h=28):  Unit Trend (w=6) |  Top Companies (w=6)
# ROW-5  (h=24):  Top Skills (w=6) |  (reserved for future charts) (w=6)

LAYOUT_ROWS = [
    {"row_id": "ROW-1", "height": 12, "charts_width": [4, 4, 4]},     # 3 KPI cards
    {"row_id": "ROW-2", "height": 28, "charts_width": [12]},           # monthly trend
    {"row_id": "ROW-3", "height": 28, "charts_width": [4, 4, 4]},     # 2 pies + bar
    {"row_id": "ROW-4", "height": 28, "charts_width": [6, 6]},        # stacked + table
    {"row_id": "ROW-5", "height": 24, "charts_width": [6]},           # skills
]


# ── Utility Functions ────────────────────────────────────────────────────────

def _log(message: str) -> None:
    print(f"[Gold Dashboard] {message}")


def _request_with_fallback(session: requests.Session, method: str, url: str, **kwargs):
    response = session.request(method, url, **kwargs)
    if response.status_code == 200:
        return response
    if method.lower() == "get" and "page_size" in (kwargs.get("params") or {}):
        params = kwargs.get("params", {})
        page_size = params.get("page_size", 1000)
        response = session.request(
            method, url,
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
    return resp.json().get("access_token")


def get_csrf_token(session: requests.Session, access_token: str) -> str:
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", headers=headers)
    resp.raise_for_status()
    return resp.json().get("result")


def list_databases(session: requests.Session, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    resp = _request_with_fallback(
        session, "GET", f"{SUPERSET_URL}/api/v1/database/",
        headers=headers, params={"page_size": 1000, "page": 0},
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


def update_database(session: requests.Session, headers: Dict[str, str], database_id: int) -> None:
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
    resp = session.put(
        f"{SUPERSET_URL}/api/v1/database/{database_id}",
        headers=headers, json=payload,
    )
    resp.raise_for_status()


def list_datasets(session: requests.Session, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    resp = _request_with_fallback(
        session, "GET", f"{SUPERSET_URL}/api/v1/dataset/",
        headers=headers, params={"page_size": 1000, "page": 0},
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
    """Register a virtual (SQL-defined) dataset in Superset."""
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
        session, "GET", f"{SUPERSET_URL}/api/v1/dashboard/",
        headers=headers, params={"page_size": 1000, "page": 0},
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
        session, "GET", f"{SUPERSET_URL}/api/v1/chart/",
        headers=headers, params={"page_size": 1000, "page": 0},
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


# ── Query Context ─────────────────────────────────────────────────────────────

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
    """Extract SQL-type adhoc_filters into a WHERE clause string."""
    adhoc = form_data.get("adhoc_filters", [])
    clauses = []
    for f in adhoc:
        if f.get("expressionType") == "SQL" and f.get("clause") == "WHERE":
            sql_expr = f.get("sqlExpression", "").strip()
            if sql_expr:
                clauses.append(sql_expr)
    existing_where = form_data.get("where", "")
    if existing_where:
        clauses.insert(0, existing_where)
    return " AND ".join(clauses)


def build_query_context(
    form_data: Dict[str, Any],
    dataset_id: int,
    chart_id: int,
) -> Dict[str, Any]:
    query_columns = _query_columns_from_form_data(form_data)
    query_metrics = _query_metrics_from_form_data(form_data)
    row_limit = int(form_data.get("row_limit") or 1000)
    order_desc = bool(form_data.get("sort_desc", form_data.get("order_desc", True)))
    where_clause = _extract_where_from_adhoc(form_data)

    context_form_data = form_data.copy()
    context_form_data.update({
        "datasource": f"{dataset_id}__table",
        "slice_id": chart_id,
        "force": False,
        "result_format": "json",
        "result_type": "full",
    })

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
    resp = session.put(
        f"{SUPERSET_URL}/api/v1/chart/{chart_id}",
        headers=headers, json=payload,
    )
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
    _log(f"  ✗ Chart validation failed for '{chart_name}' "
         f"(id={chart_id}, status={resp.status_code}): {snippet}")


# ── Dashboard Layout ─────────────────────────────────────────────────────────

def update_dashboard_layout(
    session: requests.Session,
    headers: Dict[str, str],
    dashboard_id: int,
    charts: List[Dict[str, Any]],
) -> None:
    """Build a 5-row dashboard grid layout and push it via the API."""

    row_ids = [row["row_id"] for row in LAYOUT_ROWS]
    position: Dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "id": "ROOT_ID",
            "type": "ROOT",
            "children": ["GRID_ID"],
        },
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "parents": ["ROOT_ID"],
            "children": row_ids,
        },
    }

    # Initialise row containers.
    # meta MUST be present (even if empty) — Row.jsx reads meta.background and
    # crashes with "Cannot read properties of undefined" when meta is absent.
    for row in LAYOUT_ROWS:
        position[row["row_id"]] = {
            "id": row["row_id"],
            "type": "ROW",
            "parents": ["ROOT_ID", "GRID_ID"],
            "children": [],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }

    # Place charts into rows
    chart_idx = 0
    for row in LAYOUT_ROWS:
        for col_idx, width in enumerate(row["charts_width"]):
            if chart_idx >= len(charts):
                break
            chart = charts[chart_idx]
            # Node ID format: CHART-ROW1-1 (no hyphen between ROW and number)
            row_label = row['row_id'].replace('-', '')  # ROW-1 → ROW1
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
    resp = session.put(
        f"{SUPERSET_URL}/api/v1/dashboard/{dashboard_id}",
        headers=headers, json=payload,
    )
    if resp.status_code in (200, 201):
        _log("Dashboard layout updated successfully.")
    else:
        _log(f"Dashboard layout update returned status {resp.status_code}: "
             f"{(resp.text or '')[:500]}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    _log("Starting gold dashboard bootstrap...")

    session = requests.Session()

    # Auth — retry loop (Superset may still be starting up)
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

    # ── Database ──────────────────────────────────────────────────────
    dbs = list_databases(session, headers)
    db_match = next(
        (db for db in dbs if db.get("database_name") == DATABASE_NAME), None,
    )
    if db_match:
        database_id = db_match.get("id")
        _log(f"Reusing existing database '{DATABASE_NAME}' (id={database_id}).")
        # Don't update the connection — it may have Docker-internal settings
        # that differ from the host-level env vars.
    else:
        database_id = create_database(session, headers)
        _log(f"Created database '{DATABASE_NAME}' (id={database_id}).")

    # ── Datasets ──────────────────────────────────────────────────────
    existing_datasets = list_datasets(session, headers)
    dataset_ids: Dict[str, int] = {}

    # Physical datasets
    for table_name in PHYSICAL_DATASETS:
        match = next(
            (
                ds for ds in existing_datasets
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

    # Virtual SQL datasets
    for vds in VIRTUAL_DATASETS:
        name = vds["name"]
        match = next(
            (
                ds for ds in existing_datasets
                if ds.get("table_name") == name
                and _dataset_database_id(ds) == database_id
            ),
            None,
        )
        if match:
            dataset_ids[name] = match.get("id")
            _log(f"Virtual dataset already exists: {name}")
        else:
            ds_id = create_virtual_dataset(
                session, headers, database_id, name, vds["sql"],
            )
            dataset_ids[name] = ds_id
            _log(f"Created virtual dataset: {name}")

    # ── Dashboard ─────────────────────────────────────────────────────
    dashboards = list_dashboards(session, headers)
    dash_match = next(
        (d for d in dashboards if d.get("dashboard_title") == DASHBOARD_TITLE),
        None,
    )
    if dash_match:
        dashboard_id = dash_match.get("id")
        _log(f"Dashboard already exists (id={dashboard_id}).")
    else:
        dashboard_id = create_dashboard(session, headers)
        _log(f"Created dashboard '{DASHBOARD_TITLE}' (id={dashboard_id}).")

    # ── Charts ────────────────────────────────────────────────────────
    existing_charts = list_charts(session, headers)
    configured_charts: List[Dict[str, Any]] = []

    for spec in CHART_SPECS:
        ds_id = dataset_ids.get(spec["dataset"])
        if not ds_id:
            _log(f"Dataset missing for chart '{spec['name']}', skipping.")
            continue

        match = next(
            (ch for ch in existing_charts if ch.get("slice_name") == spec["name"]),
            None,
        )
        if match:
            chart_id = match.get("id")
            _log(f"Chart already exists: '{spec['name']}' (id={chart_id})")
        else:
            chart_id = create_chart(session, headers, ds_id, dashboard_id, spec)
            _log(f"Created chart: '{spec['name']}' (id={chart_id})")

        configured_charts.append({
            "id": chart_id,
            "name": spec["name"],
            "dataset_id": ds_id,
            "form_data": spec["form_data"],
        })

    # ── Query Context ─────────────────────────────────────────────────
    _log("Setting query context for each chart...")
    for chart in configured_charts:
        cid = chart["id"]
        if not cid:
            continue
        update_chart_query_context(
            session, headers,
            chart_id=cid,
            dataset_id=chart["dataset_id"],
            form_data=chart["form_data"],
        )
        validate_chart_data(session, headers, chart_id=cid, chart_name=chart["name"])

    # ── Dashboard Layout ──────────────────────────────────────────────
    if configured_charts:
        update_dashboard_layout(session, headers, dashboard_id, configured_charts)

    _log(f"Gold dashboard bootstrap completed. "
         f"{len(configured_charts)} charts configured on dashboard id={dashboard_id}.")
    _log(f"Open: {SUPERSET_URL}/superset/dashboard/list/")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        _log(f"Bootstrap failed: {exc}")
        raise
