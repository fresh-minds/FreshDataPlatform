# TEMPLATE — copy and customise for your new source.
# See docs/INGESTION_GUIDE.md § Step 1 for details.
#
# Rename this file to:
#   src/ingestion/<source_name>/config.py
"""Source table configuration for <SOURCE_NAME> <DATASET>.

This config is consumed by ``ensure_source_ddl()`` and ``upsert_records()``
in ``src.ingestion.common.postgres``.
"""
from __future__ import annotations

from src.ingestion.common.source_config import Column, SourceTableConfig

# TODO: Replace with your source's schema/table/columns.
# The column list must exactly match the fields your parser produces.
#
# Guidelines:
#   - First column should be the natural primary key (primary_key=True)
#   - Add index=True to columns you'll filter/sort on frequently
#   - Always include checksum_sha256 (text) and ingested_at (timestamp)
#   - Use snake_case for all column names
#   - Use Postgres types: text, date, numeric, boolean,
#     timestamp without time zone, integer, jsonb

MY_SOURCE_CONFIG = SourceTableConfig(
    source_name="my_source",          # snake_case identifier (used in MinIO paths, metrics)
    dataset="my_dataset",             # dataset within the source
    schema="my_source",               # Postgres schema
    table="my_dataset",               # Postgres table name
    columns=[
        Column("entity_id",           "text",                             primary_key=True),
        Column("title",               "text"),
        Column("status",              "text",                             index=True),
        Column("created_date",        "date",                             index=True),
        Column("description",         "text"),
        Column("source_url",          "text"),
        Column("bronze_object_path",  "text"),
        Column("checksum_sha256",     "text"),
        Column("ingested_at",         "timestamp without time zone",      not_null=True),
    ],
    checksum_column="checksum_sha256",
    ingested_at_column="ingested_at",
)
