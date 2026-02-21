"""Source table configuration for Source SP1 vacatures.

This config exactly mirrors the DDL previously hardcoded in
``src.ingestion.common.postgres``.  It is consumed by ``ensure_source_ddl()``
and ``upsert_records()`` in the generic postgres module.
"""
from __future__ import annotations

from src.ingestion.common.source_config import Column, SourceTableConfig

SP1_VACATURES_CONFIG = SourceTableConfig(
    source_name="source_sp1",
    dataset="vacatures",
    schema="source_sp1",
    table="vacatures",
    columns=[
        Column("vacature_id",       "text",                             primary_key=True),
        Column("title",             "text"),
        Column("status",            "text",                             index=True),
        Column("client_name",       "text"),
        Column("location",          "text"),
        Column("publish_date",      "date",                             index=True),
        Column("closing_date",      "date"),
        Column("hours",             "numeric"),
        Column("hours_text",        "text"),
        Column("description",       "text"),
        Column("category",          "text"),
        Column("updated_at_source", "timestamp without time zone"),
        Column("source_url",        "text"),
        Column("bronze_object_path","text"),
        Column("checksum_sha256",   "text"),
        Column("ingested_at",       "timestamp without time zone",      not_null=True),
        Column("is_active",         "boolean"),
    ],
    checksum_column="checksum_sha256",
    ingested_at_column="ingested_at",
)
