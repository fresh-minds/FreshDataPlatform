"""Declarative table configuration for ingestion sources.

Every source defines a ``SourceTableConfig`` that describes its Postgres
silver table.  The common ``postgres`` module uses this config to generate
DDL, upsert SQL, and indexes — no hardcoded SQL per source.

Example
-------
>>> from src.ingestion.common.source_config import SourceTableConfig, Column
>>> MY_CONFIG = SourceTableConfig(
...     source_name="acme_portal",
...     dataset="orders",
...     schema="acme_portal",
...     table="orders",
...     columns=[
...         Column("order_id", "text", primary_key=True),
...         Column("customer", "text"),
...         Column("amount", "numeric"),
...         Column("order_date", "date", index=True),
...     ],
... )
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Column:
    """A single column in a silver table."""

    name: str
    pg_type: str                          # e.g. "text", "date", "numeric", "boolean", "timestamp without time zone"
    primary_key: bool = False
    index: bool = False
    not_null: bool = False                # Extra NOT NULL (PK is always NOT NULL)


@dataclass(frozen=True)
class SourceTableConfig:
    """Describes a source's silver table so that DDL and upsert SQL can be
    generated automatically.

    Attributes
    ----------
    source_name : str
        Snake-case identifier used in MinIO paths, Airflow tags, dbt source
        names, and StatsD metrics (e.g. ``"source_sp1"``).
    dataset : str
        Dataset within the source (e.g. ``"vacatures"``).  A source can have
        multiple datasets, each with its own config.
    schema : str
        Postgres schema for the silver table.
    table : str
        Postgres table name inside *schema*.
    columns : list[Column]
        Ordered column definitions.  The first column marked ``primary_key``
        becomes the ``PRIMARY KEY`` constraint.  Columns named
        ``checksum_column`` and ``ingested_at_column`` are handled specially
        by the upsert logic.
    checksum_column : str
        Column that stores the SHA-256 content checksum.  Used in the
        ``ON CONFLICT … WHERE checksum IS DISTINCT FROM`` guard.
    ingested_at_column : str
        Column with ``DEFAULT now()`` that is excluded from the upsert SET
        clause so the original ingestion timestamp is preserved on updates.
    """

    source_name: str
    dataset: str
    schema: str
    table: str
    columns: list[Column]
    checksum_column: str = "checksum_sha256"
    ingested_at_column: str = "ingested_at"

    # -- Derived helpers -------------------------------------------------------

    @property
    def fqn(self) -> str:
        """Fully qualified table name: ``schema.table``."""
        return f"{self.schema}.{self.table}"

    @property
    def pk_column(self) -> str:
        """Name of the primary-key column."""
        for col in self.columns:
            if col.primary_key:
                return col.name
        raise ValueError(f"No primary_key column defined for {self.fqn}")

    @property
    def data_columns(self) -> list[Column]:
        """All columns except PK and ingested_at (used in upsert SET)."""
        return [
            c for c in self.columns
            if not c.primary_key and c.name != self.ingested_at_column
        ]

    @property
    def insert_columns(self) -> list[str]:
        """Column names for the INSERT (everything except ingested_at)."""
        return [c.name for c in self.columns if c.name != self.ingested_at_column]

    @property
    def indexed_columns(self) -> list[Column]:
        """Columns that should have a B-tree index."""
        return [c for c in self.columns if c.index and not c.primary_key]
