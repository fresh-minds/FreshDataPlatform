"""
Spark helper utilities for data processing.

Provides common utilities for working with PySpark DataFrames,
including type conversion and schema handling.
"""

from typing import Any, List, Dict

import pandas as pd


def clean_df_for_spark(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean a Pandas DataFrame to make it Spark-compatible.

    Handles common issues that cause Spark to fail on DataFrame conversion:
    - Mixed-type columns (e.g., strings and numbers in same column)
    - All-null columns (Spark can't infer type)
    - Nested types (dicts, lists) that can't be directly converted

    Args:
        df: Input Pandas DataFrame

    Returns:
        Cleaned DataFrame safe for spark.createDataFrame()
    """
    if df.empty:
        return df

    for col in df.columns:
        unique_types = df[col].apply(lambda x: type(x)).unique()

        # All nulls - convert to string as safe default
        if df[col].isnull().all():
            df[col] = df[col].astype(str)
            continue

        # Single type - no conversion needed
        if len(unique_types) <= 1:
            continue

        types_set = set(unique_types)

        # Handle common mixed-type scenarios
        if str in types_set and (float in types_set or int in types_set):
            # String + numeric -> string
            df[col] = df[col].astype(str)
        elif float in types_set and bool in types_set:
            # Float + bool -> float
            df[col] = df[col].replace({True: 1.0, False: 0.0}).astype(float)
        elif str in types_set and bool in types_set:
            # String + bool -> string
            df[col] = df[col].astype(str)
        elif float in types_set and int in types_set:
            # Float + int -> float
            df[col] = df[col].astype(float)
        else:
            # Fallback for complex types (dicts, lists)
            df[col] = df[col].astype(str)

    return df


def flatten_nested_column(df: pd.DataFrame, column: str, prefix: str = "") -> pd.DataFrame:
    """
    Flatten a column containing nested dictionaries.

    Args:
        df: Input DataFrame
        column: Name of column containing dicts
        prefix: Prefix for flattened column names

    Returns:
        DataFrame with nested column expanded
    """
    if column not in df.columns:
        return df

    # Extract nested data
    nested_df = pd.json_normalize(df[column].dropna().tolist())

    if nested_df.empty:
        return df

    # Add prefix to column names
    if prefix:
        nested_df.columns = [f"{prefix}_{c}" for c in nested_df.columns]

    # Drop original column and merge flattened data
    df = df.drop(columns=[column])
    df = pd.concat([df.reset_index(drop=True), nested_df.reset_index(drop=True)], axis=1)

    return df


def safe_column_rename(df: pd.DataFrame, rename_map: Dict[str, str]) -> pd.DataFrame:
    """
    Safely rename columns, ignoring columns that don't exist.

    Args:
        df: Input DataFrame
        rename_map: Dictionary of old_name -> new_name

    Returns:
        DataFrame with renamed columns
    """
    existing_renames = {k: v for k, v in rename_map.items() if k in df.columns}
    return df.rename(columns=existing_renames)


def drop_columns_if_exist(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Drop columns from DataFrame, ignoring those that don't exist.

    Args:
        df: Input DataFrame
        columns: List of column names to drop

    Returns:
        DataFrame with columns removed
    """
    return df.drop(columns=[c for c in columns if c in df.columns], errors="ignore")


def get_spark_session(app_name: str = "LakehouseApp") -> Any:
    """
    Get or create a SparkSession.

    Compatibility wrapper around `shared.fabric.runtime.get_spark_session` so the
    codebase uses a single Spark configuration path.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        print("PySpark not installed. Returning None (Mock mode).")
        return None

    # Stop existing session to ensure fresh config for local runs.
    active_session = SparkSession.getActiveSession()
    if active_session:
        active_session.stop()

    # Import lazily to avoid import-time side effects for callers that never use Spark.
    from shared.fabric.runtime import get_spark_session as _get_spark_session

    return _get_spark_session(app_name)
