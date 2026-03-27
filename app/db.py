from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

import duckdb
import pandas as pd


class DuckDBRepository:
    """Persist pandas DataFrames into a local DuckDB database."""

    def __init__(self, database_path: str) -> None:
        self.database_path = database_path

    def replace_table(self, table_name: str, dataframe: pd.DataFrame) -> int:
        if dataframe.empty and len(dataframe.columns) == 0:
            return 0

        with self._connect() as connection:
            self._ensure_schema(connection, table_name)
            connection.register("incoming_df", dataframe)
            connection.execute(
                f"create or replace table {self._qualified_name(table_name)} as "
                "select * from incoming_df"
            )
            connection.unregister("incoming_df")

        return len(dataframe)

    def upsert_table(
        self,
        table_name: str,
        dataframe: pd.DataFrame,
        *,
        key_columns: Sequence[str],
    ) -> int:
        if dataframe.empty:
            return 0
        if not key_columns:
            raise ValueError("key_columns must not be empty")

        with self._connect() as connection:
            self._ensure_schema(connection, table_name)
            connection.register("incoming_df", dataframe)
            qualified_name = self._qualified_name(table_name)
            connection.execute(
                f"create table if not exists {qualified_name} as "
                "select * from incoming_df limit 0"
            )
            connection.execute("create or replace temp table incoming_stage as select * from incoming_df")

            join_conditions = " and ".join(
                f'target.{self._quote_identifier(column)} = stage.{self._quote_identifier(column)}'
                for column in key_columns
            )
            connection.execute(
                f"delete from {qualified_name} as target "
                f"using incoming_stage as stage where {join_conditions}"
            )
            connection.execute(f"insert into {qualified_name} by name select * from incoming_stage")
            connection.unregister("incoming_df")

        return len(dataframe)

    def query(self, sql: str) -> pd.DataFrame:
        with self._connect() as connection:
            return connection.execute(sql).df()

    def execute(self, sql: str) -> None:
        with self._connect() as connection:
            connection.execute(sql)

    def _connect(self) -> duckdb.DuckDBPyConnection:
        database_file = Path(self.database_path)
        if database_file.parent != Path("."):
            database_file.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(database_file))

    def _ensure_schema(self, connection: duckdb.DuckDBPyConnection, table_name: str) -> None:
        schema_name, _ = self._split_table_name(table_name)
        if schema_name:
            connection.execute(f"create schema if not exists {self._quote_identifier(schema_name)}")

    def _qualified_name(self, table_name: str) -> str:
        schema_name, bare_table_name = self._split_table_name(table_name)
        if not schema_name:
            return self._quote_identifier(bare_table_name)
        return f"{self._quote_identifier(schema_name)}.{self._quote_identifier(bare_table_name)}"

    def _split_table_name(self, table_name: str) -> tuple[str | None, str]:
        if "." not in table_name:
            return None, table_name
        schema_name, bare_table_name = table_name.split(".", 1)
        return schema_name, bare_table_name

    def _quote_identifier(self, identifier: str) -> str:
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'


def save_summary(repository: DuckDBRepository, summary: Mapping[str, float]) -> int:
    dataframe = pd.DataFrame(
        [{"category": category, "value": value} for category, value in summary.items()]
    )
    return repository.replace_table("analytics.summary", dataframe)
