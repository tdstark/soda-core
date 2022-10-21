from __future__ import annotations

import logging
import os

from helpers.data_source_fixture import DataSourceFixture

logger = logging.getLogger(__name__)


class OracleDataSourceFixture(DataSourceFixture):
    def __init__(self, test_data_source: str):
        super().__init__(test_data_source=test_data_source)

    def _build_configuration_dict(self, schema_name: str | None = None) -> dict:
        return {
            "data_source oracle": {
                "type": "oracle",
                "host": os.getenv("ORACLE_HOST", "localhost"),
                "username": os.getenv("ORACLE_USERNAME", "sodatestuesr"),
                "password": os.getenv("ORACLE_PASSWORD", "adminpass"),
                "database": os.getenv("ORACLE_DATABASE", "ORCLCDB"),
            }
        }

    def _create_schema_name(self):
        return f'"SODATESTUESR"'

    def _create_schema_if_not_exists_sql(self) -> str:
        return "SELECT 1 FROM dual"

    def _create_view_from_table_sql(self, test_table: TestTable):
        return f'CREATE OR REPLACE VIEW "SODATESTUESR".{test_table.unique_view_name} AS SELECT * FROM "SODATESTUESR".{test_table.unique_table_name}'

    def _create_test_table_sql_compose(self, qualified_table_name, columns_sql) -> str:
        return f'CREATE TABLE "SODATESTUESR".{qualified_table_name} ( \n{columns_sql} \n)'

    def _drop_schema_if_exists_sql(self) -> str:
        return "SELECT 1 FROM dual"

    def _drop_test_table_sql(self, table_name):
        qualified_table_name = self.data_source.qualified_table_name(table_name)
        return f'DROP TABLE "SODATESTUESR"."{qualified_table_name}" PURGE'

    def _insert_test_table_sql(self, test_table: TestTable) -> str:
        if test_table.values:
            quoted_table_name = (
                self.data_source.quote_table(test_table.unique_table_name)
                if test_table.quote_names
                else test_table.unique_table_name
            )
            qualified_table_name = self.data_source.qualified_table_name(quoted_table_name)

            def sql_test_table_row(row):
                return ",".join(self.data_source.literal(value) for value in row)

            rows_sql = ",\n".join([f"  ({sql_test_table_row(row)})" for row in test_table.values])
            rows_sql = rows_sql.replace("date", "to_date")
            rows_sql = rows_sql.replace("None", "NULL")
            return f'INSERT INTO "SODATESTUESR".{qualified_table_name} VALUES \n" f"{rows_sql}'
