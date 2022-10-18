from __future__ import annotations

import logging
import os

from helpers.data_source_fixture import DataSourceFixture

logger = logging.getLogger(__name__)


class OracleDataSourceFixture(DataSourceFixture):
    def _build_configuration_dict(self, schema_name: str | None = None) -> dict:
        return {
            "data_source oracle": {
                "type": "oracle",
                "host": os.getenv("ORACLE_HOST", "localhost"),
                "username": os.getenv("ORACLE_USERNAME", "system"),
                "password": os.getenv("ORACLE_PASSWORD", "admin"),
                "database": schema_name if schema_name else os.getenv("ORACLE_DATABASE", "ORCLCDB"),
            }
        }

    def _create_schema_data_source(self) -> DataSource:
        schema_data_source = super()._create_schema_data_source()
        schema_data_source.connection.set_session(autocommit=True)
        return schema_data_source

    def _drop_schema_if_exists(self):
        super()._drop_schema_if_exists()

    def _create_schema_if_not_exists_sql(self):
        return f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}"

    def _drop_schema_if_exists_sql(self):
        return f"DROP SCHEMA IF EXISTS {self.schema_name}"
