import logging
from typing import List, Optional

from soda.common.logs import Logs
from soda.execution.data_source import DataSource
from soda.execution.data_type import DataType

import oracledb

logger = logging.getLogger(__name__)


class OracleDataSource(DataSource):
    TYPE = "oracle"

    SQL_TYPE_FOR_CREATE_TABLE_MAP: dict = {
        DataType.TIMESTAMP_TZ: "timestamp with time zone",
        DataType.TEXT: "varchar(255)",
    }

    def __init__(self, logs: Logs, data_source_name: str, data_source_properties: dict):
        super().__init__(logs, data_source_name, data_source_properties)
        self.host = data_source_properties.get("host", "localhost")
        self.port = data_source_properties.get("port", 1521)
        self.password = data_source_properties.get("password", "adminpass")
        self.username = data_source_properties.get("username", "sodatestuesr")
        self.database = data_source_properties.get("database", "ORCLCDB")

    def connect(self):
        if self.password == "":
            self.password = None

        if isinstance(self.host, str) and len(self.host) > 0:
            # self.logs.debug(
            #     f'Oracle connection properties: host="{self.host}", port="{self.port}", database="{self.database}", user="{self.username}", options="{options}", connection_timeout="{self.connection_timeout}"'
            # )
            self.connection = oracledb.connect(
                user=self.username,
                password=self.password,
                dsn=f"{self.host}:{self.port}/{self.database}",
            )
        else:
            raise ConnectionError(f"Invalid Oracle connection properties: invalid host: {self.host}")
        return self.connection

    def sql_select_all(self, table_name, limit=None, filter=None) -> str:
        qualified_table_name = self.qualified_table_name(table_name)

        filter_sql = ""
        if filter:
            filter_sql = f" \n WHERE {filter}"

        limit_sql = ""
        if limit and filter:
            limit_sql = f" \n AND ROWNUM <= {limit}"
        elif limit and not filter:
            limit_sql = f" \n WHERE ROWNUM <= {limit}"

        sql = f"SELECT * FROM {qualified_table_name}{filter_sql}{limit_sql}"
        return sql

    def sql_get_duplicates(
        self, column_names, table_name, filter, limit=None
    ):
        sql = f"""WITH frequencies AS (
              SELECT {column_names}, COUNT(*) AS frequency
              FROM {table_name}
              WHERE {filter}
              GROUP BY {column_names})
            SELECT *
            FROM frequencies
            WHERE frequency > 1"""

        if limit:
            sql += f"\n AND ROWNUM <= {limit}"

        return sql

    def sql_information_schema_tables(self) -> str:
        return "ALL_TABLES"

    def sql_information_schema_columns(self) -> str:
        return "ALL_TAB_COLUMNS"

    def expr_regexp_like(self, expr: str, excaped_regex_pattern: str):
        return f"{expr} ~ '{excaped_regex_pattern}'"
