import logging
from typing import List, Optional

from soda.common.logs import Logs
from soda.execution.data_source import DataSource

import oracledb

logger = logging.getLogger(__name__)


class OracleDataSource(DataSource):
    def __init__(self, logs: Logs, data_source_name: str, data_source_properties: dict):
        super().__init__(logs, data_source_name, data_source_properties)
        self.host = data_source_properties.get("host")
        self.port = data_source_properties.get("port")
        self.password = data_source_properties.get("password")
        self.username = data_source_properties.get("username")
        #self.connection_timeout = data_source_properties.get("connection_timeout")

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
                host=self.host,
                port=self.port,
                dsn=self.database,
            )
        else:
            raise ConnectionError(f"Invalid Oracle connection properties: invalid host: {self.host}")
        return self.connection

    def expr_regexp_like(self, expr: str, excaped_regex_pattern: str):
        return f"{expr} ~ '{excaped_regex_pattern}'"
