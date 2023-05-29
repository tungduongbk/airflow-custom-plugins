from typing import Tuple
from airflow.hooks.base import BaseHook


class AmplitudeHook(BaseHook):
    def __init__(self, amplitude_conn='amplitude_conn', **kwargs):
        super(AmplitudeHook, self).__init__(source='amplitude')
        self.http_conn_id = amplitude_conn
        self.base_url = None
        self._auth = None

    def get_conn(self) -> Tuple[str, Tuple[str, str]]:
        conn = self.get_connection(self.http_conn_id)
        if conn.host and "://" in conn.host:
            self.base_url = conn.host
        else:
            # schema defaults to HTTP
            schema = conn.schema if conn.schema else "http"
            host = conn.host if conn.host else ""
            self.base_url = schema + "://" + host
        if conn.port:
            self.base_url = self.base_url + ":" + str(conn.port)
        if conn.login:
            self._auth = (conn.login, conn.password)
        return self.base_url, self._auth
