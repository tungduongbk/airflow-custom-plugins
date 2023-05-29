import sys
import base64
if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class MoEngageDataApiHook(BaseHook):
    """
        A Hook to load moengage secret information from airflow config
        and prepare authorization's encrytion
    """

    conn_name_attr = 'data_api_conn'
    default_conn_name = 'moengage_data_api'
    conn_type = 'moengage_data_api'
    hook_name = 'MoEngage Data Api'

    def __init__(self, data_api_conn=default_conn_name):
        self.data_api_conn = data_api_conn
        self.parse_connection()

    def parse_connection(self):
        self.log.info("Fetching MoEngage connection: %s", self.data_api_conn)
        conn = self.get_connection(self.data_api_conn)
        self.endpoint = conn.host
        self.app_id = conn.schema
        self.data_api_id = conn.login
        self.data_api_key = conn.password

    def get_b64_authorization(self) -> str:
        auth = f"{self.data_api_id}:{self.data_api_key}"
        auth_b64 = base64.b64encode(auth.encode("ascii"))
        return auth_b64
