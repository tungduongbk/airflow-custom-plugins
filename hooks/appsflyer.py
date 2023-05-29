from typing import Any, Dict
from airflow import AirflowException
from airflow.hooks.base import BaseHook

class AppsflyerHook(BaseHook):
    conn_name_attr = 'appsflyer_conn_id'
    default_conn_name = 'appsflyer_report_conn'
    conn_type = 'appsflyer'
    hook_name = 'Appsflyer'
    
    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["host", "schema", "port"],
            "relabeling": {
                "login": 'App IDs',
                "password": "Api Token"
            },
        }
    
    def __init__(self, appsflyer_conn_id: str = default_conn_name):
        self.appsflyer_conn_id = appsflyer_conn_id
        conn = self.get_connection(self.appsflyer_conn_id)
        if not conn.password:
            raise AirflowException("Must provide api token")
        self.__app_ids = conn.login.split(",")
        self.__api_token = conn.password
        self.__timezone = conn.extra_dejson.get("timezone", "Asia/Ho_Chi_Minh")

    def get_conn(self) -> Dict[str, Any]:
        return {
            "app_ids": self.__app_ids,
            "api_token": self.__api_token,
            "timezone": self.__timezone
        }
