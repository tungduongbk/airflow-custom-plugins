from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
from api.appstoreconnect import Api as AppStoreConnectApi


class AppStoreConnectHook(BaseHook, LoggingMixin):
    conn_name_attr = 'appstore_connect_conn_id'
    default_conn_name = 'appstore_connect_default'
    conn_type = 'appstore_connect'
    hook_name = 'AppStore Connect'

    def __init__(self, conn_id: str = default_conn_name) -> None:
        self.conn_id = conn_id
        conn = self.get_connection(self.conn_id)
        config = conn.extra_dejson
        if not config.get('key_id', None):
            raise AirflowException('No Key ID provided')
        if not config.get('key_file', None):
            raise AirflowException('No Key File provided')
        if not config.get('issuer_id', None):
            raise AirflowException('No Issuer ID provided')
        self.key_id = config['key_id']
        self.key_file = config['key_file']
        self.issuer_id = config['issuer_id']
        self._api: AppStoreConnectApi = None


    def get_api(self) -> AppStoreConnectApi:
        if not self._api:
            self._api = AppStoreConnectApi(key_id=self.key_id, key_file=self.key_file, issuer_id=self.issuer_id)
        return self._api
