from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

try:
    from hdfs3 import HDFileSystem
    hdfs3_loaded = True

except ImportError:
    hdfs3_loaded = False

class HDFSHookException(AirflowException):
    pass


class HDFSHook(BaseHook):
    """
        Interact with HDFS. This class is a wrapper around the hdfs3 library.
        :param hdfs_conn_id: Connection id to fetch connection info
        :type hdfs_conn_id: str
        :param proxy_user: effective user for HDFS operations
        :type proxy_user: str
    """

    def __init__(self, hdfs_conn_id='hdfs_default', hdfs_user='hadoop'):
        if not hdfs3_loaded:
            raise ImportError('This HDFSHook implementation requires hdfs3 and libhdfs3')
        self.hdfs_conn_id = hdfs_conn_id
        self.hdfs_user = hdfs_user
        self.client: HDFileSystem = None
        self.uri = None

    def get_conn(self) -> HDFileSystem:
        """
        Returns a hdfs3.HDFileSystem object.
        """
        if self.client is not None:
            return self.client

        try:
            conn = self.get_connection(self.hdfs_conn_id)
            config_extra = conn.extra_dejson
            if not self.hdfs_user:
                self.hdfs_user = conn.login
        except AirflowException:
            raise("Can't get HDFS connection. Please add HDFS connection on WebUI")

        try:
            self.client = HDFileSystem(
                host=conn.host,
                port=conn.port,
                autoconf=config_extra.get('autoconf', True),
                pars=config_extra.get("pars", None),
                user=self.hdfs_user
            )
            return self.client
        except:
            raise HDFSHookException("conn_id doesn't exist in the repository "
                                    "and autoconfig is not specified")
    
    def get_conn_uri(self) -> str:
        if self.uri is not None:
            return self.uri
        try:
            conn = self.get_connection(self.hdfs_conn_id)
            if not self.hdfs_user:
                self.hdfs_user = conn.login
            uri = f"hdfs://{conn.host}"
            if conn.port:
                uri += f":{conn.port}"
            return uri
        except:
            return None

    def get_file_conf(self):
        conn = self.get_connection(self.hdfs_conn_id)
        config_extra = conn.extra_dejson
        return config_extra.get('file_conf', None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def disconnect(self):
        """Closes fs connection (if applicable)."""
        self.client.disconnect()
