import json
import requests
from furl import furl
from airflow.providers.apache.druid.hooks.druid import DruidHook
from airflow.exceptions import AirflowException


class DruidCustomHook(DruidHook):
    """
    Connection to Druid overlord (or Coordinator) for ingestion

    To connect to a Druid cluster that is secured with the druid-basic-security
    extension, add the username and password to the druid ingestion connection.

    :param druid_ingest_conn_id: The connection id to the Druid overlord machine which accepts index jobs
    :type druid_ingest_conn_id: str

    :param timeout: The interval between polling the Druid job for the status of the ingestion job. Must be greater than or equal to 1
    :type timeout: int

    :param max_ingestion_time: The maximum ingestion time before assuming the job failed
    :type max_ingestion_time: int

    :param endpoint: The endpoint that overwrite endpoint in connection
    :type endpoint: int
    """

    def __init__(
            self,
            endpoint=None,
            **kwargs):
        super(DruidCustomHook, self).__init__(**kwargs)
        self.endpoint = endpoint

    def _get_conn_url(self, endpoint=None):
        conn = self.get_connection(self.druid_ingest_conn_id)
        host = conn.host
        if conn.port is not None:
            host += ':{port}'.format(port=conn.port)
        conn_type = 'http' if not conn.conn_type else conn.conn_type
        _endpoint = endpoint if endpoint is not None else conn.extra_dejson.get('endpoint', 'druid/indexer/v1/task')
        return furl(f"{conn_type}://{host}").add(path=_endpoint).url

    def get_base_url(self):
        return self._get_conn_url('')

    def get_conn_url(self):
        return self._get_conn_url(self.endpoint)

    def _mark_unused(self, payload, datasource):
        self.log.info(payload)
        url = furl(self.get_base_url()).add(path=f"/druid/coordinator/v1/datasources/{datasource}/markUnused").url
        req_index = requests.post(url, data=payload, headers=self.header, auth=self.get_auth())
        if req_index.status_code != 200:
            raise AirflowException('Did not get 200 when '
                                    'submitting the Druid job to {}'.format(url))
        self.log.info("Mark as unused return: {}".format(req_index.json()))

    def mask_unused_by_intervals(self, datasource, since, until):
        payload = json.dumps({
            "interval": f"{since}/{until}"
        }, indent=2)
        self._mark_unused(payload, datasource)

    def mark_unused_by_segment_ids(self, datasource, ids):
        payload = json.dumps({
            "segmentIds": ids
        }, indent=2)
        self._mark_unused(payload, datasource)

    def get_all_segments(self, datasource):
        self.log.info(f"Getting all segments of datasource: {datasource}")
        url = furl(self.get_base_url()).add(path=f"/druid/coordinator/v1/metadata/datasources/{datasource}/segments").url
        req_index = requests.get(url, headers=self.header, auth=self.get_auth())
        if req_index.status_code != 200:
            raise AirflowException('Did not get 200 when '
                                    'submitting the Druid job to {}'.format(url))
        self.log.info(req_index.json())
        return req_index.json()

    def truncate_datasource(self, datasource):
        self.log.info(f"Deleting datasource: {datasource}")
        url = furl(self.get_base_url()).add(path=f"/druid/coordinator/v1/datasources/{datasource}").url
        req_index = requests.delete(url, headers=self.header, auth=self.get_auth())
        if req_index.status_code != 200:
            raise AirflowException('Did not get 200 when '
                                    'submitting the Druid job to {}'.format(url))
        self.log.info(req_index.json())
        return req_index.json()
