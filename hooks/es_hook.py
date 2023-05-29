import json
from ssl import CERT_NONE

from airflow.hooks.base import BaseHook
from elasticsearch import Elasticsearch, exceptions


class ElasticSearchHook(BaseHook):

    def __init__(self, conn_id='es_dev_conn', *args, **kwargs):
        super(ElasticSearchHook, self).__init__()
        self.es_conn_id = conn_id
        self._connection = self.get_connection(conn_id)
        self.client = None
        self.extras = self._connection.extra_dejson.copy()

        self._uri = '{content_type}://{creds}{host}{port}'.format(
            content_type=self._connection.conn_type,
            creds='{}:{}@'.format(
                self._connection.login, self._connection.password
            ) if self._connection.login else '',

            host=self._connection.host,
            port='' if self._connection.port is None else ':{}'.format(self._connection.port),
        )

    def check_valid_client(self) -> None:
        try:
            # get information on client
            client_info = Elasticsearch.info(self.client)
            self.log.info('Elasticsearch client info:\n {} '.format(json.dumps(client_info, indent=4)))
        except exceptions.ConnectionError as err:
            self.log.debug('Elasticsearch client error:', err)

    def get_conn(self) -> Elasticsearch:
        """
        Fetches ElasticSearch Client
        """
        if self.client is not None:
            return self.client

        # Elastic search Connection Options dict that is unpacked when passed to Elasticsearch client
        options = self.extras

        # If we are using SSL disable requiring certs from specific hostname
        if options.get('ssl', False):
            options.update({'ssl_cert_reqs': CERT_NONE})
        self.client = Elasticsearch(self._uri, **options)
        # self.check_valid_client()
        return self.client

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client is not None:
            self.close_conn()

    def close_conn(self):
        client = self.client
        if client is not None:
            client.transport.close()
            self.client = None
