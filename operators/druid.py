import os
import json
import pandas as pd
from typing import Dict, Optional, Callable
from airflow.models import BaseOperator
from airflow.providers.apache.druid.hooks.druid import DruidDbApiHook
from airflow.exceptions import AirflowException
from hooks.postgres import PostgresPandasHook
from hooks.druid_custom import DruidCustomHook
from operators.hdfs import HDFSException, PutHDFSOperator, RmHDFSOperator
from hooks.hdfs import HDFSHook
from utils.common import timing



class DruidIngestOperator(BaseOperator):
    """
    Ingest data into specific Druid database with input payload

    :param payload: the ingest payload to be executed. (templated)
    :type payload: str

    :param payload_generator: the function that return payload
    :type payload_generator: callable

    :param payload_generator_kwargs: a dict of parameters that passed into payload_generator function
    :type payload_generator_kwargs: dict

    :param druid_ingest_conn_id: reference to a specific druid ingest api
    :type druid_ingest_conn_id: str

    :param timeout: period time to check task status
    :type timeout: int

    :param max_ingestion_time: max ingestion time to wait before shutdown task
    :type max_ingestion_time: int
    """

    template_fields = ('payload', 'payload_generator_kwargs',)
    template_ext = ('.sql',)
    ui_color = '#64d97d'

    def __init__(self,
            payload = None,
            payload_generator: callable = None,
            payload_generator_kwargs: dict = {},
            druid_ingest_conn_id = 'druid_ingest_default',
            timeout = 30,
            max_ingestion_time = None,
            wait_to_finish: bool = True,
            *args, **kwargs):
        super(DruidIngestOperator, self).__init__(*args, **kwargs)
        self.payload = payload
        self.payload_generator = payload_generator
        self.payload_generator_kwargs = payload_generator_kwargs
        self.druid_ingest_conn_id = druid_ingest_conn_id
        self.timeout = timeout
        self.max_ingestion_time = max_ingestion_time
        self.wait_to_finish = wait_to_finish

    @timing
    def execute(self, context):
        if self.payload is not None:
            payload = self.payload
        elif self.payload_generator is not None:
            payload = self.payload_generator(**self.payload_generator_kwargs)
        else:
            raise AirflowException("Require payload or payload_generator to execute DruidIngestOperator")

        if not payload:
            self.log.info("Return with no payload to ingest")
            return

        if type(payload) is dict:
            payload = json.dumps(payload, indent=2)

        self.hook = DruidCustomHook(
            druid_ingest_conn_id=self.druid_ingest_conn_id,
            timeout=self.timeout,
            max_ingestion_time=self.max_ingestion_time
        )
        self.hook.submit_indexing_job(payload)


class Druid2PostgresOperator(BaseOperator):
    """
    Executes sql code in a specific Druid database api and transfer it to postgresql

    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'

    :param sql_generator: the function that return sql code
    :type sql_generator: callable

    :param sql_generator_kwargs: a dict of parameters that passed into sql_generator function (templated)
    :type sql_generator_kwargs: dict

    :param druid_query_conn_id: reference to a specific postgres database
    :type druid_query_conn_id: str

    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: str

    :param pg_database: name of postgres database which overwrite defined one in connection
    :type pg_database: str

    :param pg_insert_kwargs: key words arguments that pass into insert method
        (see more: PostgresInsertHook.insert_pandas_2_postgres)
    :type pg_insert_kwargs: dict
    """

    template_fields = ('sql', 'sql_generator_kwargs',)
    template_ext = ('.sql',)
    ui_color = '#00eaff'

    def __init__(
            self,
            sql = None,
            sql_generator: callable = None,
            sql_generator_kwargs: dict = {},
            druid_query_conn_id = "druid_query_default",
            postgres_conn_id='postgres_default',
            pg_database=None,
            pg_insert_kwargs={},
            *args, **kwargs):

        super(Druid2PostgresOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.sql_generator = sql_generator
        self.sql_generator_kwargs = sql_generator_kwargs
        self.druid_query_conn_id = druid_query_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.pg_database = pg_database
        self.pg_insert_kwargs = pg_insert_kwargs

    def get_druid_hook(self):
        """
        Return the druid db api sqoop_hook.
        """
        return DruidDbApiHook(druid_broker_conn_id=self.druid_query_conn_id)

    def get_postgres_hook(self):
        return PostgresPandasHook(
            postgres_conn_id=self.postgres_conn_id,
            schema=self.pg_database
        )

    @timing
    def execute(self, context):
        if self.sql is not None:
            sql = self.sql
        elif self.sql_generator is not None:
            sql = self.sql_generator(**self.sql_generator_kwargs)
        else:
            raise AirflowException("Require sql or sql_generator to execute PostgresExtraOperator")

        engine = self.get_druid_hook().get_sqlalchemy_engine()
        dataframe = pd.read_sql_query(sql, con=engine)
        dataframe.info()

        self.get_postgres_hook().insert_pandas_2_postgres(
            dataframe=dataframe,
            **self.pg_insert_kwargs
        )


class DruidMarkSegmentAsUnusedOperator(BaseOperator):
    """
    Mard unused segments by intervals or segment ids

    :param datasource: name of datasource
    :type datasource: str

    :param since: the start timestamp in UTC of interval to mark segments as unused (ex: 2021-01-01T12:00:00.000Z)
    :type since: str

    :param until: the end timestamp in UTC of interval to mark segments as unused (ex: 2021-01-02T12:00:00.000Z)
    :type until: str

    :param interval_generator: a callable that return tuple (since, until) or list[(since1, until1), (since2, until2)]
        Note that since and until parameters with be ignored when this is not None
    :type interval_generator: callable

    :param interval_generator_kwargs: a dict of parameters that passed into interval_generator
    :type interval_generator_kwargs: dict

    :param segment_ids: list of segment id
    :type segment_ids: list(str)

    :param segment_ids_generator: a callable that return segment_ids
    :type segment_ids_generator: callable

    :param segment_ids_generator_kwargs: a dict of parameters that passed into segment_ids_generator
    :type segment_ids_generator_kwargs: dict

    :param druid_ingest_conn_id: reference to a specific druid ingest api
    :type druid_ingest_conn_id: str
    """

    template_fields = ('datasource', 'since', 'until', 'interval_generator_kwargs', 'segment_ids_generator_kwargs')
    ui_color = '#ff6924'

    def __init__(self,
            datasource,
            since = None,
            until = None,
            interval_generator: callable = None,
            interval_generator_kwargs: dict = {},
            segment_ids = None,
            segment_ids_generator: callable = None,
            segment_ids_generator_kwargs: dict = {},
            druid_ingest_conn_id = 'druid_ingest_default',
            *args, **kwargs):
        super(DruidMarkSegmentAsUnusedOperator, self).__init__(*args, **kwargs)
        self.datasource = datasource
        self.since = since
        self.until = until
        self.interval_generator = interval_generator
        self.interval_generator_kwargs = interval_generator_kwargs
        self.segment_ids = segment_ids
        self.segment_ids_generator = segment_ids_generator
        self.segment_ids_generator_kwargs = segment_ids_generator_kwargs
        self.druid_ingest_conn_id = druid_ingest_conn_id

    @timing
    def execute(self, context):
        hook = DruidCustomHook(
            druid_ingest_conn_id=self.druid_ingest_conn_id,
        )
        if (self.since and self.until) is not None or self.interval_generator:
            intervals = self.interval_generator(**self.interval_generator_kwargs) \
                if self.interval_generator else (self.since, self.until)
            if isinstance(intervals, list):
                for since, until in intervals:
                    hook.mask_unused_by_intervals(self.datasource, since, until)
            elif isinstance(intervals, tuple):
                hook.mask_unused_by_intervals(self.datasource, intervals[0], intervals[1])
            else:
                raise AirflowException("intervals return from interval_generator must be a list of tuple (since, until) or (since, until)")
        else:
            if self.segment_ids is None and self.segment_ids_generator is None:
                raise AirflowException("Require both since and until aren't None or interval_generator isn't None" + \
                    "or segment_ids isn't None or segment_ids_generator isn't None")
            segment_ids = self.segment_ids or self.segment_ids_generator(**self.segment_ids_generator_kwargs)
            hook.mark_unused_by_segment_ids(self.datasource, segment_ids)


class DruidTruncateDatasourceOperator(BaseOperator):
    """
    Mard unused all segments of datasource

    :param datasource: name of datasource
    :type datasource: str

    :param druid_ingest_conn_id: reference to a specific druid ingest api
    :type druid_ingest_conn_id: str
    """

    template_fields = ('datasource', 'druid_ingest_conn_id',)
    ui_color = '#ff6924'

    def __init__(self,
        datasource,
        druid_ingest_conn_id="druid_ingest_default",
        *args, **kwargs):
        super(DruidTruncateDatasourceOperator, self).__init__(*args, **kwargs)
        self.datasource = datasource
        self.druid_ingest_conn_id = druid_ingest_conn_id

    @timing
    def execute(self, context):
        hook = DruidCustomHook(
            druid_ingest_conn_id=self.druid_ingest_conn_id,
        )
        hook.truncate_datasource(self.datasource)


class Local2DruidOperator(BaseOperator):
    """
    This operator using to put local file or folder of data into a HDFS folder

    :param local_source: local absolute path of file or folder 
    :param dest_dir: HDFS destination directory
    :param file_conf: the configuration about storage file on HDFS include number of replication and blocksize (unit: byte)
    :type file_conf: dict (default: {'replication': 2, 'blocksize': 134217728})
    :param file_filter: a callcable that receive a file_name and return False if want to filter out this file
    :type file_conf: Callable (default: None)
    :param hook: Hook class that this operator based on
    :type hook: cls
    :param hdfs_conn_id: the connection ID of HDFS
    :type hdfs_conn_id: str
    :param hdfs_user: the user do this operator 
    :type hdfs_user : str (default: hadoop)
    """

    template_fields = ('local_source', 'hdfs_dir', 'file_conf', 'file_filter', )
    
    def __init__(self,
        local_source: str,
        hdfs_dir: str,
        file_conf: Optional[Dict] = None,
        file_filter: Optional[Callable] = None,
        hdfs_hook = HDFSHook,
        hdfs_conn_id: str = "hdfs_default",
        hdfs_user: str = "hadoop",
        remove_after = False,
        payload = None,
        payload_generator: callable = None,
        payload_generator_kwargs: dict = {},
        druid_ingest_conn_id = 'druid_ingest_default',
        timeout = 30,
        max_ingestion_time = None,
        wait_to_finish: bool = True,
        **kwargs):

        super(Local2DruidOperator, self).__init__(**kwargs)
        self.local_source = local_source
        self.hdfs_dir = hdfs_dir
        self.file_conf = file_conf
        self.file_filter = file_filter
        self.hdfs_hook = hdfs_hook
        self.hdfs_conn_id = hdfs_conn_id
        self.hdfs_user = hdfs_user
        self.remove_after = remove_after
        self.druid_hook = DruidCustomHook(
                druid_ingest_conn_id=druid_ingest_conn_id,
                timeout=timeout,
                max_ingestion_time=max_ingestion_time
            )
        self.wait_to_finish = wait_to_finish
        self.payload = payload
        self.payload_generator = payload_generator
        self.payload_generator_kwargs = payload_generator_kwargs
    
    @timing
    def execute(self, context):
        self.log.info("Local files: {}".format(self.local_source))
        if not self.local_source:
            raise HDFSException('Source must be provided !!!')
        if not os.path.exists(self.local_source):
            raise HDFSException(
                f"Source {self.local_source} isn't existed !!!")
        if not self.hdfs_dir:
            raise HDFSException('HDFS middle folder must be provided !!!')

        if self.payload is not None:
            payload = self.payload
        elif self.payload_generator is not None:
            payload = self.payload_generator(**self.payload_generator_kwargs)
        else:
            raise AirflowException("Require payload or payload_generator to execute DruidIngestOperator")
        if not payload:
            self.log.info("Return with no payload to ingest")
            return

        hdfs_hook = self.hdfs_hook(hdfs_conn_id=self.hdfs_conn_id, hdfs_user=self.hdfs_user)
        self.client = hdfs_hook.get_conn()
        self.file_conf = self.file_conf if self.file_conf is not None else hdfs_hook.get_file_conf()

        PutHDFSOperator._copyObjToDir(self.local_source, self.hdfs_dir, self.client, self.file_conf, self.file_filter)

        if type(payload) is dict:
            payload = json.dumps(payload, indent=2) 

        self.druid_hook.submit_indexing_job(payload)

        if self.remove_after:
            RmHDFSOperator._remove(self.client, self.hdfs_dir)
