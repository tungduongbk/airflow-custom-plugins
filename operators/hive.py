import time
import shutil
import contextlib
import pandas as pd
from datetime import timedelta
from typing import Callable, Dict, Optional, Union, List
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook, HiveMetastoreHook
from hooks.hdfs import HDFSHook
from operators.postgres import PostgresPandasTransformOperator
from operators.hdfs import PutHDFSOperator, RmHDFSOperator
from utils.os_helper import make_new_folder

class HiveServer2ExecOperator(BaseOperator):
    template_fields = ('hql', 'hql_generator_kwargs')

    def __init__(self,
                hql: Union[str, List[str]] = None,
                hql_generator: Optional[Callable] = None,
                hql_generator_kwargs: Dict = {},
                hiveserver2_conn_id: str = "hive_server2_default",
                hive_schema: Optional[str] = None,
                validator: Optional[str] = None,
                **kwargs):

        super(HiveServer2ExecOperator, self).__init__(**kwargs)
        self.hql = hql
        self.hql_generator = hql_generator
        self.hql_generator_kwargs = hql_generator_kwargs
        self.hiveserver2_conn_id = hiveserver2_conn_id
        self.hive_schema = hive_schema
        self.validator = validator

    def _execute_queries(self, hqls: List[str]):
        hook = HiveServer2Hook(hiveserver2_conn_id=self.hiveserver2_conn_id)
        with contextlib.closing(hook.get_conn(self.hive_schema)) as conn, contextlib.closing(conn.cursor()) as cur:
            for hql in hqls:
                # self.log.info(f"Executing HQL: {hql}")
                ret = cur.execute(hql)
                self.log.info(ret)

    def _validate_hqls(hql: Union[str, List[str]]):
        if type(hql) is list: return hql
        else: return [hql]

    def execute(self, context):
        if self.hql is not None:
            hqls = self._validate_hqls(self.hql)
        elif self.hql_generator:
            hqls = self._validate_hqls(self.hql_generator(**self.hql_generator_kwargs))
        else:
            raise AirflowException("Require hql or hql_generator to execute HiveServer2ExecOperator")
        if self.validator:
            self.log.info("Use validator hql")
            hqls.append(self.validator)
        self._execute_queries(hqls)


class Postgres2HiveOperator(PostgresPandasTransformOperator, HiveServer2ExecOperator):
    template_fields = ('hive_table', 'hive_partitions', 'hive_partitions_generator_kwargs', 'local_temporary_dir', 'hdfs_temporary_dir', \
                        'sql', 'sql_generator_kwargs', 'pd_transformer_kwargs', 'storage_config', 'file_name_prefix', 'local_destination',
                        'hql', 'hql_generator_kwargs')
    ui_color = '#3da3da'

    """
    Migrate data from postgres to hive through hiveserver2

    :param hive_table: the destination table on hive
    :type hive_table: str

    :param hive_overwrite: weather overwrite or not existed data on hive
    :type hive_overwrite: bool

    :param hive_partitions: hive specific partition using when insert into table, it'type may be List or Dict or None
        Ex: if hive_partitions = {'date': '2022-01-01'}, partitioned statement will be "PARTITION(date='2022-01-01')"
        else if hive_partitions = ['date'], the column 'date' must be contained in selected sql from postgres and partitioned statement
        will be "PARTITION(date)"
    :type hive_partitions: Union[Dict, List]

    :param hive_partitions_generator: the callable that return hive_partitions
    :type hive_partitions_generator: callable

    :param hive_partitions_generator_kwargs: the dict contained parameters that will pass into hive_partitions_generator
    :type hive_partitions_generator_kwargs: dict

    :param local_temporary_dir: local temporary directory to store intermediary data from postgres
    :type local_temporary_dir: str
    
    :param hdfs_temporary_dir: hdfs temporary directory to store intermediary data before loading to hive table
    :type hdfs_temporary_dir: str
    """

    def __init__(self,
                hive_table: str,
                hive_overwrite: bool = False,
                hive_partitions: Union[Dict, List] = None,
                hive_partitions_generator: Optional[Callable] = None,
                hive_partitions_generator_kwargs: Dict = {},
                local_temporary_dir: Optional[str] = None,
                hdfs_temporary_dir: Optional[str] = None,
                metastore_conn_id: str = "hive_metastore",
                hdfs_conn_id: str = "hdfs_default",
                hdfs_user: str = "hive",
                 **kwargs):

        super(Postgres2HiveOperator, self).__init__(**kwargs)

        self.hivehive_overwrite = hive_overwrite
        self.hive_table = hive_table
        self.hive_partitions = hive_partitions
        self.hive_partitions_generator = hive_partitions_generator
        self.hive_partitions_generator_kwargs = hive_partitions_generator_kwargs
        self.local_temporary_dir = local_temporary_dir
        self.hdfs_temporary_dir = hdfs_temporary_dir
        self.metastore_conn_id = metastore_conn_id
        self.hdfs_conn_id = hdfs_conn_id
        self.hdfs_user = hdfs_user
        self.is_partition_explicit = True

    def _get_table_description(self):
        hms_hook = HiveMetastoreHook(metastore_conn_id=self.metastore_conn_id)
        return hms_hook.get_table(self.hive_table, self.hive_schema)

    def _normalize_pandas(self, df: pd.DataFrame):
        t = self._get_table_description()
        cols = t.sd.cols if self.is_partition_explicit else t.sd.cols + t.partitionKeys
        for col in cols:
            if col.type == "tinyint":
                df[col.name] = df[col.name].astype('Int8')
            elif col.type == "smallint":
                df[col.name] = df[col.name].astype('Int16')
            elif col.type == "int":
                df[col.name] = df[col.name].astype('Int32')
            elif col.type == "bigint":
                df[col.name] = df[col.name].astype('Int64')
            elif col.type == "float":
                df[col.name] = df[col.name].astype('float32')
            elif col.type == "double":
                df[col.name] = df[col.name].astype('float64')
            elif col.type == "timestamp":
                df[col.name] = pd.to_datetime(df[col.name])
            elif col.type == "date":
                df[col.name] = df[col.name].astype('str')
            elif col.type == "boolean":
                pass
            else:
                df[col.name] = df[col.name].astype('str')
        return df


    def _generate_create_hive_temporay_table(self):
        t = self._get_table_description()
        cols = t.sd.cols if self.is_partition_explicit else t.sd.cols + t.partitionKeys
        normalized_cols = list(map(lambda c: (c.name, 'string') if c.type == "date" else (c.name, c.type), cols))
        defined_cols = ",".join([f"`{col[0]}` {col[1]}" for col in normalized_cols])
        return [
            f"DROP TABLE IF EXISTS {self.hive_temporary_table}",
            f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {self.hive_schema}.{self.hive_temporary_table} ({defined_cols})
                COMMENT 'temporary for transfer data from postgres to hive'
                STORED AS PARQUET
                LOCATION '{self.hdfs_temporary_dir}'
                TBLPROPERTIES ('external.table.purge'='true')
            """,
        ]

    def _generate_insert_data_from_temporary(self):
        def _resolve_partition(kv):
            if type(kv[1]) is str: return f"{kv[0]}='{kv[1]}'"
            else: return f"{kv[0]}={kv[1]}"
        
        partition_clause = ""
        if self.hive_partitions:
            if self.is_partition_explicit:
                partition_cols = ", ".join(list(map(lambda kv: _resolve_partition(kv), self.hive_partitions.items())))
            else:
                partition_cols = ", ".join(self.hive_partitions)
            partition_clause = f"PARTITION({partition_cols})"

        overwrite_clause = "OVERWRITE" if self.hivehive_overwrite else "INTO"

        return [
            "SET hive.execution.engine = mr",
            f"""
                INSERT {overwrite_clause} TABLE {self.hive_table}
                {partition_clause}
                SELECT * FROM {self.hive_temporary_table}
            """,
        ]
    
    def _generate_drop_hive_temporary_table(self):
        return [f"""
            DROP TABLE {self.hive_temporary_table}
        """]

    def _preprocess_partition(self):
        if self.hive_partitions_generator:
            self.hive_partitions = self.hive_partitions_generator(**self.hive_partitions_generator_kwargs)
        if self.hive_partitions:
            if type(self.hive_partitions) is dict:
                self.is_partition_explicit = True
            elif type(self.hive_partitions) is list:
                self.is_partition_explicit = False 
            else:
                raise AirflowException("Type of hive_partitions must be List or Dict")

    def execute(self, context):
        execution_date = (context['dag_run'].execution_date + timedelta(hours=7)).strftime('%Y%m%d')
        self.local_temporary_dir = self.local_temporary_dir or f'/tmp/airflow/{self.dag_id}/{self.task_id}/{execution_date}'
        self.hdfs_temporary_dir = self.hdfs_temporary_dir or f'/tmp/airflow/{self.dag_id}/{self.task_id}/{execution_date}'
        self.hive_temporary_table = self.hive_table + "_" + execution_date

        start_time = time.time()
        df = self._pull_postgres_to_pandas()
        if self.column_map: df.rename(columns=self.column_map, inplace=True)
        df = self._transform_pandas(df)
        df = self._normalize_pandas(df)
        make_new_folder(self.local_temporary_dir)
        df.to_parquet(f"{self.local_temporary_dir}/{self.hive_table}.parquet", index=False,  engine="pyarrow", compression=None, allow_truncated_timestamps=True, use_deprecated_int96_timestamps=True)
        self.log.info("STEP 1: took {}s to pull and transform data from postgres".format(time.time() - start_time))
        
        start_time = time.time()
        hook = HDFSHook(hdfs_conn_id=self.hdfs_conn_id, hdfs_user=self.hdfs_user)
        client = hook.get_conn()
        file_conf = hook.get_file_conf()
        PutHDFSOperator._copyObjToDir(self.local_temporary_dir, self.hdfs_temporary_dir, client, file_conf, file_filter=None)
        self.log.info("STEP 2: took {}s to push data to hdfs".format(time.time() - start_time))
        
        start_time = time.time()
        hqls = []
        self._preprocess_partition()
        hqls.extend(self._generate_create_hive_temporay_table())
        hqls.extend(self._generate_insert_data_from_temporary())
        hqls.extend(self._generate_drop_hive_temporary_table())
        self._execute_queries(hqls)
        self.log.info("STEP 3: took {}s to load data from hdfs to hive".format(time.time() - start_time))

        shutil.rmtree(self.local_temporary_dir)
        self.log.info(f"STEP 4: clean local temporary dir: {self.local_temporary_dir}")

        RmHDFSOperator._remove(client, self.hdfs_temporary_dir)
        self.log.info(f"STEP 5: clean hdfs temporary dir: {self.hdfs_temporary_dir}")
