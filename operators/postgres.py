
import os
import time
import pandas as pd
from typing import Callable, Dict, Optional
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from hooks.postgres import PostgresPandasHook
from utils.os_helper import make_new_folder


class PostgresExecOperator(BaseOperator):
    """
    Executes sql code in a specific Postgres database

    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'

    :param sql_generator: the function that return sql code
    :type sql_generator: callable

    :param sql_generator_kwargs: a dict of parameters that passed into sql_generator function (templated)
    :type sql_generator_kwargs: dict

    :param hook_cls: reference to a name of sqoop_hook class
    :type hook_cls: cls

    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: str

    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool

    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: mapping or iterable
    
    :param schema: schema's name which overwrite one defined in connection
    :type schema: str
    """

    template_fields = ('sql', 'sql_generator_kwargs',)
    template_ext = ('.sql',)
    ui_color = '#3da3da'

    def __init__(
            self,
            sql = None,
            sql_generator: callable = None,
            sql_generator_kwargs: dict = {},
            hook_cls = PostgresHook, 
            postgres_conn_id='postgres_default',
            autocommit = False,
            parameters = None,
            schema = None,
            *args, **kwargs):
        super(PostgresExecOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.sql_generator = sql_generator
        self.sql_generator_kwargs = sql_generator_kwargs
        self.hook_cls = hook_cls
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.schema = schema

    def execute(self, context):
        if self.sql is not None:
            sql = self.sql
        elif self.sql_generator is not None:
            sql = self.sql_generator(**self.sql_generator_kwargs)
        else:
            raise AirflowException("Require sql or sql_generator to execute PostgresExtraOperator")

        self.hook = self.hook_cls(
            postgres_conn_id=self.postgres_conn_id,
            schema=self.schema
        )
        start_time = time.time()
        self.hook.run(sql, self.autocommit, parameters=self.parameters)
        for output in self.hook.conn.notices:
            self.log.info(output)
        self.log.info(f"Took {time.time() - start_time} s to execute query")


class PostgresPandasTransformOperator(BaseOperator):
    """
    Executes sql code from a specific Postgres database, load to pandas Dataframe and transform, and save to local file

    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'

    :param sql_generator: the function that return sql code
    :type sql_generator: callable

    :param sql_generator_kwargs: a dict of parameters that passed into sql_generator function (templated)
    :type sql_generator_kwargs: dict

    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: str

    :param schema: schema's name which will overwrite one defined in connection
    :type schema: str

    :param table: postgres's table name for selecting all from
    :type table: str. Default: None

    :param pd_transformer:  the function take a dataframe, transform and return a dataframe or list of dataframe
        this function must take dataframe argument named 'dataframe' to pass a dataframe into function to transform
    :type pd_transformer: callable

    :param pd_transformer_kwargs: a dict of parameters that passed into pd_transformer function (templated)
    :type pd_transformer_kwargs: dict

    :param storage_type: type of file. Currently only support 'csv' or 'parquet' (default: 'parquet')
    :type storage_type: str

    :param storage_config: key value pair config for storing file (templated)
        that will pass into pandas.to_csv or pandas.to_parquet function as kwargs
    :type storage_config: dict

    :param file_name_prefix: file name prefix when save file (default: 'file')
    :type file_name_prefix: str

    :param local_destination: directory to save file (default: '/tmp')
    :type local_destination: str
    """

    template_fields = ('sql', 'sql_generator_kwargs', 'pd_transformer_kwargs', 'storage_config', 'file_name_prefix', 'local_destination',)
    template_ext = ('.sql',)
    ui_color = '#c27878'

    def __init__(
            self,
            sql: Optional[str] = None,
            sql_generator: Optional[Callable] = None,
            sql_generator_kwargs: Dict = {},
            postgres_conn_id: str = "postgres_default",
            schema: Optional[str] = None,
            table: str = None,
            pd_transformer: Optional[Callable] = None,
            pd_transformer_kwargs: Dict = {},
            column_map: Dict = {},
            storage_type: str = "parquet",
            storage_config: Dict = {},
            file_name_prefix: str = "file",
            local_destination: str = "/tmp/airflow",
            *args, **kwargs):
        super(PostgresPandasTransformOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.sql_generator = sql_generator
        self.sql_generator_kwargs = sql_generator_kwargs
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.table = table
        self.pd_transformer = pd_transformer
        self.pd_transformer_kwargs = pd_transformer_kwargs
        self.column_map = column_map
        self.storage_type = storage_type
        self.storage_config = storage_config
        self.local_destination = local_destination
        self.file_name_prefix = file_name_prefix

    def _pull_postgres_to_pandas(self):
        if self.table is not None:
            sql = f"SELECT * FROM {self.schema}.{self.table}"
        elif self.sql is not None:
            sql = self.sql
        elif self.sql_generator is not None:
            sql = self.sql_generator(**self.sql_generator_kwargs)
        else:
            raise AirflowException("Require table, sql or sql_generator to execute PostgresTransformOperator")
        start_time = time.time()
        hook = PostgresPandasHook(
            postgres_conn_id=self.postgres_conn_id,
            # schema=self.schema
        )
        self.log.info(f"SQL: {sql}")
        df = hook.query_from_postgres(sql)
        self.log.info(f"Took {time.time() - start_time}s to pull SQL")
        return df

    def _transform_pandas(self, df: pd.DataFrame):
        start_time = time.time()
        if not self.pd_transformer:
            return df
        transformer_kwargs = self.pd_transformer_kwargs.copy()
        transformer_kwargs["dataframe"] = df
        transformed_df = self.pd_transformer(**transformer_kwargs)
        self.log.info(f"Took {time.time() - start_time} s to transform pandas dataframe")
        return transformed_df

    def _save_dataframe(self, df: pd.DataFrame, surfix=None):
        if self.storage_type not in ["csv", "parquet"]:
            raise AirflowException(f"Storage type: {self.storage_type} currently not supported !!!" + \
                "Please use storage_type = 'csv' or 'parquet'")
        else:
            location = os.path.join(self.local_destination, self.file_name_prefix)
            if surfix is not None: location = location + "_" + str(surfix)
            file_name = f"{location}.{self.storage_type}"
            if self.storage_type == "csv":
                df.to_csv(file_name, **self.storage_config)
            else:
                df.to_parquet(file_name, **self.storage_config)
            self.log.info(f"Saved file: {file_name}")

    def execute(self, context):
        df = self._pull_postgres_to_pandas()
        result = self._transform_pandas(df)
        if self.column_map:
            df.rename(columns=self.column_map, inplace=True)

        make_new_folder(self.local_destination)
        if isinstance(result, pd.DataFrame):
            self._save_dataframe(result)
        elif isinstance(result, list):
            for i, _df in enumerate(result):
                self._save_dataframe(_df, surfix=i)
        else:
            raise AirflowException("Transformer must return a dataframe or list of dataframe")
