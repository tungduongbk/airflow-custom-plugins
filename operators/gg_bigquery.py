import os
from datetime import timedelta
from google.cloud import bigquery
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from hooks.postgres_custom import PostgresCustomHook
from operators.pg_transform import PostgresTransformOperator


class BigQueryPandasToPostgresOperator(PostgresTransformOperator):
    ui_color = '#11BCFE'

    def __init__(
            self,
            bigquery_conn_id='bigquery_default',
            *args, **kwargs):
        super(BigQueryPandasToPostgresOperator, self).__init__(*args, **kwargs)
        self.bigquery_conn_id = bigquery_conn_id

    def get_records(self):
        self.bq_hook = BigQueryHook(gcp_conn_id=self.bigquery_conn_id, use_legacy_sql=False)
        bq_client = bigquery.Client(project=self.bq_hook._get_field("project"),
                                    credentials=self.bq_hook._get_credentials())
        if self.run_date is None:
            raise ValueError("Missing run_date argument!!")
        sql = self.sql_builder(self.run_date)
        self.log.info('Executing: %s', sql)
        df_records = bq_client.query(sql).to_dataframe()
        target_fields = df_records.columns.tolist()
        self.log.info("Target Fields: {}".format(target_fields))
        records = list(df_records.itertuples(index=False, name=None))
        self.log.info("Get first {}".format(records[0]))
        self.log.info("Start loading %s records to %s ..." % (len(records), self.transferred_table))
        if len(records) < 1:
            raise ValueError(
                "Data Quality validation FAILED on {} in table {}.{}.".format(self.run_date,
                                                                                self.schema,
                                                                                self.transferred_table))
        return target_fields, records


class BigQueryPandas2FileOperator(BaseOperator):
    template_fields = ('run_date',)
    ui_color = '#21BFFE'

    def __init__(self, bigquery_conn_id,
                store_dir,
                run_date,
                sql_builder,
                file_format="csv",
                 *args, **kwargs):
        super(BigQueryPandas2FileOperator, self).__init__(*args, **kwargs)
        self.store_dir = store_dir
        self.bigquery_conn_id = bigquery_conn_id
        self.run_date = run_date
        self.sql_builder = sql_builder
        self.file_format = file_format

    def get_records(self):
        self.bq_hook = BigQueryHook(gcp_conn_id=self.bigquery_conn_id, use_legacy_sql=False)
        bq_client = bigquery.Client(project=self.bq_hook._get_field("project"),
                                    credentials=self.bq_hook._get_credentials())
        if self.run_date is None:
            raise ValueError("Missing run_date argument!!")
        sql = self.sql_builder(self.run_date)
        self.log.info('Executing: %s', sql)
        df_records = bq_client.query(sql).to_dataframe()
        self.log.info("df with shape: {}".format(df_records.shape))
        target_fields = df_records.columns.tolist()
        self.log.info("Target Fields: {}".format(target_fields))
        if df_records.shape[0] < 1:
            raise ValueError("Data Quality validation FAILED!!")
        return target_fields, df_records

    def execute(self, context):
        target_fields, df_records = self.get_records()
        self.log.info("Executing on %s", self.run_date)
        if not os.path.exists(self.store_dir):
            os.makedirs(self.store_dir)
        save_path = f"{self.store_dir}/{self.run_date}.{self.file_format}"
        self.log.info(" *** Saved file to local path: {}".format(save_path))
        if self.file_format == "csv":
            df_records.to_csv(save_path, index=False)
        elif self.file_format == "parquet":
            df_records.to_parquet(save_path, index=False, engine='pyarrow')
        else:
            raise ValueError("Please specify file format either csv or parquet")


class BigqueryTransformPostgreOperator(BaseOperator):
    """
    author: phuong.nguyen.huu@vn
    """
    
    template_fields = ('run_date',)
    ui_color = '#33CBFE'

    def __init__(
            self,
            target_fields: list,
            transferred_table: str,
            replace: bool,
            is_update: bool,
            # schema: str = '',
            # self.validator = validator,
            database: str = None,
            run_date: str = None,
            sql_builder: callable = None,
            postgres_conn_id: str = 'postgres_default',
            bigquery_conn_id: str = 'bigquery_default',
            use_legacy_sql: bool = False,
            commit_every: int = 5000,
            replace_index: list = None,
            sum_index: list = [],
            coalesces=None,
            *args, **kwargs):

        super(BigqueryTransformPostgreOperator, self).__init__(*args, **kwargs)
        self.target_fields = target_fields
        self.sql_builder = sql_builder
        self.transferred_table = transferred_table
        self.replace = replace
        self.replace_index = replace_index
        self.is_update = is_update
        self.sum_index = sum_index
        self.commit_every = commit_every
        self.database = database
        self.run_date = run_date
        self.coalesces = coalesces
        self.postgres_conn_id = postgres_conn_id
        self.bigquery_conn_id = bigquery_conn_id
        self.use_legacy_sql = use_legacy_sql
        self.postgres_hook = PostgresCustomHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        self.bigquery_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id, use_legacy_sql=self.use_legacy_sql)

    def execute(self, context):
        if context['dag_run'].conf is not None:
            if context['dag_run'].conf.get('run_date') is not None:
                self.run_date = context['dag_run'].conf.get('run_date')
                self.log.info("**** trigger by externally dags with run_date: {}".format(self.run_date))
        if self.run_date is None or self.run_date == 'None':
            self.run_date = (context['dag_run'].execution_date + timedelta(hours=7)).strftime('%Y-%m-%d')
        self.log.info('**** Run query with execution date: {}'.format(self.run_date))
        self.log.info('**** Executing {}'.format(self.sql_builder))
        sql = self.sql_builder(self.run_date)
        self.log.info('**** Query command: {}'.format(sql))
        df_records = self.bigquery_hook.get_pandas_df(sql)
        records = list(df_records.itertuples(index=False, name=None))
        if len(records) > 0:
            self.log.info("Get first {}".format(records[0]))
            self.log.info("Start loading {} records to {} ...".format(len(records), self.transferred_table))
            self.insert_data_to_postgres(records)
        else:
            self.log.warn("Zero records insert to table {}".format(self.transferred_table))

    def insert_data_to_postgres(self, records):
        self.hook.insert_rows(table=self.transferred_table,
                                rows=records,
                                target_fields=self.target_fields,
                                replace=self.replace,
                                replace_index=self.replace_index,
                                is_update=self.is_update,
                                sum_index=self.sum_index,
                                commit_every=self.commit_every,
                                coalesces=self.coalesces)
