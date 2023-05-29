import pandas.io.sql as psql
import pendulum
from airflow.models.baseoperator import BaseOperator

from hooks.postgres_custom import PostgresCustomHook

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')


class PostgresTransformOperator(BaseOperator):
    template_fields = ('run_date', 'delete_sql',)
    ui_color = '#33CBFE'

    def __init__(
            self,
            replace: bool,
            transferred_table: str,
            schema: str = '',
            conflict_sql: str = '',
            sql_builder: callable = None,
            postgres_conn_id='postgres_default',
            database=None,
            run_date: any = None,
            commit_every: int = 10000,
            delete_sql=None,
            *args, **kwargs):

        super(PostgresTransformOperator, self).__init__(*args, **kwargs)
        self.sql_builder = sql_builder
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.transferred_table = transferred_table
        self.schema = schema
        self.run_date = run_date
        self.replace = replace
        self.commit_every = commit_every
        self.conflict_sql = conflict_sql
        self.delete_sql = delete_sql
        self.hook = PostgresCustomHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)

    def execute(self, context):
        target_fields, records = self.get_records()
        self.log.info("Executing on %s", self.run_date)
        if self.delete_sql is not None:
            self.do_delete()
        self.insert_rows(target_fields=target_fields, records=records)
        for output in self.hook.conn.notices:
            self.log.info(output)

    def get_records(self):
        if self.run_date is None:
            raise ValueError("Missing run_date argument!!")
        sql = self.sql_builder(self.run_date)
        print(f"Source SQL: {sql}")
        self.log.info('Executing: %s', sql)
        df_records = psql.read_sql(sql, con=self.hook.get_conn())
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

    def insert_rows(self, target_fields, records):
        self.hook.insert_rows(table=self.transferred_table,
                                rows=records,
                                target_fields=target_fields,
                                replace=self.replace,
                                conflict_sql=self.conflict_sql,
                                commit_every=self.commit_every)

    def do_delete(self):
        self.hook.run(self.delete_sql, autocommit=True)
        self.log.info("Executing delete statement:\n %s", self.delete_sql)
