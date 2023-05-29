from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class DataQualityOperator(BaseOperator):
    template_fields = ('run_date',)
    ui_color = '#89DA59'

    def __init__(self,
                sql_builder: callable,
                schema='public',
                conn_id="",
                tables=[],
                run_date: str = '',
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.tables = tables
        self.run_date = run_date
        self.sql_builder = sql_builder
        self.schema = schema

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)

        for table in self.tables:

            self.log.info("Starting data quality validation on table : {}".format(table))
            sql = self.sql_builder(self.run_date, self.schema, table)
            records = redshift_hook.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(
                    "Data Quality validation failed on {} in table {}.{}.".format(self.run_date, self.schema, table))
                raise ValueError(
                    "Data Quality validation failed on {} in table {}.{}.".format(self.run_date, self.schema, table))
            self.log.info(
                "Data Quality validation failed on {} in table {}.{}.".format(self.run_date, self.schema, table))
