import os

import pandas.io.sql as psql

from operators.pg_transform import PostgresTransformOperator


class PostgresToParquetOperator(PostgresTransformOperator):
    template_fields = ('path_dir', 'file_name')
    ui_color = '#33CBCE'

    def __init__(self,
                path_dir: str,
                file_name: str,
                compress: str = "snappy",
                *args, **kwargs):
        super(PostgresToParquetOperator, self).__init__(*args, **kwargs)
        self.path_dir = path_dir
        self.file_name = file_name
        self.compress = compress

    def execute(self, context):
        if not os.path.exists(self.path_dir):
            os.makedirs(self.path_dir)
        full_path = "{}/{}.parquet".format(self.path_dir,self.file_name)
        target_fields, df_records = self.get_records()
        df_records.to_parquet(full_path, index=False, engine='pyarrow', compression=self.compress)
        self.log.info("Store data in tmp file: {}".format(full_path))

    def get_records(self):
        if self.run_date is None:
            raise ValueError("Missing run_date argument!!")
        sql = self.sql_builder(self.run_date)
        self.log.info('Executing: %s', sql)
        df_records = psql.read_sql(sql, con=self.hook.get_conn())
        target_fields = df_records.columns.tolist()
        self.log.info("Target Fields: ", target_fields)
        self.log.info("Get number of records {}".format(df_records.shape[0]))
        if df_records.shape[0] < 1:
            raise ValueError(
                "Data Quality validation FAILED, get ZERO records!!")
        return target_fields, df_records
