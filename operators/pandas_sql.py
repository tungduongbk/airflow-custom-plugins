import pendulum

from operators.pg_transform import PostgresTransformOperator

local_tz = pendulum.timezone('Asia/Ho_Chi_Minh')


class PandasSqlOperator(PostgresTransformOperator):
    ui_color = '#12CCFE'

    def __init__(
            self,
            df_transform_function: callable,
            op_kwargs: dict = {},
            *args, **kwargs):
        super(PandasSqlOperator, self).__init__(*args, **kwargs)
        self.df_transform_function = df_transform_function
        self.op_kwargs = op_kwargs

    def get_records(self):
        if self.run_date is None:
            raise ValueError("Missing run_date argument!!")
        self.log.info("**** Run query with execution_date: {}".format(self.run_date))
        conn = self.hook.get_conn()
        df_records = self.df_transform_function(run_date=self.run_date, conn=conn, **self.op_kwargs)
        target_fields = df_records.columns.tolist()
        self.log.info("Target Fields: {}".format(target_fields))
        records = list(df_records.itertuples(index=False, name=None))
        del df_records
        if len(records) < 1:
            raise ValueError(
                "Data Quality validation FAILED on {} in table {}.{}.".format(self.run_date,
                                                                            self.schema,
                                                                            self.transferred_table))
        else:
            self.log.info("Get first {}".format(records[0]))
            self.log.info("Start loading %s records to %s ..." % (len(records), self.transferred_table))
                    
        return target_fields, records
