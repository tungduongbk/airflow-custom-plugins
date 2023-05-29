import os
import pandas as pd
import pandas.io.sql as psql
from operators.pandas_sql import PandasSqlOperator


class PandasToReportOperator(PandasSqlOperator):
    # template_fields = ('dir_path', 'excel_name', 'run_date', 'delete_sql', )

    def __init__(self,
                dir_path: str,
                excel_name: str,
                # graph_name: str,
                *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dir_path = dir_path
        self.excel_name = excel_name
        # self.graph_name = graph_name

    def execute(self, context):
        if not os.path.exists(self.dir_path):
            os.makedirs(self.dir_path)
        full_path = "{}/{}.xlsx".format(self.dir_path, self.excel_name)
        target_fields, records, df_records = self.get_records()
        self.log.info("Executing on %s", self.run_date)
        if self.delete_sql is not None:
            self.do_delete()
        self.insert_rows(target_fields=target_fields, records=records)
        for output in self.hook.conn.notices:
            self.log.info(output)
        writer = pd.ExcelWriter(full_path, engine='xlsxwriter')
        df_records.to_excel(writer, sheet_name='Sheet1', columns=target_fields, index=False)
        self.log.info("Store data in tmp file: {}".format(full_path))

    def get_records(self):
        if self.run_date is None:
            raise ValueError("Missing run_date argument!!")
        self.log.info("**** Run query with execution_date: {}".format(self.run_date))
        conn = self.hook.get_conn()
        df_records = self.df_transform_function(run_date=self.run_date, conn=conn, **self.op_kwargs)
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
        return target_fields, records, df_records
