import psycopg2
import pandas as pd
import numpy as np
from contextlib import closing
from airflow.providers.postgres.hooks.postgres import PostgresHook

def nan_to_null(f,
                _NULL=psycopg2.extensions.AsIs('NULL'),
                _Float=psycopg2.extensions.Float):
    if not np.isnan(f):
        return _Float(f)
    return _NULL

class PostgresPandasHook(PostgresHook):
    """
    Add Insert a dataframe to postgres method
    """
    def __init__(self, *args, **kwargs):
        super(PostgresPandasHook, self).__init__(*args, **kwargs)

    def insert_pandas_2_postgres(self,
        dataframe,
        table,
        conflict_columns = [],
        on_conflict_update_columns = [],
        schema="public"):

        psycopg2.extensions.register_adapter(float, nan_to_null)

        values = [tuple(x) for x in dataframe.to_numpy()]
        columns = ','.join(list(dataframe.columns))
        on_conflict = "" if not conflict_columns else "ON CONFLICT (%s)" % (','.join(conflict_columns))
        conflict_do = " DO NOTHING;" if not on_conflict_update_columns else "DO UPDATE SET %s;" \
            % (','.join(list(map(lambda x: f"{x} = EXCLUDED.{x}", on_conflict_update_columns))))
        
        conflict_resolve = on_conflict if not on_conflict else on_conflict + conflict_do
        query = """
            INSERT INTO %s.%s as f (%s)
            VALUES %%s
            %s
            """ % (schema, table, columns, conflict_resolve)

        psycopg2.extensions.register_adapter(float, nan_to_null)
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            psycopg2.extras.execute_values(cursor, query, values)
            conn.commit()
            self.log.info(f"Upsert data to {table} successful!!!")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

    def query_from_postgres(self, sql: str) -> pd.DataFrame:
        engine = self.get_sqlalchemy_engine()
        dataframe = pd.read_sql_query(sql, con=engine)
        return dataframe
