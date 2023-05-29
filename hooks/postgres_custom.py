import psycopg2.extensions
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook
import numpy as np


def nan_to_null(f,
                _NULL=psycopg2.extensions.AsIs('NULL'),
                _Float=psycopg2.extensions.Float):
    if not np.isnan(f):
        return _Float(f)
    return _NULL


psycopg2.extensions.register_adapter(float, nan_to_null)


class PostgresCustomHook(PostgresHook):
    conn_name_attr = 'postgres_conn_id'
    default_conn_name = 'postgres_default'
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def _generate_insert_sql(table, values, target_fields, replace, **kwargs):
        """
        Static helper method that generate the INSERT SQL statement.
        The REPLACE variant is specific to MySQL syntax.

        :param table: Name of the target table
        :type table: str
        :param values: The row to insert into the table
        :type values: tuple of cell values
        :param target_fields: The names of the columns to fill in the table
        :type target_fields: iterable of strings
        :param replace: Whether to replace instead of insert
        :type replace: bool
        :param replace_index: the column or list of column names to act as
            index for the ON CONFLICT clause
        :type replace_index: str or list
        :return: The generated INSERT or REPLACE SQL statement
        :rtype: str
        """
        placeholders = ["%s", ] * len(values)
        if target_fields:
            target_fields_fragment = ", ".join(target_fields)
            target_fields_fragment = "({})".format(target_fields_fragment)
        else:
            target_fields_fragment = ''
        sql = "INSERT INTO {0} as {0} {1} VALUES ({2})".format(
            table,
            target_fields_fragment,
            ",".join(placeholders))

        if replace:
            conflict_sql = kwargs.get("conflict_sql", None)
            if conflict_sql is None:
                raise ValueError("PostgreSQL ON CONFLICT statement must be specify!")
            # ex: conflict_sql = "ON CONFLICT (p1, p2) DO UPDATE SET f1 = EXCLUDE.f1, f2 = EXCLUDE.f2"
            sql += " " + conflict_sql

        return sql
