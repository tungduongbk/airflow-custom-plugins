from typing import List, Any, Optional, Dict

from airflow.providers.apache.sqoop.hooks.sqoop import SqoopHook


class SqoopCustomHook(SqoopHook):

    def __init__(self, schema, conn_id, verbose, num_mappers):
        super(SqoopCustomHook, self).__init__(num_mappers=num_mappers, conn_id=conn_id, verbose=verbose)
        self.schema: str = schema

    def import_query_cmd(
            self,
            query: str,
            target_dir: Optional[str] = None,
            append: bool = False,
            file_type: str = "text",
            split_by: Optional[str] = None,
            direct: Optional[bool] = None,
            driver: Optional[Any] = None,
            extra_import_options: Optional[Dict[str, Any]] = None,
    ) -> Any:

        cmd = self._import_cmd(target_dir, append, file_type, split_by, direct, driver, extra_import_options)
        cmd += ["--query", query]

        return cmd

    def import_table_cmd(
            self,
            table: str,
            target_dir: Optional[str] = None,
            append: bool = False,
            file_type: str = "text",
            columns: Optional[str] = None,
            split_by: Optional[str] = None,
            where: Optional[str] = None,
            direct: bool = False,
            driver: Any = None,
            extra_import_options: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Imports table from remote location to target dir. Arguments are
        copies of direct sqoop command line arguments

        :param table: Table to read
        :param target_dir: HDFS destination dir
        :param append: Append data to an existing dataset in HDFS
        :param file_type: "avro", "sequence", "text" or "parquet".
            Imports data to into the specified format. Defaults to text.
        :param columns: <col,col,col…> Columns to import from table
        :param split_by: Column of the table used to split work units
        :param where: WHERE clause to use during import
        :param direct: Use direct connector if exists for the database
        :param driver: Manually specify JDBC driver class to use
        :param extra_import_options: Extra import options to pass as dict.
            If a key doesn't have a value, just pass an empty string to it.
            Don't include prefix of -- for sqoop options.
        """
        cmd = self._import_cmd(target_dir, append, file_type, split_by, direct, driver, extra_import_options)

        cmd += ["--table", table]

        if columns:
            cmd += ["--columns", columns]
        if where:
            cmd += ["--where", where]

        return cmd
