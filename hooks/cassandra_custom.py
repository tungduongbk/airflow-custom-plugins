import pandas as pd
from typing import List, Union, Optional
from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.query import BatchStatement, Statement
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from airflow.exceptions import AirflowException

default_write_consistency_level = ConsistencyLevel.LOCAL_QUORUM

class CassandraCustomHook(CassandraHook):
    """
    Cassandra connect interaction wrapper

    :param keyspace: The keyspace that overwrite keyspace in connection
    :type keyspace: str

    """
    

    def __init__(
            self,
            keyspace=None,
            **kwargs):
        super(CassandraCustomHook, self).__init__(**kwargs)
        if keyspace is not None:
            self.keyspace = keyspace

    def _resolve_consistency_level(self, consistency_level) -> ConsistencyLevel:
        if type(consistency_level) is str:
            if consistency_level == "ALL":
                return ConsistencyLevel.ALL
            elif consistency_level == "EACH_QUORUM":
                return ConsistencyLevel.EACH_QUORUM
            elif consistency_level == "QUORUM":
                return ConsistencyLevel.QUORUM
            elif consistency_level == "LOCAL_QUORUM":
                return ConsistencyLevel.LOCAL_QUORUM
            elif consistency_level == "ONE":
                return ConsistencyLevel.ONE
            elif consistency_level == "TWO":
                return ConsistencyLevel.TWO
            elif consistency_level == "THREE":
                return ConsistencyLevel.THREE
            elif consistency_level == "LOCAL_ONE":
                return ConsistencyLevel.LOCAL_ONE
            elif consistency_level == "ANY":
                return ConsistencyLevel.ANY
            else:
                self.log.warning(f"Consistency level {consistency_level} not supperted" +
                    "\nPlease read https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/dml/dmlConfigConsistency.html to more details" +
                    f"\nUse default consistency level: {default_write_consistency_level}")
                return default_write_consistency_level
        elif type(consistency_level) is ConsistencyLevel:
            return consistency_level
        else:
            self.log.warning(f"Consistency level type {type(consistency_level)} is invalid." +
                "\nPlease use correct type [str, cassandra.ConsistencyLevel]." +
                f"\nUse default consistency level: {default_write_consistency_level}")
            return default_write_consistency_level

    def insert_dataframe(self,
        df: pd.DataFrame,
        table: str,
        batch_insert_records = 500,
        async_timeout = 300000,
        write_consistency_level: Union[str, ConsistencyLevel] = default_write_consistency_level) -> bool:
        """
        Write pandas.DataFrame to cassandra

        :param batch_insert_records: the number of row with insert into a BatchStatement
        :type batch_insert_records: int

        :param async_timeout: the timeout in miliseconds to wait a async query
        :type async_timeout: long

        :param write_consistency_level: the consistency level
        :type write_consistency_level: Union[str, cassandra.ConsistencyLevel]
        """
        _write_consistency_level = self._resolve_consistency_level(write_consistency_level)
        _is_failed = False
        def when_success(results):
            self.log.info(f"Insert rows successful")
        def when_failed(err):
            self.log.error("Insert failed: %s", err)
            _is_failed = True

        cols = df.columns.tolist()
        session = self.get_conn()
        query = "INSERT INTO %s(%s) VALUES (%s)" % (table, ','.join(cols), ','.join(['?']*len(cols)))
        prepared_query = session.prepare(query)
        for partition in self._split_to_partitions(df, batch_insert_records):
            batch = BatchStatement(consistency_level=_write_consistency_level)
            for _, item in partition.iterrows():
                values = tuple(item.to_list())
                batch.add(prepared_query, values)
            f = session.execute_async(batch, async_timeout)
            f.add_callbacks(when_success, when_failed)
        if _is_failed:
            raise AirflowException("Error when insert data to cassandra")

    def _split_to_partitions(self, df: pd.DataFrame, batch_insert_records=500) -> List[pd.DataFrame]:
        partitions = []
        i = 0
        while i < df.shape[0]:
            partitions.append(df.loc[i:i+batch_insert_records - 1])
            i = i + batch_insert_records
        return partitions

    def select_dataframe(self, cql: Union[str, Statement], fetch_size = None, timeout = None, existed_session: Optional[Session] = None) -> pd.DataFrame:
        def pandas_factory(colnames, rows) -> pd.DataFrame:
            return pd.DataFrame(rows, columns=colnames)
        session = existed_session if existed_session is not None else self.get_conn()
        session.row_factory = pandas_factory
        session.default_fetch_size = fetch_size

        result = session.execute(cql, timeout=timeout)
        df = result._current_rows
        return df
