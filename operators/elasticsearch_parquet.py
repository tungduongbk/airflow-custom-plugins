import json
import os
import time

import pandas as pd
from airflow.models.baseoperator import BaseOperator
from airflow.utils.operator_helpers import context_to_airflow_vars
from elasticsearch.helpers import ScanError
from hooks.es_hook import ElasticSearchHook
from utils.parse_date import create_path_from_date_time


class ES2ParquetOperator(BaseOperator):
    template_fields = ('op_kwargs',)
    ui_color = '#33BBFE'

    def __init__(self,
                parse_function: callable,
                query_builder: callable,
                es_conn_id: str = "",
                es_index: str = "",
                source: list = [],
                scroll: str = "1m",
                chunk_size: int = 30,
                size: int = 10000,
                path_to_store: str = "",
                op_kwargs: dict = {},
                extension: str = "gzip",
                force_str_cols=[],
                 *args, **kwargs):

        super(ES2ParquetOperator, self).__init__(*args, **kwargs)
        self.es_conn_id = es_conn_id
        self.index = es_index
        self.source = source
        self.parse_function = parse_function
        self.query_builder = query_builder
        self.scroll = scroll
        self.local_storage = path_to_store
        self.chunk_size = chunk_size
        self.size = size
        self.op_kwargs = op_kwargs
        self.extension = extension
        self.force_str_cols = force_str_cols
        if not os.path.exists(self.local_storage):
            os.makedirs(self.local_storage)

    def execute(self, context):
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)

        self.log.debug("Exporting the following env vars:\n%s",
                        '\n'.join(["{}={}".format(k, v)
                                for k, v in airflow_context_vars.items()]))
        os.environ.update(airflow_context_vars)

        es_hook = ElasticSearchHook(conn_id=self.es_conn_id)
        es_client = es_hook.get_conn()
        lucene_query = self.query_builder(**self.op_kwargs)
        self.log.info(
            "Run elastic search query of index {}: \n{}".format(self.index, json.dumps(lucene_query, indent=2)))
        start_time = time.time()
        self.scan(client=es_client,
                    scroll=self.scroll,
                    query=lucene_query,
                    index=self.index,
                    size=self.size,
                    raise_on_error=False)

        self.log.info("Process after {}s".format(time.time() - start_time))

    def _to_parquet(self, store_dir, file_name, result_list):
        self.log.info(" *** Saving data to parquet file ...")
        df = pd.DataFrame(result_list)
        if not os.path.exists(store_dir):
            os.makedirs(store_dir)
        save_path = "{}/{}.parquet.{}".format(store_dir, file_name, self.extension)
        self.log.info(" *** Saved file to local path: {}".format(save_path))
        for f in self.force_str_cols:
            df[f] = df[f].astype(str)
        df.to_parquet(save_path, index=False, engine='pyarrow', compression=self.extension)

    def scan(self,
            client,
            query=None,
            scroll="5m",
            raise_on_error=True,
            preserve_order=False,
            size=10000,
            request_timeout=None,
            clear_scroll=True,
            scroll_kwargs=None,
            **kwargs):
        """
        Simple abstraction on top of the
        :meth:`~elasticsearch.Elasticsearch.scroll` api - a simple iterator that
        yields all hits as returned by underlining scroll requests.

        By default scan does not return results in any pre-determined order. To
        have a standard order in the returned documents (either by score or
        explicit sort definition) when scrolling, use ``preserve_order=True``. This
        may be an expensive operation and will negate the performance benefits of
        using ``scan``.

        :arg client: instance of :class:`~elasticsearch.Elasticsearch` to use
        :arg query: body for the :meth:`~elasticsearch.Elasticsearch.search` api
        :arg scroll: Specify how long a consistent view of the index should be
            maintained for scrolled search
        :arg raise_on_error: raises an exception (``ScanError``) if an error is
            encountered (some shards fail to execute). By default we raise.
        :arg preserve_order: don't set the ``search_type`` to ``scan`` - this will
            cause the scroll to paginate with preserving the order. Note that this
            can be an extremely expensive operation and can easily lead to
            unpredictable results, use with caution.
        :arg size: size (per shard) of the batch send at each iteration.
        :arg request_timeout: explicit timeout for each call to ``scan``
        :arg clear_scroll: explicitly calls delete on the scroll id via the clear
            scroll API at the end of the method on completion or error, defaults
            to true.
        :arg scroll_kwargs: additional kwargs to be passed to
            :meth:`~elasticsearch.Elasticsearch.scroll`

        Any additional keyword arguments will be passed to the initial
        :meth:`~elasticsearch.Elasticsearch.search` call::

            scan(es,
                query={"query": {"match": {"title": "python"}}},
                index="orders-*",
                doc_type="books"
            )

        """
        scroll_kwargs = scroll_kwargs or {}

        if not preserve_order:
            query = query.copy() if query else {}
            query["sort"] = "_doc"

        # initial search
        resp = client.search(
            body=query, scroll=scroll, size=size, request_timeout=request_timeout, **kwargs
        )
        scroll_id = resp.get("_scroll_id")
        no_returned_res = resp["hits"]["total"]["value"]
        self.log.info("Got {} total documents".format(no_returned_res))
        no_loops = no_returned_res // self.size + 1
        self.log.info("Number of scroll loops: {}".format(no_loops))
        scroll_times = 0
        results = list()
        number = 0
        no_actual_res = 0

        try:
            path_dir, file_name = create_path_from_date_time(self.op_kwargs.get('run_date'))
            store_dir = os.path.join(self.local_storage, path_dir)
            while scroll_id and resp["hits"]["hits"]:
                scroll_times += 1
                for hit in resp["hits"]["hits"]:
                    r = self.parse_function(hit)
                    if r is not None:
                        results.append(r)
                if scroll_times % self.chunk_size == 0:
                    no_actual_res += len(results)
                    self._to_parquet(store_dir, file_name + "_" + str(number), results)
                    number += 1
                    del results
                    results = list()

                if (resp["_shards"]["successful"] + resp["_shards"]["skipped"]) < resp[
                    "_shards"
                ]["total"]:
                    self.log.warning(
                        "Scroll request has only succeeded on %d (+%d skipped) shards out of %d.",
                        resp["_shards"]["successful"],
                        resp["_shards"]["skipped"],
                        resp["_shards"]["total"],
                    )
                    if raise_on_error:
                        raise ScanError(
                            scroll_id,
                            "Scroll request has only succeeded on %d (+%d skiped) shards out of %d."
                            % (
                                resp["_shards"]["successful"],
                                resp["_shards"]["skipped"],
                                resp["_shards"]["total"],
                            ),
                        )
                resp = client.scroll(
                    body={"scroll_id": scroll_id, "scroll": scroll}, **scroll_kwargs
                )
                scroll_id = resp.get("_scroll_id")

            if len(results) > 0:
                no_actual_res += len(results)
                self._to_parquet(store_dir,
                                    file_name + "_" + str(number),
                                    results)

            self.log.info("Number of scroll times: {}".format(scroll_times))
            self.log.info("Get number of {}/{} results".format(no_actual_res, no_returned_res))

        finally:
            if scroll_id and clear_scroll:
                client.clear_scroll(body={"scroll_id": [scroll_id]}, ignore=(404,))
