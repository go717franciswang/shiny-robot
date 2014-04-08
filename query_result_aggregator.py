from query_builder import QueryBuilder
from queue import Queue

class QueryResultAggregator:
    def __init__(self):
        self._max_concurrent_queries = 1
        self._schema = []
        self._unprocessed_queries = Queue()

    def set_max_concurrent_queries(self, max_concurrent_queries):
        if max_concurrent_queries < 1:
            raise ValueError('max concurrent queries must be > 0')
        self._max_concurrent_queries = max_concurrent_queries

    def set_schema(self, schema):
        self._schema = schema

    def add_query(self, query, cursor):
        self._unprocessed_queries.put((query, cursor))

    def aggregate(self):
        """docstring for aggregate"""
        pass
