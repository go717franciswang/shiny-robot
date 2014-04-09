from query_builder import QueryBuilder
from Queue import Queue
import sqlite3
from threading import Thread
import decimal
import datetime

class QueryResultAggregator:
    def __init__(self):
        self._max_concurrent_queries = 1
        self._schema = None
        self._unprocessed_queries = Queue()
        self._responses = Queue()
        self._conn = sqlite3.connect(':memory:')
        self._insertion_query = None

    def set_max_concurrent_queries(self, max_concurrent_queries):
        if max_concurrent_queries < 1:
            raise ValueError('max concurrent queries must be > 0')
        self._max_concurrent_queries = max_concurrent_queries

    def set_schema(self, schema):
        self._schema = schema
        self._create_tmp()
        self._setup_insertion_query()

    def add_query(self, query, connection):
        self._unprocessed_queries.put((query, connection))

    def _create_tmp(self):
        q = 'create table if not exists tmp ('
        q += ', '.join(self._schema)
        q += ')'
        c = self._conn.cursor()
        c.execute(q)
        c.close()

    def _setup_insertion_query(self):
        self._insertion_query = 'insert into tmp '
        self._insertion_query += 'values (?' + ',?' * (len(self._schema)-1) + ')'

    def _query(self):
        while True:
            if self._unprocessed_queries.empty():
                self._responses.put(StopIteration)
                return

            q, conn = self._unprocessed_queries.get()
            c = conn.cursor()
            rs = c.execute(q)
            self._responses.put((rs, c, q))
            self._unprocessed_queries.task_done()

    def _norm_row(self, row):
        res = []
        for i in range(len(row)):
            typeof = type(row[i])
            if typeof == decimal.Decimal:
                res.append(float(row[i]))
            elif typeof == bytes or typeof == bytearray:
                res.append(row[i].decode())
            elif typeof == datetime.date:
                res.append(row[i].strftime('%Y-%m-%d'))
            else: res.append(row[i])
        return res

    def _start_query(self):
        t = Thread(target=self._query)
        t.daemon = True
        t.start()

    def _multplex(self):
        if self._max_concurrent_queries == 1:
            self._query()
        else:
            for i in range(self._max_concurrent_queries):
                t = Thread(target=self._query)
                t.daemon = True
                t.start()

        threads_completed = 0
        while True:
            rs = self._responses.get()
            if rs == StopIteration:
                threads_completed += 1
                if threads_completed == self._max_concurrent_queries:
                    return
                continue
            yield rs

    def aggregate(self, query):
        responses = self._multplex()
        cursor = self._conn.cursor()
        for rs, c, q in responses:
            for row in rs:
                cursor.execute(self._insertion_query, self._norm_row(row))
            c.close()

        for row in cursor.execute(query):
            yield row
        cursor.close()

