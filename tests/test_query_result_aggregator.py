import unittest
import sqlite3
from query_result_aggregator import QueryResultAggregator

class TestQueryResultAggregator(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect('tests/Chinook_Sqlite_AutoIncrementPKs.sqlite')
        self.a = QueryResultAggregator()
        self.a.set_schema(['q integer'])

    def testAggregate(self):
        for i in range(1, 11):
            self.a.add_query(
                    'select sum(quantity) q from invoiceline where invoiceid = %d' % (i,), 
                    self.conn)
        q = self.a.aggregate('select sum(q) from tmp').next()[0]

        c = self.conn.cursor()
        c.execute('''select sum(quantity) 
            from invoiceline 
            where invoiceid between 1 and 10''')
        q2 = c.fetchone()[0]

        self.assertEquals(q, q2)





