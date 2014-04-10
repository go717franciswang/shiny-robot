import unittest
import sqlite3
from query_result_aggregator import QueryResultAggregator

class TestQueryResultAggregator(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect('tests/Chinook_Sqlite_AutoIncrementPKs.sqlite')
        self.a = QueryResultAggregator()

    def testAggregate(self):
        self.a.set_schema(['q integer'])
        self.assertSimpleAggregation()

    def testAggregateNoSchema(self):
        self.assertSimpleAggregation()

    def assertSimpleAggregation(self):
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

    def testInconsistentNumberOfColumnsError(self):
        self.a.add_query('select sum(quantity) q from invoiceline where invoiceid = 1', self.conn)
        self.a.add_query('''
            select sum(quantity) q, sum(unitprice * quantity) c
            from invoiceline 
            where invoiceid = 2''', self.conn)
        with self.assertRaisesRegexp(Exception, "expected \d columns, got \d columns from .*"):
            self.a.aggregate('select sum(q) from tmp').next()




