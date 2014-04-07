import unittest
import re
from query_builder import QueryBuilder

class TestQueryBuilder(unittest.TestCase):
    def setUp(self):
        # table relationships
        self.builder = QueryBuilder()
        self.builder.add_table('table_a', 'a')
        self.builder.add_field('a.f', 'fa')

        self.builder.add_table('table_b', 'b')
        self.builder.link_tables('a.id = b.id')
        self.builder.add_field('b.f', 'fb')

        self.builder.add_table('table_c', 'c')
        self.builder.link_tables('a.id = c.id')
        self.builder.add_field('c.f', 'fc')

        self.builder.add_table('table_d', 'd')
        self.builder.link_tables('c.id = d.id')
        self.builder.add_field('d.f', 'fd')

    def testSelectA(self):
        query = self.builder.select(['fa'])
        self.assertQuery(query, 'select a.f fa from table_a a')

    def testSelectB(self):
        query = self.builder.select(['fb'])
        self.assertQuery(query, 'select b.f fb from table_b b')

    def testSelectC(self):
        query = self.builder.select(['fc'])
        self.assertQuery(query, 'select c.f fc from table_c c')

    def testSelectD(self):
        query = self.builder.select(['fd'])
        self.assertQuery(query, 'select d.f fd from table_d d')

    def testSelectAB(self):
        query = self.builder.select(['fa', 'fb'])
        self.assertQuery(query, '''
            select a.f fa, b.f fb 
            from table_a a,
                table_b b 
            where a.id = b.id''')

    def testSelectAC(self):
        query = self.builder.select(['fa', 'fc'])
        self.assertQuery(query, '''
            select a.f fa, c.f fc 
            from table_a a,
                table_c c 
            where a.id = c.id''')

    def testSelectAD(self):
        query = self.builder.select(['fa', 'fd'])
        self.assertQuery(query, '''
            select a.f fa, d.f fd 
            from table_a a,
                table_c c, 
                table_d d 
            where a.id = c.id and c.id = d.id''')

    def testSelectBC(self):
        query = self.builder.select(['fb', 'fc'])
        self.assertQuery(query, '''
            select b.f fb, c.f fc 
            from table_a a,
                table_b b,
                table_c c 
            where a.id = b.id and a.id = c.id''')

    def testSelectBD(self):
        query = self.builder.select(['fb', 'fd'])
        self.assertQuery(query, '''
            select b.f fb, d.f fd 
            from table_a a,
                table_b b, 
                table_c c, 
                table_d d 
            where a.id = b.id and a.id = c.id and c.id = d.id''')

    def testSelectCD(self):
        query = self.builder.select(['fc', 'fd'])
        self.assertQuery(query, '''
            select c.f fc, d.f fd 
            from table_c c,
                table_d d 
            where c.id = d.id''')

    def testSelectABC(self):
        query = self.builder.select(['fa', 'fb', 'fc'])
        self.assertQuery(query, '''
            select a.f fa, b.f fb, c.f fc 
            from table_a a,
                table_b b, 
                table_c c 
            where a.id = b.id and a.id = c.id''')

    def testSelectABCD(self):
        query = self.builder.select(['fa', 'fb', 'fc', 'fd'])
        self.assertQuery(query, '''
            select a.f fa, b.f fb, c.f fc, d.f fd 
            from table_a a, 
                table_b b, 
                table_c c, 
                table_d d
            where a.id = b.id and a.id = c.id and c.id = d.id''')

    def assertQuery(self, q1, q2):
        q1 = re.sub('\s+', ' ', q1)
        q2 = re.sub('\s+', ' ', q2)
        # print "q1: \n" + q1
        # print "q2: \n" + q2
        self.assertEquals(q1, q2)
