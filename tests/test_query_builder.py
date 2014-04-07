import unittest
from query_builder import QueryBuilder

class TestQueryBuilder(unittest.TestCase):
    def setUp(self):
        # table relationships
        self.builder = QueryBuilder()
        self.builder.add_table('table_a', 'a')
        self.builder.alias('a.f fa')

        self.builder.add_table('table_b', 'b')
        self.builder.link_tables('a.id = b.id')
        self.builder.alias('b.f fb')

        self.builder.add_table('table_c', 'c')
        self.builder.link_tables('a.id = c.id')
        self.builder.alias('c.f fc')

        self.builder.add_table('table_d', 'd')
        self.builder.link_tables('c.id = d.id')
        self.builder.alias('d.f fd')

    def testSelectA(self):
        query = self.builder.select(['fa'])
        self.assertEquals(query, 'select a.f fa from table_a a')

    def testSelectB(self):
        query = self.builder.select(['fb'])
        self.assertEquals(query, 'select b.f fb from table_b b')

    def testSelectC(self):
        query = self.builder.select(['fc'])
        self.assertEquals(query, 'select c.f fc from table_c c')

    def testSelectD(self):
        query = self.builder.select(['fd'])
        self.assertEquals(query, 'select d.f fd from table_d d')

    def testSelectAB(self):
        query = self.builder.select(['fa', 'fb'])
        self.assertEquals(query, '''
            select a.f fa, b.f fb 
            from table_a a 
            join table_b b on a.id = b.id''')

    def testSelectAC(self):
        query = self.builder.select(['fa', 'fc'])
        self.assertEquals(query, '''
            select a.f fa, c.f fc 
            from table_a a 
            join table_c c on a.id = c.id''')

    def testSelectAD(self):
        query = self.builder.select(['fa', 'fd'])
        self.assertEquals(query, '''
            select a.f fa, d.f fd 
            from table_a a 
            join table_c c on a.id = c.id 
            join table_d d on c.id = d.id''')

    def testSelectBC(self):
        query = self.builder.select(['fb', 'fc'])
        self.assertEquals(query, '''
            select b.f fb, c.f fc 
            from table_a a 
            join table_b b on a.id = b.id 
            join table_c c on a.id = c.id''')

    def testSelectBD(self):
        query = self.builder.select(['fb', 'fd'])
        self.assertEquals(query, '''
            select b.f fb, d.f fd 
            from table_a a 
            join table_b b on a.id = b.id 
            join table_c c on a.id = c.id 
            join table_d d on c.id = d.id''')

    def testSelectCD(self):
        query = self.builder.select(['fc', 'fd'])
        self.assertEquals(query, '''
            select c.f fc, d.f fd 
            from table_c c 
            join table_d d on c.id = d.id''')

    def testSelectABC(self):
        query = self.builder.select(['fa', 'fb', 'fc'])
        self.assertEquals(query, '''
            select a.f fa, b.f fb, c.f fc 
            from table_a a 
            join table_b b on a.id = b.id 
            join table_c c on a.id = c.id''')

    def testSelectABCD(self):
        query = self.builder.select(['fa', 'fb', 'fc', 'fd'])
        self.assertEquals(query, '''
            select a.f fa, b.f fb, c.f fc, d.f fd 
            from table_a a 
            join table_b b on a.id = b.id 
            join table_c c on a.id = c.id 
            join table_d d on c.id = d.id''')
