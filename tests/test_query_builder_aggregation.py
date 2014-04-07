from tests.test_query_builder_base import TestQueryBuilderBase
from query_builder import QueryBuilder

class TestQueryBuilderAggregation(TestQueryBuilderBase):
    def setUp(self):
        self.builder = QueryBuilder()
        self.builder.add_table('table_a', 'a')
        self.builder.add_field('sum(a.f1)', 'fa')
        self.builder.add_field('a.f2', 'ga')

        self.builder.add_table('table_b', 'b')
        self.builder.add_field('count(b.f)', 'fb')
        self.builder.link_tables('a.id = b.id')

        self.builder.add_field('sum(a.f1) / count(b.f)', 'fab')

    def testSelectSumA(self):
        query = self.builder.select(['fa'])
        self.assertQuery(query, '''
            select sum(a.f1) fa
            from table_a a''')

    def testSelectCountB(self):
        query = self.builder.select(['fb'])
        self.assertQuery(query, '''
            select count(b.f) fb
            from table_b b''')

    def testSelectAvgAB(self):
        query = self.builder.select(['fab'])
        self.assertQuery(query, '''
            select sum(a.f1) / count(b.f) fab
            from table_a a,
                table_b b
            where a.id = b.id''')

    def testGroupBy(self):
        query = self.builder.group_by(['ga']).select('ga', 'fa')
        self.assertQuery(query, '''
            select sum(a.f1) fa
            from table_a a
            group by a.f2''')
