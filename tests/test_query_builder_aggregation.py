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
        self.builder.add_field('concat(a.f2, b.f)', 'cab')
        self.builder.add_field('0', 'c')

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
        query = self.builder.group_by(['ga']).select(['ga', 'fa'])
        self.assertQuery(query, '''
            select a.f2 ga, sum(a.f1) fa
            from table_a a
            group by a.f2''')

        query = self.builder.group_by(['ga']).select(['fa'])
        self.assertQuery(query, '''
            select sum(a.f1) fa
            from table_a a
            group by a.f2''')

    def testGroupByFormula(self):
        query = self.builder.group_by(['cab']).select(['fa'])
        self.assertQuery(query, '''
            select sum(a.f1) fa
            from table_a a,
                table_b b
            where a.id = b.id
            group by concat(a.f2, b.f)''')

        query = self.builder.group_by(['ga', 'cab']).select(['fa'])
        self.assertQuery(query, '''
            select sum(a.f1) fa
            from table_a a,
                table_b b
            where a.id = b.id
            group by a.f2, concat(a.f2, b.f)''')

    def testHaving(self):
        query = self.builder.having('fa > 1').select(['fa'])
        self.assertQuery(query, '''
            select sum(a.f1) fa
            from table_a a
            having fa > 1''')

        query = self.builder.group_by(['ga']).having('fa > 1').select(['fa'])
        self.assertQuery(query, '''
            select sum(a.f1) fa
            from table_a a
            group by a.f2
            having fa > 1''')

    def testHavingContainUnselectedAlias(self):
        with self.assertRaisesRegexp(Exception, "alias 'cab' needs to be selected"):
            self.builder.having('cab = "abc"').select(['fa'])

    def testSelectConstant(self):
        query = self.builder.select(['c'])
        self.assertQuery(query, 'select 0 c')

        query = self.builder.select(['fa', 'c'])
        self.assertQuery(query, '''
            select sum(a.f1) fa, 0 c
            from table_a a''')

