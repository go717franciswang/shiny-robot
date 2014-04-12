import sqlite3
from tests.test_query_builder_base import TestQueryBuilderBase
from query_builder import QueryBuilder

class TestQueryBuilderAddTableWithFields(TestQueryBuilderBase):
    def testSqlite3(self):
        builder = QueryBuilder()
        conn = sqlite3.connect('tests/Chinook_Sqlite_AutoIncrementPKs.sqlite')
        builder.add_table_with_fields('album', 'a', conn)
        query = builder.select(['albumid', 'artistid'])
        self.assertQuery(query, 'select a.albumid albumid, a.artistid artistid from album a')

