from tests.test_query_builder_base import TestQueryBuilderBase
from query_builder import QueryBuilder
import configparser

class TestQueryBuilderAddTableWithFields(TestQueryBuilderBase):
    def setUp(self):
        config = configparser.RawConfigParser()
        config.read('tests/db_cred.ini')

    def testSqlite3(self):
        import sqlite3
        builder = QueryBuilder()
        conn = sqlite3.connect('tests/Chinook_Sqlite_AutoIncrementPKs.sqlite')
        builder.add_table_with_fields('album', 'a', conn)
        query = builder.select(['albumid', 'artistid'])
        self.assertQuery(query, 'select a.albumid albumid, a.artistid artistid from album a')

    def testMySql(self):
        import mysql.connector as mdb
        builder = QueryBuilder()
        conn = sqlite
