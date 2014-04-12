from tests.test_query_builder_base import TestQueryBuilderBase
from query_builder import QueryBuilder
import ConfigParser

class TestQueryBuilderAddTableWithFields(TestQueryBuilderBase):
    def setUp(self):
        self.config = ConfigParser.RawConfigParser()
        self.config.read('tests/db_cred.ini')

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
        conn = mdb.connect(
                host=self.config.get('mysql', 'host'), 
                port=self.config.get('mysql', 'port'),
                user=self.config.get('mysql', 'user'),
                passwd=self.config.get('mysql', 'passwd'))
        builder.add_table_with_fields('test.album', 'a', conn)
        query = builder.select(['albumid', 'artistid'])
        self.assertQuery(query, 'select a.albumid albumid, a.artistid artistid from test.album a')

