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
        c = conn.cursor()
        c.execute('drop table if exists test.album')
        c.execute('''create table if not exists test.album
            (AlbumId int, Title varchar(100), ArtistId int)''')
        c.close()
        builder.add_table_with_fields('test.album', 'a', conn)
        query = builder.select(['albumid', 'artistid'])
        self.assertQuery(query, 'select a.albumid albumid, a.artistid artistid from test.album a')

    def testPostgresql(self):
        import psycopg2
        builder = QueryBuilder()
        conn = psycopg2.connect("dbname=test host=%s port=%s user=%s password=%s" % ( \
                self.config.get('postgresql', 'host'),
                self.config.get('postgresql', 'port'),
                self.config.get('postgresql', 'user'),
                self.config.get('postgresql', 'passwd')))
        c = conn.cursor()
        c.execute('drop table if exists album')
        c.execute('''create table album
            (AlbumId int, Title char(100), ArtistId int)''')
        c.close()
        builder.add_table_with_fields('album', 'a', conn)
        query = builder.select(['albumid', 'artistid'])
        self.assertQuery(query, 'select a.albumid albumid, a.artistid artistid from album a')

