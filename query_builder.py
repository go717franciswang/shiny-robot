class QueryBuilder:
    def __init__(self):
        self._table_alias = {}

    def add_table(self, table, alias):
        self._table_alias[table] = alias

    def alias(self, a):
        """docstring for alias"""
        pass

    def link_tables(self, link):
        """docstring for link_tables"""
        pass

    def select(self, aliases):
        """docstring for select"""
        pass

