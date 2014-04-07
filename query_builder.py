import re

class QueryBuilder:
    def __init__(self):
        self._table_alias = {}
        self._alias_table = {}
        self._field_alias = {}
        self._G = {}

    def add_table(self, table, alias):
        if alias in self._alias_table:
            raise Exception("Alias '%s' is already used by table '%s'" % (alias, table))
        self._table_alias[table] = alias
        self._alias_table[alias] = table

    def add_field(self, field, alias):
        self._field_alias[field] = alias

    def link_tables(self, link):
        table_aliases = self.extract_aliases(link)
        if len(table_aliases) != 2:
            raise Exception("Invalid table link '%s'" % (link,))

        a1, a2 = table_aliases
        if a1 not in self._G:
            self._G[a1] = {}
        if a2 not in self._G:
            self._G[a2] = {}
        self._G[a1][a2] = link
        self._G[a2][a1] = link

    def extract_aliases(self, exp):
        alias_pattern = '([a-zA-Z0-9\_]+)\.'
        m = re.findall(alias_pattern, exp)
        return m

    def select(self, aliases):
        """docstring for select"""
        pass

