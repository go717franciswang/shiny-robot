import re
import copy

class QueryBuilder:
    def __init__(self):
        self._table_alias = {}
        self._alias_table = {}
        self._table_alias_order = []
        self._field_alias = {}
        self._alias_field = {}
        self._G = {}
        self._link_order = []
        self._where_conditions = []
        self._group_by_fields = []
        self._having_conditions = []

    def add_table(self, table, alias):
        if alias in self._alias_table:
            raise Exception("Alias '%s' is already used by table '%s'" % (alias, table))
        self._table_alias[table] = alias
        self._alias_table[alias] = table
        self._table_alias_order.append(alias)

    def add_field(self, field, alias):
        self._field_alias[field] = alias
        self._alias_field[alias] = field

    def link_tables(self, link):
        table_aliases = self._extract_aliases(link)
        if len(table_aliases) != 2:
            raise Exception("Invalid table link '%s'" % (link,))

        a1, a2 = table_aliases
        if a1 not in self._G:
            self._G[a1] = {}
        if a2 not in self._G:
            self._G[a2] = {}
        self._G[a1][a2] = link
        self._G[a2][a1] = link
        self._link_order.append(link)

    def _extract_aliases(self, exp):
        alias_pattern = '([a-zA-Z]+[a-zA-Z0-9\_]*)\.'
        m = re.findall(alias_pattern, exp)
        return m

    def select(self, aliases):
        table_aliases = self._fields2tables(aliases)
        table_aliases = table_aliases.union(self._where2tables())
        table_aliases = table_aliases.union(self._group_by2tables())
        required_table_aliases, required_links = self._get_requirements(table_aliases)

        select_stmt = [self._alias_field[x] + ' ' + x for x in aliases]
        from_stmt = [self._alias_table[x] + ' ' + x for x in required_table_aliases]

        query = "select " + ', '.join(select_stmt) + \
            " from " + ', '.join(from_stmt)
        if len(required_links + self._where_conditions) > 0:
            query += " where " + ' and '.join(required_links + self._where_conditions)

        if len(self._group_by_fields) > 0:
            query += " group by " + ', '.join(self._group_by_fields)

        if len(self._having_conditions) > 0:
            self._validate_having(aliases)
            query += " having " + ' and '.join(self._having_conditions)
            
        return query

    def _get_requirements(self, table_aliases):
        s = table_aliases.pop()
        reached = set()
        reached.add(s)
        unreached = table_aliases
        links = set()

        # depth first search to find links to reach every required table with minimum distance
        while len(unreached) != 0:
            t = unreached.pop()
            discovered = set()
            discovered.add(s)
            _, path = self._dfs(s, t, discovered)
            if t not in path:
                raise Exception("Cannot reach table '%s'" % (t,))

            for v in path.keys():
                reached.add(v)
                links.add(path[v])
                unreached.discard(v)

        reached_sorted = [x for x in self._table_alias_order if x in reached]
        links_sorted = [x for x in self._link_order if x in links]

        return reached_sorted, links_sorted

    def _dfs(self, s, t, discovered):
        for v in self._G[s].keys():
            if v in discovered:
                continue

            discovered.add(v)
            if v == t:
                return [discovered, {v: self._G[s][v]}]
            else:
                discovered, path = self._dfs(v, t, discovered)
                if len(path) > 0:
                    path[v] = self._G[s][v]
                    return [discovered, path]

        return [discovered, {}]

    def _fields2tables(self, aliases):
        fields = [self._alias_field[x] for x in aliases]
        table_aliases = set()
        for field in fields:
            for y in self._extract_aliases(field):
                table_aliases.add(y)
        return table_aliases

    def _where2tables(self):
        table_aliases = set()
        for x in self._where_conditions:
            for y in self._extract_aliases(x):
                table_aliases.add(y)
        return table_aliases

    def _group_by2tables(self):
        table_aliases = set()
        for x in self._group_by_fields:
            for y in self._extract_aliases(x):
                table_aliases.add(y)
        return table_aliases

    def copy(self):
        return copy.deepcopy(self)

    def where(self, condition):
        c = self.copy()
        c._where(condition)
        return c

    def _where(self, condition):
        condition = self._sub_alias_with_field(condition)
        self._where_conditions.append(condition)

    def _sub_alias_with_field(self, condition):
        matched = self._extract_alias_span(condition)

        # replace from back to front so that there is not change in offset in substituted string
        matched.reverse()
        for s,e in matched:
            condition = condition[:s] + self._alias_field[condition[s:e]] + condition[e:]

        return condition

    def _extract_alias_span(self, condition):
        # even number of quotes followed by alias-like string
        alias_pattern = '(?:\'[^\']*\')|(?:"[^"]*")|([a-zA-Z]+[a-zA-Z0-9\_]*)'
        matches = re.finditer(alias_pattern, condition)

        matched = []
        for m in matches:
            alias = m.group()
            # (?:..) noncapture group does not seem to work in re.finditer
            if alias == '' or alias[0] == '"' or alias[0] == "'":
                continue

            if alias not in self._alias_field:
                raise Exception("Unknown alias '%s' in condition '%s'" % (alias, condition))
            matched.append(m.span())

        return matched

    def _group_by(self, aliases):
        fields = [self._alias_field[x] for x in aliases]
        self._group_by_fields = fields

    def group_by(self, aliases):
        c = self.copy()
        c._group_by(aliases)
        return c

    def _having(self, condition):
        self._having_conditions.append(condition)

    def having(self, condition):
        c = self.copy()
        c._having(condition)
        return c

    def _validate_having(self, selected_aliases):
        selected_aliases = set(selected_aliases)
        for condition in self._having_conditions:
            for s,e in self._extract_alias_span(condition):
                alias = condition[s:e]
                if alias not in selected_aliases:
                    raise Exception("alias '%s' needs to be selected" % (alias,))

