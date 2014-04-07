import unittest
import re

class TestQueryBuilderBase(unittest.TestCase):

    def assertQuery(self, q1, q2):
        q1 = re.sub('\s+', ' ', q1).strip()
        q2 = re.sub('\s+', ' ', q2).strip()
        self.assertEquals(q1, q2)
