
import unittest
import pandas as pd
from src.monitor import check_range, check_non_null

class TestRules(unittest.TestCase):
    def test_range_rule(self):
        df = pd.DataFrame({"x":[1,2,3,100]})
        r = check_range(df, "x", 1, 10)
        self.assertFalse(r.passed)

    def test_non_null_rule(self):
        df = pd.DataFrame({"a":[1,None]})
        r = check_non_null(df, ["a"])
        self.assertFalse(r.passed)

if __name__ == "__main__":
    unittest.main()
