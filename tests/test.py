import unittest
from decimal import Decimal

import utils


class TestFunctions(unittest.TestCase):
    def test_str_to_decimal(self):
        """
        Test converting a string to decimal
        """
        s = ""
        result = utils.str_to_decimal(s)
        self.assertEqual(result, Decimal(0))

    # TODO: mock csv with a few rows


if __name__ == '__main__':
    unittest.main()
