import unittest
import pandas as pd

class TestData(unittest.TestCase):
    def test_nyt_data(self):
        df = pd.read_csv("nyt_books_google_data.csv")
        self.assertGreaterEqual(len(df), 10)
        self.assertIn("Author", df.columns)

if __name__ == '__main__':
    unittest.main()
