import unittest
from functools import reduce
import int_to_big_int
import psycopg2
from multiprocessing import Process
from time import sleep
import os
from decimal import Decimal
from math import floor

TEST_TABLE = '''
CREATE TABLE test_table (
    test_column INT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT NOW()
);
'''

INSERT_TEST_ROW = '''
INSERT INTO "test_table" ("test_column")
VALUES ({});
'''

DROP_TABLE = '''
DROP TABLE test_table;
'''

ROWS = 5
MORE_ROWS = 5

class TestIntToBigInt(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        self.subject = int_to_big_int.IntToBigInt("test_table", "test_column")
        self.conn.cursor().execute(TEST_TABLE)
        self.conn.commit()

        for i in range(ROWS):
            self.conn.cursor().execute(INSERT_TEST_ROW.format(i))
        self.conn.commit()

    def tearDown(self):
        self.conn.cursor().execute(DROP_TABLE)
        self.conn.commit()

    def get_column_type(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT data_type FROM "information_schema"."columns"
            WHERE table_name = 'test_table'
            AND column_name = 'test_column';
        ''')
        return cursor.fetchone()[0]

    def get_one(self, query):
        cursor = self.conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]

    def run_test(self):
        self.assertEqual(self.get_column_type(), 'integer')
        noise = Process(target=random_queries)
        noise.start()
        self.subject.run()
        noise.join()
        self.assertEqual(self.get_column_type(), 'bigint')

    def test_avg(self):
        self.run_test()
        avg = self.get_one('SELECT FLOOR(AVG("test_column")::numeric) FROM "test_table"')
        self.assertEqual(floor(sum(range(ROWS+MORE_ROWS))/(ROWS+MORE_ROWS)), avg) # Floating point errors

    def test_max(self):
        self.run_test()
        max_ = self.get_one('SELECT MAX("test_column") FROM "test_table"')
        self.assertEqual(max_, ROWS+MORE_ROWS-1)

    def test_min(self):
        self.run_test()
        min_ = self.get_one('SELECT MIN("test_column") FROM "test_table"')
        self.assertEqual(min_, 0)

    def test_count(self):
        self.run_test()
        count = self.get_one('SELECT COUNT(*) FROM "test_table"')
        self.assertEqual(count, ROWS+MORE_ROWS)

    def test_sum(self):
        self.run_test()
        sum_ = self.get_one('SELECT SUM("test_column") FROM "test_table"')
        self.assertEqual(sum_, sum(range(ROWS+MORE_ROWS)))
        


def random_queries():
    conn = psycopg2.connect(os.environ.get('DATABASE_URL'))

    print('START CONCURRENT DATA INPUT')
    for n in range(ROWS, ROWS+MORE_ROWS):
        cursor = conn.cursor()
        query = INSERT_TEST_ROW.format(n)
        cursor.execute(query)
        conn.commit() # Make it visible immediately
        print(query)
    print('END CONCURRENT DATA INPUT')
    conn.close()


if __name__ == '__main__':
    unittest.main()
