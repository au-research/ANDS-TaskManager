import sys
import unittest
import pymysql
import myconfig


def helper_is_connected():
    try:
        db = pymysql.connect(
            host=myconfig.db_host,
            user=myconfig.db_user,
            passwd=myconfig.db_passwd,
            db=myconfig.db)
        cursor = db.cursor()
        cursor.execute("SELECT VERSION()")
        results = cursor.fetchone()
        if results:
            return True
        else:
            return False
    except Exception:
        e = sys.exc_info()[1]
        raise RuntimeError("Database Exception %s" % e)


class TestConfig(unittest.TestCase):

    def test_creation(self):
        self.assertIsInstance(myconfig.run_dir, str)
        self.assertIsInstance(myconfig.response_url, str)
        self.assertIsInstance(myconfig.maintenance_request_url, str)
        self.assertIsInstance(myconfig.log_dir, str)
        self.assertIsInstance(myconfig.log_level, str)
        self.assertIsInstance(myconfig.db_host, str)
        self.assertIsInstance(myconfig.db_user, str)
        self.assertIsInstance(myconfig.db_passwd, str)
        self.assertIsInstance(myconfig.db, str)
        self.assertIsInstance(myconfig.tasks_table, str)

    def test_database_connection(self):
        self.assertTrue(helper_is_connected())


if __name__ == '__main__':
    unittest.main()
