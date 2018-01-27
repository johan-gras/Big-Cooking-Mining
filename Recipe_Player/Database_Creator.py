#If MySQLdb not installed
#pip install mysqlclient
import MySQLdb


class Database:
    # Database arguments :
    location = "localhost"
    user = "root"
    password = ""
    database_name = "big_cooking_data"

    # Open database connection
    db = MySQLdb.connect(location, user, password, database_name)
    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    def disconnect(self):
        self.db.close()

    def get_version(self):
        self.cursor.execute("SELECT VERSION()")
        return self.cursor.fetchone()[0]
