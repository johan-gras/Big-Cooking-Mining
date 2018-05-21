#If MySQLdb not installed
#pip install mysqlclient
import MySQLdb


class Database:
    # Database arguments :
    location = "localhost"
    user = "root"
    password = ""
    database_name = "big_cooking_data"

    db = None
    cursor = None

    def connect(self):
        self.db = MySQLdb.connect(self.location, self.user, self.password, self.database_name)
        self.cursor = self.db.cursor()

    def disconnect(self):
        self.db.close()

    def get_version(self):
        self.cursor.execute("SELECT VERSION()")
        return self.cursor.fetchone()[0]

    def create_db(self):
        self.connect()
        #
        # TO DO : TABLES DROP AND CREATION
        #
        self.disconnect()

    def build_db(self, json):
        self.connect()
        #
        # TO DO : FOR ALL RECIPES DO THE INSERTS IN TABLES
        #         USING SUB-FUNCTION FOR EACH TYPES OF INSERTS IS A GOOD WAY IMO
        #
        self.disconnect()
