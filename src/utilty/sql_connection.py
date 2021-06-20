import mysql.connector


class MySqlConnection:
    def __init__(self, host, user_name, password, db_name):
        self.mydb = mysql.connector.connect(
            host=host,
            user=user_name,
            password=password,
            database=db_name
        )

    def get_connection(self):
        return self.mydb

    def get_cursor(self):
        return self.mydb.cursor()

    def commit(self):
        self.mydb.commit()
