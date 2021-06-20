import constant as C
from src.utilty.sql_connection import MySqlConnection
import sql_utility as fn

mapping_col = {
    'Customer_Name': 'Customer',
    'Customer_Id': 'Customer_ID',
    'Open_Date': 'Customer_Open_Date',
    'Last_Consulted_Date': 'Last_Consulted_Date',
    'Vaccination_Id': 'Vaccination_Type',
    'Dr_Name': 'Doctor_Consulted',
    'State': 'State',
    'Country': 'Country',
    'Post_Code': 'Post_Code',
    'DOB': 'Date_of_Birth',
    'Is_Active': 'Active_Customer'
}


class HospitalChain:
    def __init__(self):
        self.mysql_db = MySqlConnection(C.HOST_SERVER, C.USER, C.PASSWORD, C.DATABASE)
        self.db_connection = self.mysql_db.get_connection()

    def bulk_insert(self, cursor, table_name, data, header):
        lenght = len(header)
        mapped_header = ", ".join([mapping_col.get(i) for i in header])
        val = ", ".join([i for i in [('%s ' * lenght).split(" ")][0]][0:-1])
        query2 = f"INSERT INTO {table_name} ({mapped_header}) VALUES ({val})"
        try:
            cursor.executemany(query2, data)
        except Exception as e:
            print(e)

    def insert_one(self, cursor, table_name, data, header=None):
        if header:
            pass
        else:
            header = "Customer, Customer_ID, Customer_Open_Date, Last_Consulted_Date, Vaccination_Type, Doctor_Consulted, State, Country, Post_Code, Date_of_Birth, Active_Customer"
            val = "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s"
            query = f"INSERT INTO {table_name} ({header}) VALUES ({val})"
            try:
                cursor.execute(query, data)
            except Exception as e:
                print(e)

    def save_data_to_staging_table(self):
        cursor = self.mysql_db.get_cursor()
        fn.drop_table(cursor, "staging_table")
        fn.create_table(cursor=cursor, table_name="staging_table")
        file = open(C.SAMPLE_FILE_PATH, 'r')
        file_content = file.readlines()
        values_list = [line.split() for line in file_content[0:]]
        values = [tuple(v[0].split("|")[2:]) for v in values_list]
        self.bulk_insert(cursor, "staging_table", values[1:], values[0])

    def split_tables_country_based(self):
        cursor = self.mysql_db.get_cursor()
        query = "select * from staging_table"
        cursor.execute(query)
        for row in cursor.fetchall():
            table_name = f"TABLE_{row[7]}"
            fn.create_table(cursor=cursor, table_name=table_name)
            self.insert_one(cursor, table_name, row)
        self.mysql_db.commit()


A = HospitalChain()
A.save_data_to_staging_table()
A.split_tables_country_based()
