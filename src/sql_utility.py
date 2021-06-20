import metadata.header_type as ht


def create_table(cursor, table_name):
    query = f"CREATE TABLE {table_name} {ht.file_header}"
    try:
        cursor.execute(query)
    except Exception as e:
        print("table already exits")


def drop_table(cursor, table_name):
    query = f"DROP TABLE {table_name}"
    try:
        cursor.execute(query)
    except Exception as e:
        print(f"No Table Found As {table_name}")
