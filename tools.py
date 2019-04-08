def get_columns_map(cursor):
    columns = {}
    for i in range(len(cursor.description)):
        col_name = cursor.description[i][0]
        columns[col_name] = i
    return columns