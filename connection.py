import pyodbc
import credentials


def connect():
    conn = pyodbc.connect(
        r'DRIVER={' + credentials.Driver + '};'
        r'SERVER=' + credentials.Server + ';'
        r'DATABASE=' + credentials.Database + ';'
        r'UID=' + credentials.Username + ';'
        r'PWD=' + credentials.Password
        )
    cursor = conn.cursor()
    return conn, cursor
