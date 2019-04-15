import connection

conn, cursor = connection.connect()

cursor.execute('select * from SETTINGS')
query = cursor.fetchall()

cursor.execute('select max(ResultsId) from RESULTS')
query = cursor.fetchone()


