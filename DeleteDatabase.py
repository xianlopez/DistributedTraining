import connection
import credentials

conn, cursor = connection.connect()

cursor.execute('SET FOREIGN_KEY_CHECKS = 0')
conn.commit()

cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + credentials.Username + "'")
query = cursor.fetchall()
for row in query:
    cursor.execute('DROP TABLE IF EXISTS ' + row[0])
conn.commit()

cursor.execute('SET FOREIGN_KEY_CHECKS = 1')
conn.commit()



