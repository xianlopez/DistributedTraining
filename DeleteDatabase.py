import connection
import credentials

conn, cursor = connection.connect()

cursor.execute('SET FOREIGN_KEY_CHECKS = 0')
conn.commit()

cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + credentials.Username + "'")
query = cursor.fetchall()
n_tables = len(query)
print('Found ' + str(n_tables) + ' custom tables.')
i = 0
for row in query:
    i += 1
    print(str(i) + ' / ' + str(n_tables) + '. Deleting ' + row[0] + '.')
    cursor.execute('DROP TABLE IF EXISTS ' + row[0])
conn.commit()

cursor.execute('SET FOREIGN_KEY_CHECKS = 1')
conn.commit()



