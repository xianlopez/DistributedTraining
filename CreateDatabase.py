import connection

conn, cursor = connection.connect()

cursor.execute("CREATE TABLE EXECUTIONS ("
               "ExecId int NOT NULL PRIMARY KEY, "
               "ExpId int NOT NULL, "
               "WorkerId int NOT NULL, "
               "Success bit, "
               "ResultsId int, "
               "ErrorCode int, "
               "ExecTime_s float, "
               "DateStart datetime, "
               "AssignmentId int NOT NULL"
               ")")

cursor.execute("CREATE TABLE EXPERIMENTS ("
               "ExpId int NOT NULL PRIMARY KEY, "
               "ConfigPath varchar(255) NOT NULL, "
               "Finished bit NOT NULL, "
               "Assigned bit NOT NULL, "
               "FinalExecId int, "
               "Priority int NOT NULL, "
               "InsufficientMemory_GB int"
               ")")

cursor.execute("CREATE TABLE ASSIGNMENTS ("
               "AssignmentId int NOT NULL PRIMARY KEY, "
               "ExpId int NOT NULL, "
               "WorkerId int NOT NULL, "
               "InProgress bit NOT NULL, "
               "Completed bit NOT NULL, "
               "AssignmentDate datetime NOT NULL, "
               "TimedOut bit, "
               "Forgotten bit"
               ")")

cursor.execute("CREATE TABLE RESULTS ("
               "ResultsId int NOT NULL PRIMARY KEY, "
               "ExecId int NOT NULL, "
               "BestEpoch int, "
               "Loss float, "
               "Accuracy float, "
               "IoU float, "
               "mAP float)")

cursor.execute("CREATE TABLE REGISTERED_WORKERS ("
               "WorkerId int NOT NULL PRIMARY KEY, "
               "WorkerName varchar(255) NOT NULL, "
               "GPUName varchar(255) NOT NULL, "
               "GPUMemory varchar(255) NOT NULL"
               ")")

cursor.execute("CREATE TABLE ONLINE_WORKERS ("
               "WorkerId int NOT NULL, "
               "LastHeartbit datetime"
               ")")

cursor.execute("CREATE TABLE VALID_WORKERS ("
               "ExpId int NOT NULL, "
               "WorkerId int NOT NULL, "
               "Valid bit"
               ")")
cursor.execute("CREATE TABLE SETTINGS ("
               "Name varchar(255) NOT NULL, "
               "Value varchar(255) NOT NULL"
               ")")

conn.commit()


