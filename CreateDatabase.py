import connection

conn, cursor = connection.connect()

cursor.execute("CREATE TABLE EXECUTIONS (ExecId int, ExpId int, WorkerId int, Success bit, ResultsId int, "
               "ErrorCode int, ExecTime_s float, DateStart datetime, AssignmentId int)")
cursor.execute("CREATE TABLE EXPERIMENTS (ExpId int, ConfigPath varchar(255), Finished bit, "
               "Assigned bit, FinalExecId int)")
cursor.execute("CREATE TABLE ASSIGNMENTS (AssignmentId int, ExpId int, WorkerId int, InProgress bit, "
               "Completed bit, AssignmentDate datetime, TimedOut bit, Forgotten bit)")
cursor.execute("CREATE TABLE RESULTS (ExecId int, BestEpoch int, Loss float, Accuracy float, IoU float, mAP float)")
cursor.execute("CREATE TABLE REGISTERED_WORKERS (WorkerId int, WorkerName varchar(255), GPUName varchar(255), "
               "GPUMemory varchar(255))")
cursor.execute("CREATE TABLE ONLINE_WORKERS (WorkerId int, LastHeartbit datetime)")
conn.commit()


