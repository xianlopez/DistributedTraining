import time
import connection
import tools
import datetime

loopPeriod_s = 30
maxTaskTime_h = 72
maxAssignmentWaitingTime_h = 24


class Director:
    def __init__(self):
        self.conn, self.cursor = connection.connect()

    def DoWork(self):
        while True:
            start = time.time()

            # Assign new tasks to workers.
            self.AssignNewTasksToWorkers()

            # Check finished tasks.
            self.CheckFinishedTasks()

            # Check timed-out tasks.
            self.CheckTimedoutTasks()

            # Check forgotten tasks.
            self.CheckForgottenTasks()

            # Wait for new loop.
            end = time.time()
            lapse_s = end - start
            if lapse_s < loopPeriod_s:
                time.sleep(loopPeriod_s - lapse_s)

    def AssignNewTasksToWorkers(self):
        self.cursor.execute('select * from EXPERIMENTS where Finished=0 and Assigned=0')
        query = self.cursor.fetchall()

    def CheckFinishedTasks(self):
        self.cursor.execute('select * from EXECUTIONS as exec '
                            'join EXPERIMENTS as exp on exec.IdExp = exp.IdExp '
                            'join ASSIGNMENTS as asg on exec.IdExp = asg.IdExp '
                            'where asg.Completed=1 and asg.TimedOut=0 and asg.Forgotten=0')
        query = self.cursor.fetchall()
        columns = tools.get_columns_map(self.cursor)
        for row in query:
            if row[columns['exec.Success']] == 1:
                self.cursor.execute('update EXPERIMENTS set Finished=1, FinalExecId=' + str(row[columns['exec.ExecId']])
                                    + ' where ExpId=' + str(row[columns['exec.ExpId']]))
            else:
                # TODO: Reassign or give up
                pass

    def CheckTimedoutTasks(self):
        now = datetime.datetime.now()
        self.cursor.execute('select * from ASSIGNMENTS where InProgress=1 and Completed=0 and TimedOut=0')
        query = self.cursor.fetchall()
        columns = tools.get_columns_map(self.cursor)
        for row in query:
            timedelta = now - row[columns['AssignmentDate']]
            if timedelta > datetime.timedelta(hours=maxTaskTime_h):
                self.cursor.execute('update ASSIGNMENTS set TimedOut=1 where IdAssignment=' + str(row[columns['AssignmentId']]))
                self.conn.commit()
                self.ReassignExperiment(row[columns['ExpId']])

    def CheckForgottenTasks(self):
        now = datetime.datetime.now()
        self.cursor.execute('select * from ASSIGNMENTS where InProgress=0 and Completed=0 and Forgotten=0')
        query = self.cursor.fetchall()
        columns = tools.get_columns_map(self.cursor)
        for row in query:
            timedelta = now - row[columns['AssignmentDate']]
            if timedelta > datetime.timedelta(hours=maxAssignmentWaitingTime_h):
                self.cursor.execute('update ASSIGNMENTS set Forgotten=1 where IdAssignment=' + str(row[columns['AssignmentId']]))
                self.conn.commit()
                self.ReassignExperiment(row[columns['ExpId']])

    def ReassignExperiment(self, expId):
        pass