import time
import connection
import tools
import datetime

loopPeriod_s = 30
maxTaskTime_h = 72
maxAssignmentWaitingTime_h = 24
EC_OOM = 1


class Director:
    def __init__(self):
        self.conn, self.cursor = connection.connect()

    def DoWork(self):
        while True:
            start = time.time()

            # Check finished tasks.
            self.CheckFinishedTasks()

            # Check assignments with off-line workers.
            self.CheckAssignmentsWithOfflineWorkers()

            # Assign new tasks to workers.
            self.AssignNewTasksToWorkers()

            # Wait for new loop.
            end = time.time()
            lapse_s = end - start
            if lapse_s < loopPeriod_s:
                time.sleep(loopPeriod_s - lapse_s)

    def AssignNewTasksToWorkers(self):
        self.cursor.execute('select * from EXPERIMENTS where Finished=0 and Assigned=0')
        query_exp = self.cursor.fetchall()
        columns_exp = tools.get_columns_map(self.cursor)
        for exp in query_exp:
            memory = exp[columns_exp['InsufficientMemory_GB']]
            if memory is None:
                memory = 0
            valid_workers, queues = self.GetValidOnlineWorkersWithQueue(memory)

    def CheckFinishedTasks(self):
        self.cursor.execute('select * from EXECUTIONS where Processed=0')
        query = self.cursor.fetchall()
        columns = tools.get_columns_map(self.cursor)
        for row in query:
            if row[columns['exec.Success']] == 1:
                # Successful execution.
                self.cursor.execute('update EXPERIMENTS set Finished=1, FinalExecId=' + str(row[columns['exec.ExecId']])
                                    + ' where ExpId=' + str(row[columns['exec.ExpId']]))
                self.conn.commit()
            else:
                if row[columns['exec.ErrorCode']] == EC_OOM:
                    # Out Of Memory.
                    # Mark this amount of memory as insufficient in the experiment:
                    currentMemory = self.GetMemoryOfWorker(row[columns['exec.WorkerId']])
                    self.cursor.execute('update EXPERIMENTS set InsufficientMemory_GB=' + str(currentMemory) +
                                        ' where ExpId=' + str(row[columns['ExpId']]))
                    self.conn.commit()
                    # Check if there are workers with more memory:
                    self.cursor.execute('select WorkerId from REGISTERED_WORKERS where GPUMemory>' + str(currentMemory))
                    query_workers = self.cursor.fetchall()
                    if len(query_workers) > 0:
                        # There are online workers with more memory.
                        # Mark the experiment as not assigned.
                        self.cursor.execute('update EXPERIMENTS set Assigned=0 where ExpId=' +
                                            str(row[columns['ExpId']]))
                        self.conn.commit()
                    else:
                        # There aren't. Mark as failure.
                        self.cursor.execute('update EXPERIMENTS set Finished=1, FinalExecId=' + str(row[columns['exec.ExecId']])
                                            + ' where ExpId=' + str(row[columns['exec.ExpId']]))
                        self.conn.commit()
                else:
                    # Other error. Mark as failure.
                    self.cursor.execute('update EXPERIMENTS set Finished=1, FinalExecId=' + str(row[columns['exec.ExecId']])
                                        + ' where ExpId=' + str(row[columns['exec.ExpId']]))
                    self.conn.commit()

            # Finally, mark the execution as processed, in order not to process it again:
            self.cursor.execute('update EXECUTIONS set Processed=1 where ExecId=' + str(row[columns['exec.ExecId']]))
            self.conn.commit()

    def GetMemoryOfWorker(self, workerId):
        self.cursor.execute('select GPUMemory from REGISTERED_WORKERS where WorkerId=' + str(workerId))
        query = self.cursor.fetchall()
        return query[0][0]

    def GetValidOnlineWorkersWithQueue(self, memory):
        # self.cursor.execute('select * from ONLINE_WORKERS as ow join REGISTERED_WORKERS as rw '
        #                     'on ow.WorkerId=rw.WorkerId where rw.GPUMemory>' + str(memory))
        self.cursor.execute('select * from ONLINE_WORKERS as ow '
                            'join REGISTERED_WORKERS as rw on ow.WorkerId=rw.WorkerId '
                            'join ASSIGNMENTS as asg on ow.WorkerId=asg.WorkerId '
                            'where asg.Finished=0 and asg.Discarded=0 rw.GPUMemory>' + str(memory))
        query = self.cursor.fetchall()
        columns = tools.get_columns_map(self.cursor)
        valid_workers = []
        queues = {}
        for row in query:
            workerId = row[columns['ow.WorkerId']]
            if workerId in valid_workers:
                queues[workerId] += 1
            else:
                valid_workers.append(workerId)
                queues[workerId] = 1
        return valid_workers, queues

    def MarkExperimentAsFailure(self, expId, execId):
        self.cursor.execute('update EXPERIMENTS set Finished=1, FinalExecId=' + str(execId)
                            + ' where ExpId=' + str(expId))
        self.conn.commit()

    def CheckAssignmentsWithOfflineWorkers(self):
        self.cursor.execute('select asg.* from ASSIGNMENTS where InProgress=1 and Finished=0 and Discarded=0')
        query_asg = self.cursor.fetchall()
        columns_asg = tools.get_columns_map(self.cursor)
        self.cursor.execute('select * from ONLINE_WORKERS')
        query_ow = self.cursor.fetchall()
        columns_ow = tools.get_columns_map(self.cursor)
        for asg in query_asg:
            workerId = asg[columns_asg['WorkerId']]
            foundOnlineWorker = False
            for ow in query_ow:
                if ow[columns_ow['WorkerId']] == workerId:
                    foundOnlineWorker = True
                    break
            if not foundOnlineWorker:
                # De-assign.
                self.cursor.execute('update ASSIGNMENTS set InProgress=0, Discarded=1 where AssignmentId=' +
                                    str(asg[columns_asg['AssignmentId']]))
                self.cursor.execute('update EXPERIMENTS set Assigned=0 where ExpId=' + str(asg[columns_asg['ExpId']]))
                self.conn.commit()



