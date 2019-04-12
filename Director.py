import time
import connection
import tools
import datetime
import numpy as np
import glob

maxTaskTime_h = 72
maxAssignmentWaitingTime_h = 24


class Director:
    def __init__(self):
        self.conn, self.cursor = connection.connect()
        self.maxLapseWithoutHeartbeat = datetime.timedelta(seconds=2*self.heartbeatPeriod_s)
        self.ReadSettings()

    # Read settings from database.
    def ReadSettings(self):
        self.cursor.execute('select Value from SETTINGS where Name=\'HeartbeatPeriod_s\'')
        self.heartbeatPeriod_s = self.cursor.fetchone()[0][0]

    def DoWork(self):
        while True:
            start = time.time()

            # Send heartbeat:
            self.SendHeartbeat()

            # Set as offline the workers that have no heartbeat.
            self.UpdateOnlineWorkers()

            # Check finished tasks.
            self.CheckFinishedTasks()

            # Check assignments with off-line workers.
            self.CheckAssignmentsWithOfflineWorkers()

            # Assign new tasks to workers.
            self.AssignNewTasksToWorkers()

            # Wait for new loop.
            end = time.time()
            lapse_s = end - start
            if lapse_s < self.heartbeatPeriod_s:
                time.sleep(self.heartbeatPeriod_s - lapse_s)

    def SendHeartbeat(self):
        print('Updating heartbeat...')
        self.cursor.execute('update MASTER set LastHeartbeat=?', (datetime.datetime.now()))
        self.conn.commit()

    def UpdateOnlineWorkers(self):
        print('Reading workers heartbeats to update ONLINE_WORKERS table...')
        self.cursor.execute('select * from ONLINE_WORKERS')
        query = self.cursor.fetchall()
        columns = tools.get_columns_map(self.cursor)
        now = datetime.datetime.now()
        for row in query:
            workerId = row[columns['WorkerId']]
            lastHearbit = row[columns['LastHeartbeat']]
            lapse = now - lastHearbit
            if lapse > self.maxLapseWithoutHeartbeat:
                print('Worker ' + str(workerId) + ' is offline.')
                self.cursor.execute('delete from ONLINE_WORKERS where WorkerId=' + str(workerId))
                self.conn.commit()
                print('Deleted from online table.')


    def AssignNewTasksToWorkers(self):
        print('Assigning new tasks to workers...')
        self.cursor.execute('select * from EXPERIMENTS where Finished=0 and Assigned=0')
        query_exp = self.cursor.fetchall()
        columns_exp = tools.get_columns_map(self.cursor)
        if len(query_exp) == 0:
            print('There are no new tasks.')
        else:
            for exp in query_exp:
                expId = exp[columns_exp['IdExp']]
                print('Experiment ' + str(expId))
                memory = exp[columns_exp['InsufficientMemory_GB']]
                if memory is None:
                    memory = 0
                valid_workers, queues = self.GetValidOnlineWorkersWithQueue(memory)
                selected_worker = self.SelectoWorkerToAssignTask(valid_workers, queues)
                self.cursor.execute('insert into ASSIGNMENTS (ExpId, WorkerId, '
                                    'InProgress, Finished, Discarded, AssignmentDate) values (?, ?, ?, ?, ?, ?)',
                                    (expId, selected_worker, False, False, False, datetime.datetime.now()))
                self.cursor.execute('update EXPERIMENTS set Assigned=1 where IdExp=' + str(expId))
                self.conn.commit()
                print('Assigned to ' + str(selected_worker))

    def CheckFinishedTasks(self):
        print('Checking finished tasks...')
        self.cursor.execute('select * from EXECUTIONS where Processed=0')
        query = self.cursor.fetchall()
        columns = tools.get_columns_map(self.cursor)
        for row in query:
            if row[columns['exec.Success']] == 1:
                # Successful execution.
                self.cursor.execute('update EXPERIMENTS set Finished=1, FinalExecId=' + str(row[columns['exec.ExecId']])
                                    + ' where ExpId=' + str(row[columns['exec.ExpId']]))
            else:
                if row[columns['exec.ErrorCode']] == glob.EC_OOM:
                    # Out Of Memory.
                    # Mark this amount of memory as insufficient in the experiment:
                    currentMemory = self.GetMemoryOfWorker(row[columns['exec.WorkerId']])
                    self.cursor.execute('update EXPERIMENTS set InsufficientMemory_GB=' + str(currentMemory) +
                                        ' where ExpId=' + str(row[columns['ExpId']]))
                    # Check if there are workers with more memory:
                    self.cursor.execute('select WorkerId from REGISTERED_WORKERS where GPUMemory>' + str(currentMemory))
                    query_workers = self.cursor.fetchall()
                    if len(query_workers) > 0:
                        # There are online workers with more memory.
                        # Mark the experiment as not assigned.
                        self.cursor.execute('update EXPERIMENTS set Assigned=0 where ExpId=' +
                                            str(row[columns['ExpId']]))
                    else:
                        # There aren't. Mark as failure.
                        self.cursor.execute('update EXPERIMENTS set Finished=1, FinalExecId=' + str(row[columns['exec.ExecId']])
                                            + ' where ExpId=' + str(row[columns['exec.ExpId']]))
                else:
                    # Other error. Mark as failure.
                    self.cursor.execute('update EXPERIMENTS set Finished=1, FinalExecId=' + str(row[columns['exec.ExecId']])
                                        + ' where ExpId=' + str(row[columns['exec.ExpId']]))

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

    def SelectoWorkerToAssignTask(self, workers, queues):
        if len(workers) == 0:
            raise Exception('No workers to select.')
        min_queue = np.inf
        for wk in workers:
            min_queue = np.minimum(min_queue, queues[wk])
        selected_worker = None
        for wk in workers:
            if queues[wk] == min_queue:
                selected_worker = wk
                break
        if selected_worker is None:
            raise Exception('Error selecting worker.')
        else:
            return selected_worker

    def CheckAssignmentsWithOfflineWorkers(self):
        print('Checking assignments with offline workers...')
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
                asg_id = asg[columns_asg['AssignmentId']]
                print('Worker of assignment ' + str(asg_id) + ' is offline.')
                self.cursor.execute('update ASSIGNMENTS set InProgress=0, Discarded=1 where AssignmentId=' +
                                    str(asg_id))
                self.cursor.execute('update EXPERIMENTS set Assigned=0 where ExpId=' + str(asg[columns_asg['ExpId']]))
                self.conn.commit()
                print('De-assigned.')



