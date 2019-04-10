import time
import connection
import datetime
import tools
from threading import Thread
import os

loopPeriod_s = 30
experimentsFolder = ''  # TODO
workerInfoFileName = 'workerid.txt'


class ExecutionInfo:
    def __init__(self, expId, assignmentId, success, errorCode, execTime):
        self.expId = expId
        self.assignmentId = assignmentId
        self.success = success
        self.errorCode = errorCode
        self.execTime = execTime


class Worker:
    def __init__(self):
        self.working = False
        self.pendingFinishedTask = False
        self.conn, self.cursor = connection.connect()
        self.workerThread = None
        self.ReadInfo()

    def ReadInfo(self):
        thisDir = os.path.dirname(os.path.abspath(__file__))
        infoPath = os.path.join(thisDir, workerInfoFileName)
        if not os.path.exists(infoPath):
            print('Could not find worker info file in ' + str(infoPath))
            print('This probably mean that this worker has not been registered.\n'
                  'Please run RegisterWorker.py before trying to raise the worker.')
            raise Exception('InfoFile not found.')
        else:
            with open(infoPath, 'r') as fid:
                lines = fid.read().split('\n')
            lines = [line for line in lines if line != '']
            assert len(lines) == 1, 'There should be exactly one non-empty line in ' + workerInfoFileName + \
                                    ', but there are ' + str(len(lines))
            try:
                self.workerId = int(lines[0])
            except Exception as ex:
                print('Error parsing worker id.')
                print(ex)
                raise

    def DoWork(self):
        while True:
            start = time.time()

            try:
                # Send heartbit:
                self.SendHeartbit()

                # See if a task has been finished.
                if self.ReadPendingFinishedTaskFlag():
                    self.ProcessFinishedTask()

                # Search for new tasks, and execute them.
                if not self.ReadWorkingFlag():
                    self.SearchAndDoTasks()

            except Exception as ex:
                print('Error in main loop.')
                print(ex)

            # Wait for new loop.
            end = time.time()
            lapse_s = end - start
            if lapse_s < loopPeriod_s:
                time.sleep(loopPeriod_s - lapse_s)

    def DoTask(self, exp, columns, assignmentId):
        print('Start of working thread!')
        start = time.time()
        expId = exp[columns['ExpId']]
        CreateExperimentFolder(assignmentId)
        time.sleep(120)
        end = time.time()
        execTime = datetime.timedelta(seconds=start - end)
        success = True  # TODO
        errorCode = -1  # TODO
        execInfo = ExecutionInfo(expId, assignmentId, success, errorCode, execTime)
        self.SetExecInfo(execInfo)
        self.SetPendingFinishedTaskFlag(True)
        print('End of working thread.')
        return

    def ProcessFinishedTask(self):
        bestEpoch, loss, accuracy, iou, mAP = ReadExecutionResults()
        self.cursor.execute('insert into RESULTS (BestEpoch, Loss, Accuracy, IoU, mAP) values (?, ?, ?, ?, ?)',
                            (bestEpoch, loss, accuracy, iou, mAP))
        resultsId = self.cursor.execute("select SCOPE_IDENTITY()").fetchone()[0]
        execInfo = self.GetExecInfo()
        self.cursor.execute('insert into EXECUTIONS (ExpId, WorkerId, AssignmnetId, ResultsId, Success, ErrorCode, '
                            'ExecTime, Processed) values (?, ?, ?, ?, ?, ?, ?, ?)',
                            (execInfo.expId, self.workerId, execInfo.assignmentId, resultsId, execInfo.success,
                             execInfo.errorCode, execInfo.execTime, False))
        self.conn.commit()
        self.SetPendingFinishedTaskFlag(False)
        self.SetWorkingFlag(False)
        return

    def SearchAndDoTasks(self):
        print('Searching for new tasks...')
        self.cursor.execute('select * from ASSIGNMENTS where WorkerId=' + str(self.workerId) + ' and Discarded=0 and '
                            'Finished=0 and InProgress=0')
        query = self.cursor.fetchall()
        columns = tools.get_columns_map(self.cursor)
        # Look for the oldest assignment, in case there are several:
        oldest_assignment_pos = None
        oldest_assignment_date = None
        for pos in range(len(query)):
            row = query[pos]
            assignmentDate = row[columns['AssignmentDate']]
            if oldest_assignment_pos is None:
                oldest_assignment_pos = pos
                oldest_assignment_date = assignmentDate
            else:
                if assignmentDate < oldest_assignment_date:
                    oldest_assignment_pos = pos
                    oldest_assignment_date = assignmentDate
        assignmentId = query[oldest_assignment_pos][columns['AssignmentId']]
        expId = query[oldest_assignment_pos][columns['ExpId']]
        # Start a thread to do the selected assignment, and continue with the normal loop meanwhile:
        self.cursor.execute('select * from EXPERIMENTS where ExpId=' + str(expId))
        query_exp = self.cursor.fetchall()
        assert len(query_exp) == 1, 'There should be exactly one experiment with id ' + str(expId) + \
                                    'but there are ' + str(len(query_exp))
        exp = query_exp[0]
        columns_exp = tools.get_columns_map(self.cursor)
        self.workerThread = Thread(target=self.DoTask, args=(exp, columns_exp, assignmentId))
        self.cursor.execute('update ASSIGNMENTS set InProgress=1 where AssignmentId=' + str(assignmentId))
        self.conn.commit()
        self.SetWorkingFlag(True)
        print('Worker thread launched. Continue with normal loop.')
        return

    def SendHeartbit(self):
        print('Updating heartbit...')
        self.cursor.execute('select WorkerId from ONLINE_WORKERS where WorkerId=' + str(self.workerId))
        query = self.cursor.fetchall()
        if len(query) == 0:
            self.cursor.execute('insert into ONLINE_WORKERS (WorkerId, LastHeartbit) values (?, ?)',
                                (self.workerId, datetime.datetime.now()))
        else:
            self.cursor.execute('update ONLINE_WORKERS set LastHeartbit=? where WorkerId=' + str(self.workerId),
                                (datetime.datetime.now()))
        self.conn.commit()

    def ReadPendingFinishedTaskFlag(self):
        return self.pendingFinishedTask

    def SetPendingFinishedTaskFlag(self, value):
        self.pendingFinishedTask = value

    def ReadWorkingFlag(self):
        return self.working

    def SetWorkingFlag(self, value):
        self.working = value

    def SetExecInfo(self, value):
        self.execInfo = value

    def GetExecInfo(self):
        return self.execInfo


def CreateExperimentFolder(assignmentId):
    now = datetime.datetime.now()
    month_str = 'month' + str(now.month) + '_' + now.strftime('%B')
    monthDir = os.path.join(experimentsFolder, month_str)
    if not os.path.exists(monthDir):
        os.makedirs(monthDir)
    day_str = 'day' + str(now.day)
    dayDir = os.path.join(monthDir, day_str)
    if not os.path.exists(dayDir):
        os.makedirs(dayDir)
    expDir = os.path.join(dayDir, 'asg' + str(assignmentId))
    if os.path.exists(expDir):
        raise Exception('There is already a directory for assignment ' + str(assignmentId))
    else:
        os.makedirs(expDir)
    return expDir


def ReadExecutionResults():
    # TODO
    return 1, 1, 1, 1, 1