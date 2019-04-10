import time
import connection
import datetime
import tools
from threading import Thread
import os
import tensorflow as tf

loopPeriod_s = 30
experimentsFolder = ''  # TODO
workerInfoFileName = 'workerid.txt'
maxExecTime_h = 72

# TODO: Share with director
EC_NOERROR = -1
EC_OOM = 1
EC_OTHER = 2


class ExecutionInfo:
    def __init__(self, expId, success, errorCode, execTime):
        self.expId = expId
        self.success = success
        self.errorCode = errorCode
        self.execTime = execTime


class ResultsSummary:
    def __init__(self, bestEpoch, loss, accuracy, iou, mAP):
        self.bestEpoch = bestEpoch
        self.loss = loss
        self.accuracy = accuracy
        self.iou = iou
        self.mAP = mAP


class Worker:
    def __init__(self):
        self.resultsSummary = None
        self.execInfo = None
        self.working = False
        self.currAsgId = None
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
            # Parse id number:
            try:
                self.workerId = int(lines[0])
            except Exception as ex:
                print('Error parsing worker id.')
                print(ex)
                raise
            # Check there is a registered worker in database with this id:
            self.cursor.execute('select WorkerId from REGISTERED_WORKERS where WorkerId=' + str(self.workerId))
            query = self.cursor.fetchall()
            assert len(query) == 1, 'There should be exactly one worker registered in database with id ' + \
                                    str(self.workerId) + ', but there are ' + str(len(query))

    def DoWork(self):
        while True:
            start = time.time()

            try:
                # Send heartbit:
                self.SendHeartbit()

                # Check timed-out tasks in progress:
                # if self.working:
                #     self.CheckTimedoutTasks()

                # See if a task has been finished.
                if self.GetPendingFinishedTask():
                    self.ProcessFinishedTask()

                # Search for new tasks, and execute them.
                if not self.working:
                    self.SearchAndDoTasks()

            except Exception as ex:
                print('Error in main loop.')
                print(ex)

            # Wait for new loop.
            end = time.time()
            lapse_s = end - start
            if lapse_s < loopPeriod_s:
                time.sleep(loopPeriod_s - lapse_s)

    def DoTask(self, exp, columns):
        print('Start of working thread!')
        start = time.time()
        expId = exp[columns['ExpId']]
        expDir = CreateExperimentFolder(self.GetCurrAsgId())
        try:
            time.sleep(120)  # TODO
            success = True
            errorCode = EC_NOERROR
        except tf.errors.ResourceExhaustedError:
            print('Error executing experiment: Out Of Memory.')
            success = False
            errorCode = EC_OOM
        except Exception as ex:
            print('Unexpected error executing experiment.')
            print(ex)
            success = False
            errorCode = EC_OTHER
        end = time.time()
        execTime = datetime.timedelta(seconds=start-end)
        execInfo = ExecutionInfo(expId, success, errorCode, execTime)
        if success:
            bestEpoch, loss, accuracy, iou, mAP = ReadExecutionResults(expDir)
            resultsSummary = ResultsSummary(bestEpoch, loss, accuracy, iou, mAP)
        else:
            resultsSummary = None
        self.SetResultsSummary(resultsSummary)
        self.SetExecInfo(execInfo)
        self.SetPendingFinishedTask(True)
        print('End of working thread.')
        return

    def ProcessFinishedTask(self):
        execInfo = self.GetExecInfo()
        if execInfo.success:
            resultsSummary = self.GetResultsSummary()
            self.cursor.execute('insert into RESULTS (BestEpoch, Loss, Accuracy, IoU, mAP) values (?, ?, ?, ?, ?)',
                                (resultsSummary.bestEpoch, resultsSummary.loss, resultsSummary.accuracy,
                                 resultsSummary.iou, resultsSummary.mAP))
            resultsId = self.cursor.execute("select SCOPE_IDENTITY()").fetchone()[0]
        else:
            resultsId = None
        self.cursor.execute('insert into EXECUTIONS (ExpId, WorkerId, AssignmnetId, ResultsId, Success, ErrorCode, '
                            'ExecTime, Processed) values (?, ?, ?, ?, ?, ?, ?, ?)',
                            (execInfo.expId, self.workerId, execInfo.assignmentId, resultsId, execInfo.success,
                             execInfo.errorCode, execInfo.execTime, False))
        self.conn.commit()
        self.SetPendingFinishedTask(False)
        self.working = False
        return

    def CheckTimedoutTasks(self):
        print('Checking if running task is timed-out...')
        # self.cursor.execute('select * from ASSIGNMENTS where AssignmentId=' + str(self.GetCurrAsgId()))
        # query = self.cursor.fetchall()
        # columns = tools.get_columns_map(self.cursor)
        # dateTaken = query[0][columns['TakenDate']]
        # timeDelta = datetime.datetime.now() - dateTaken
        # if timeDelta > datetime.timedelta(hours=maxExecTime_h):
        #     print('Task timed-out!')
        #     TODO

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
        self.cursor.execute('update ASSIGNMENTS set InProgress=1 and TakenDate=? where AssignmentId='
                            + str(assignmentId), datetime.datetime.now())
        self.conn.commit()
        self.working = True
        self.currAsgId = assignmentId
        self.workerThread = Thread(target=self.DoTask, args=(exp, columns_exp))
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

    def SetPendingFinishedTask(self, value):
        self.pendingFinishedTask = value

    def GetPendingFinishedTask(self):
        return self.pendingFinishedTask

    def SetExecInfo(self, value):
        self.execInfo = value

    def GetExecInfo(self):
        return self.execInfo

    def SetResultsSummary(self, value):
        self.resultsSummary = value

    def GetResultsSummary(self):
        return self.resultsSummary

    def GetCurrAsgId(self):
        if not self.working:
            raise Exception('It is not allowed to get currAsgId if working=False.')
        else:
            return self.currAsgId


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


def ReadExecutionResults(expDir):
    # TODO
    return 1, 1, 1, 1, 1