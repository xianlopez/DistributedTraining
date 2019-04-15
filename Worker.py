import time
import connection
import datetime
import tools
from threading import Thread
import os
import tensorflow as tf
import glob
import subprocess

class ExecutionInfo:
    def __init__(self, expId, success, errorCode, execTime_h):
        self.expId = expId
        self.success = success
        self.errorCode = errorCode
        self.execTime_h = execTime_h


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
        self.thisDir = os.path.dirname(os.path.abspath(__file__))
        self.experimentsFolder = os.path.join(self.thisDir, 'experiments')
        if not os.path.exists(self.experimentsFolder):
            os.makedirs(self.experimentsFolder)
        self.ReadInfo()
        self.ReadSettings()

    def ReadInfo(self):
        infoPath = os.path.join(self.thisDir, glob.workerInfoFileName)
        if not os.path.exists(infoPath):
            print('Could not find worker info file in ' + str(infoPath))
            print('This probably mean that this worker has not been registered.\n'
                  'Please run RegisterWorker.py before trying to raise the worker.')
            raise Exception('InfoFile not found.')
        else:
            with open(infoPath, 'r') as fid:
                lines = fid.read().split('\n')
            lines = [line for line in lines if line != '']
            assert len(lines) == 1, 'There should be exactly one non-empty line in ' + glob.workerInfoFileName + \
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

    # Read settings from database.
    def ReadSettings(self):
        print('Reading settings from database...')
        self.cursor.execute('select Value from SETTINGS where Name=\'HeartbeatPeriod_s\'')
        self.heartbeatPeriod_s = float(self.cursor.fetchone()[0])
        print('HeartbeatPeriod_s = ' + str(self.heartbeatPeriod_s))
        self.cursor.execute('select Value from SETTINGS where Name=\'MaxExecTime_h\'')
        self.maxExecTime_h = float(self.cursor.fetchone()[0])
        print('MaxExecTime_h = ' + str(self.maxExecTime_h))
        print('All settings read.')

    def DoWork(self):
        while True:
            start = time.time()

            try:
                # Send heartbeat:
                self.SendHeartbeat()

                # See if a task has been finished.
                if self.GetPendingFinishedTask():
                    self.ProcessFinishedTask()

                # Search for new tasks, and execute them.
                if not self.working:
                    self.SearchAndDoTasks()

            except Exception as ex:
                print('Error in main loop.')
                print(ex)
                raise

            # Wait for new loop.
            end = time.time()
            lapse_s = end - start
            if lapse_s < self.heartbeatPeriod_s:
                print('Waiting until next iteration...')
                time.sleep(self.heartbeatPeriod_s - lapse_s)

    def DoTask(self, exp, columns):
        print('Start of working thread!')
        start = time.time()
        expId = exp[columns['ExpId']]
        expDir = self.CreateExperimentFolder(self.GetCurrAsgId())
        try:
            cmd = 'python C:\\development\\DistributedTraining\\executor.py'  # TODO
            maxExecTime_s = self.maxExecTime_h * 3600
            _ = subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=maxExecTime_s)
            success = True
            errorCode = glob.EC_NOERROR
        except subprocess.CalledProcessError as ex_call:
            print('Non-zero exit executing the task.')
            print(ex_call)
            success = False
            errorCode = glob.EC_NONZERO
        except subprocess.TimeoutExpired as ex_timeout:
            print('Timeout expired executing the task.')
            print(ex_timeout)
            success = False
            errorCode = glob.EC_TIMEOUT
        except tf.errors.ResourceExhaustedError:
            print('Error executing experiment: Out Of Memory.')
            success = False
            errorCode = glob.EC_OOM
        except Exception as ex:
            print('Unexpected error executing experiment.')
            print(ex)
            success = False
            errorCode = glob.EC_OTHER
        end = time.time()
        execTime = datetime.timedelta(seconds=end-start)
        execTime_h = execTime.seconds / 3600.0
        execInfo = ExecutionInfo(expId, success, errorCode, execTime_h)
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
            self.cursor.execute('select max(ResultsId) from RESULTS')
            query = self.cursor.fetchone()
            if query[0] is None:
                resultsId = 1
            else:
                maxResultsId = int(self.cursor.fetchone()[0])
                resultsId = maxResultsId + 1
            self.cursor.execute('insert into RESULTS (ResultsId, BestEpoch, Loss, Accuracy, IoU, mAP) values (?, ?, ?, ?, ?)',
                                (resultsId, resultsSummary.bestEpoch, resultsSummary.loss, resultsSummary.accuracy,
                                 resultsSummary.iou, resultsSummary.mAP))
            # resultsId = self.cursor.execute("select SCOPE_IDENTITY()").fetchone()[0]
        else:
            resultsId = None
        self.cursor.execute('insert into EXECUTIONS (ExpId, WorkerId, AssignmnetId, ResultsId, Success, ErrorCode, '
                            'ExecTime_h, Processed) values (?, ?, ?, ?, ?, ?, ?, ?)',
                            (execInfo.expId, self.workerId, execInfo.assignmentId, resultsId, execInfo.success,
                             execInfo.errorCode, execInfo.execTime_h, False))
        self.conn.commit()
        self.SetPendingFinishedTask(False)
        self.working = False
        return

    def SearchAndDoTasks(self):
        print('Searching for new tasks...')
        self.cursor.execute('select * from ASSIGNMENTS where WorkerId=' + str(self.workerId) + ' and Discarded=0 and '
                            'Finished=0 and InProgress=0')
        query = self.cursor.fetchall()
        columns = tools.get_columns_map(self.cursor)
        # Look for the oldest assignment, in case there are several:
        if len(query) > 0:
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
        else:
            print('There are no pending assignments for this worker.')
            return

    def SendHeartbeat(self):
        print('Updating heartbeat...')
        self.cursor.execute('select WorkerId from ONLINE_WORKERS where WorkerId=' + str(self.workerId))
        query = self.cursor.fetchall()
        if len(query) == 0:
            self.cursor.execute('insert into ONLINE_WORKERS (WorkerId, LastHeartbeat) values (?, ?)',
                                (self.workerId, datetime.datetime.now()))
        else:
            self.cursor.execute('update ONLINE_WORKERS set LastHeartbeat=? where WorkerId=' + str(self.workerId),
                                (datetime.datetime.now()))
        self.conn.commit()

    def CreateExperimentFolder(self, assignmentId):
        now = datetime.datetime.now()
        month_str = 'month' + str(now.month) + '_' + now.strftime('%B')
        monthDir = os.path.join(self.experimentsFolder, month_str)
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


def ReadExecutionResults(expDir):
    trainReportPath = os.path.join(expDir, 'train_report.csv')
    with open (trainReportPath, 'r') as fid:
        lines = fid.read().split('\n')
    lines = [line for line in lines if line != '']
    lines = lines[1:]  # Skip the first line, which are headers.
    best_loss = None
    best_line_idx = None
    for i in range(len(lines)):
        line = lines[i]
        line_split = line.split(',')
        loss = line_split[5]
        if best_loss is None:
            best_loss = loss
            best_line_idx = i
        else:
            if loss < best_loss:
                best_loss = loss
                best_line_idx = i
    line_split = lines[best_line_idx].split(',')
    bestEpoch = line_split[0]
    loss = line_split[5]
    accuracy = line_split[6]
    iou = line_split[7]
    mAP = line_split[8]
    return bestEpoch, loss, accuracy, iou, mAP


def StartWorker():
    worker = Worker()
    worker.DoWork()


if __name__ == '__main__':
    StartWorker()






