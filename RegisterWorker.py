import connection
import glob
import os


def RegisterNewWorker():
    thisDir = os.path.dirname(os.path.abspath(__file__))
    infoPath = os.path.join(thisDir, glob.workerInfoFileName)
    if os.path.exists(infoPath):
        print('There is already a file of a registered worker at ' + infoPath)
        return
    else:
        conn, cursor = connection.connect()
        WorkerName = input('Please insert a name for this worker:\n')
        GPUModel = input('Please insert the GPU model:\n')
        GPUMemory_GB = int(input('Please insert the GPU memory, in GB:\n'))
        cursor.execute('select * from REGISTERED_WORKERS where WorkerName=\'' + WorkerName + '\'')
        query = cursor.fetchall()
        if len(query) > 0:
            print('There is already a registered worker with name ' + WorkerName)
            return
        else:
            cursor.execute('insert into REGISTERED_WORKERS (WorkerName, GPUName, GPUMemory) values (?, ?, ?)',
                                (WorkerName, GPUModel, GPUMemory_GB))
            conn.commit()
            # workerId = cursor.execute("select SCOPE_IDENTITY()").fetchone()[0]
            cursor.execute('select WorkerId from REGISTERED_WORKERS where WorkerName=\'' + WorkerName + '\'')
            workerId = cursor.fetchone()[0]
            with open(infoPath, 'w') as fid:
                fid.write(str(workerId))


if __name__ == '__main__':
    RegisterNewWorker()