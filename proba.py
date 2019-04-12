import time
import subprocess
import datetime

print('Start of working thread!')
start = time.time()
try:
    timeout_s = 10
    cmd = 'python C:\\development\\DistributedTraining\\executor.py'
    print('Before starting subprocess')
    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, timeout=timeout_s)
    print('After subprocess')
    success = True
except subprocess.CalledProcessError as ex_call:
    print('Non-zero exit executing the task.')
    print(ex_call)
    success = False
except subprocess.TimeoutExpired as ex_timeout:
    print('Timeout expired executing the task.')
    print(ex_timeout)
    success = False
except Exception as ex:
    print('Unexpected error executing experiment.')
    print(ex)
    success = False
end = time.time()
print('start = ' + str(start))
print('end = ' + str(end))
print('end - start = ' + str(end - start))
execTime = datetime.timedelta(seconds=end-start)
print('execTime')
print(execTime)
print('End of working thread.')