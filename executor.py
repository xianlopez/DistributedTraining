import time
import numpy as np

def work():
    time_to_sleep_s = int(np.round(120 + 5 * np.random.normal()))
    time.sleep(time_to_sleep_s)

if __name__ == '__main__':
    work()