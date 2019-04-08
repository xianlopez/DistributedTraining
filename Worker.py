import time
import connection

loopPeriod_s = 30


class Worker:
    def __init__(self):
        self.working = False
        self.conn, self.cursor = connection.connect()

    def DoWork(self):
        while True:
            start = time.time()

            # Search for new tasks, and execute them.
            if not self.working:
                self.SearchAndDoTasks()

            # Send heartbit:
            self.SendHeartbit()

            # Wait for new loop.
            end = time.time()
            lapse_s = end - start
            if lapse_s < loopPeriod_s:
                time.sleep(loopPeriod_s - lapse_s)

    def SearchAndDoTasks(self):
        pass

    def SendHeartbit(self):
        pass