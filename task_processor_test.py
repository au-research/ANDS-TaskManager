import unittest
import pprint
import myconfig
import os
import time
import sys
from task_processor_daemon import TasksManagerDaemon


if __name__ == '__main__':
    sys.path.append(myconfig.run_dir + 'task_handlers')
    td = TasksManagerDaemon(myconfig.run_dir + 'daemon.pid')
    td.initalise()
    taskRow = {}
    taskRow[0] = 124227
    taskRow[1] = 'Harvester initiated import - Site Survey Data Bank(42088) - FBEA060C6FABCAD76FB3E10286E49543A845B008'
    taskRow[2] = 'PHPSHELL'
    taskRow[3] = "PENDING"
    taskRow[4] = ""
    taskRow[5] = '2019-10-08 10:42:20'
    taskRow[6] ='0000-00-00 00:00:00'
    taskRow[7] = '2019-10-08 10:42:20'
    taskRow[8] = 2
    taskRow[9] = 'ONCE'
    taskRow[10] = 'class=import&ds_id=42088&batch_id=FBEA060C6FABCAD76FB3E10286E49543A845B008&harvest_id=302&source=harvester'
    time.sleep(2)
    success = td.queueTask(taskRow)
    print(success)
    time.sleep(2)
    success = td.queueTask(taskRow)
    print(success)
    td.manageTasks()
    time.sleep(2)
    success = td.queueTask(taskRow)
    print(success)