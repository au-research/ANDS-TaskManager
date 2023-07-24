# harvester daemon script in python
# for ANDS registry
# Author: u4187959
# created 12/05/2014
#

import atexit
import os
import sys
import threading
import urllib.parse as urlparse
from datetime import datetime
import time
from collections import deque
from utils import web_server
from utils.Logger import Logger
import myconfig
from utils.Database import DataBase

sys.path.append(myconfig.run_dir)
os.getcwd()


class TasksManager:
    __scheduler = False
    logger = False
    database = False
    __queuedTasks = deque()
    __runningTasks = {}
    __running_threads = {}
    __queued_running_task_id_to_ds_id_dict = {}
    __lastLogCounter = 999
    __taskHandlersDefinitionFile = False
    __startUpTime = None
    __startedTasksCount = 0
    __completedTasksCount = 0
    __erroredTasksCount = 0
    __stoppedTasksCount = 0

    def __init__(self):
        self.__lastLogCount = None

    def handle_exception(self, tasksID, exception):
        taskStatus = 'STOPPED'
        self.__erroredTasksCount += 1
        eMessage = repr(exception).replace("'", "").replace('"', "")
        attempts = 0
        while attempts < 3:
            try:
                conn = self.database.get_connection()
                cur = conn.cursor()
                cur.execute("UPDATE %s SET `status` ='%s', `message` = '%s' where `id` = %s" % (
                    myconfig.tasks_table, taskStatus, eMessage, str(tasksID)))
                conn.commit()
                cur.close()
                del cur
                conn.close()
                break
            except Exception as e:
                attempts += 1
                time.sleep(5)
                self.logger.log_message("(handleException) %s, Retry: %d" % (str(repr(e)), attempts), "ERROR")

    def queue_task(self, taskRow):

        taskInfo = {}
        taskType = 'POKE'
        taskID = taskRow[0]
        if taskRow[2] is not None:
            taskType = taskRow[2]
        taskHandler = str(taskType + 'TaskHandler')
        taskInfo['task_id'] = taskRow[0]
        taskInfo['name'] = taskRow[1]
        taskInfo['type'] = taskType
        taskInfo['status'] = taskRow[3]
        taskInfo['message'] = taskRow[4]
        taskInfo['date_added'] = taskRow[5]
        taskInfo['next_run'] = taskRow[6]
        taskInfo['last_run'] = taskRow[7]
        taskInfo['priority'] = taskRow[8]
        taskInfo['frequency'] = taskRow[9]
        taskInfo['params'] = taskRow[10]

        urlParams = urlparse.parse_qs(taskInfo['params'])
        ds_id = None
        try:
            if isinstance(urlParams['ds_id'], list):
                ds_id = urlParams['ds_id'][0]
                taskInfo['ds_id'] = ds_id
        except KeyError:
            pass

        if ds_id is not None and len(self.__queued_running_task_id_to_ds_id_dict) > 0:
            for taskid, dsid in self.__queued_running_task_id_to_ds_id_dict.items():
                if dsid == ds_id:
                    self.logger.log_message(
                        "POSTPONING QUEUEING tasks ID:%s, (Datasource: %s is busy)" % (str(taskID), ds_id), "DEBUG")
                    return False
        try:
            self.logger.log_message("QUEUEING tasks ID:%s, type %s, ds_id: %s" % (str(taskRow[0]), taskRow[2], ds_id),
                                   "DEBUG")
            task_handler_module = __import__(taskHandler, globals={}, locals={}, fromlist=[], level=0)
            class_ = getattr(task_handler_module, taskHandler)
            taskProcessor = class_(taskInfo, self.logger, self.database)
            self.__queued_running_task_id_to_ds_id_dict[taskID] = ds_id
            self.__queuedTasks.append(taskProcessor)
        except ImportError as e:
            self.logger.log_message(e, "ERROR")
            self.handle_exception(taskID, e)
        return True

    def manage_tasks(self):
        # self.reportToRegistry()
        self.check_for_pending_tasks()
        taskID = None
        # if max hasn't reached add more harvests that are queued
        if len(self.__queuedTasks) > 0 and len(self.__runningTasks) < myconfig.max_thread_count:
            while len(self.__runningTasks) < myconfig.max_thread_count and len(self.__queuedTasks) > 0:
                try:
                    taskProcessor = self.__queuedTasks.pop()
                    taskID = taskProcessor.get_task_id()
                    if taskProcessor.get_status() == "PENDING":
                        self.__runningTasks[taskID] = taskProcessor
                        t = threading.Thread(target=taskProcessor.run_task)
                        self.__running_threads[taskID] = t
                        t.start()
                        self.__startedTasksCount = self.__startedTasksCount + 1
                    elif taskProcessor.is_completed or taskProcessor.is_stopped:
                        del self.__queued_running_task_id_to_ds_id_dict[taskID]
                except KeyError:
                    self.logger.log_message("tasksID %s already scheduled" % str(taskID), "ERROR")
                    del self.__queued_running_task_id_to_ds_id_dict[taskID]
        self.print_logs(int(len(self.__runningTasks)) + int(len(self.__queuedTasks)))
        # clean up completed harvests
        if len(self.__runningTasks) > 0:
            for taskID in list(self.__runningTasks):
                try:
                    taskProcessor = self.__runningTasks[taskID]
                    if taskProcessor.is_completed():
                        self.__completedTasksCount = self.__completedTasksCount + 1
                        del taskProcessor
                        if self.__running_threads[taskID].isAlive():
                            del self.__running_threads[taskID]
                        del self.__runningTasks[taskID]
                        del self.__queued_running_task_id_to_ds_id_dict[taskID]
                    elif taskProcessor.is_stopped():
                        self.__stoppedTasksCount = self.__stoppedTasksCount + 1
                        del taskProcessor
                        if self.__running_threads[taskID].isAlive():
                            del self.__running_threads[taskID]
                        del self.__runningTasks[taskID]
                        del self.__queued_running_task_id_to_ds_id_dict[taskID]
                except KeyError:
                    self.logger.log_message("tasksID %s already deleted" % str(taskID), "ERROR")
                    del self.__queued_running_task_id_to_ds_id_dict[taskID]

    def get_current_tasks_ids(self):
        currentTasksIDs = ''
        first = True
        for tasksID in list(self.__runningTasks):
            if first:
                first = False
                currentTasksIDs = str(tasksID)
            else:
                currentTasksIDs = currentTasksIDs + "," + str(tasksID)
        for taskHandler in list(self.__queuedTasks):
            if first:
                currentTasksIDs = str(taskHandler.get_task_id())
            else:
                currentTasksIDs = currentTasksIDs + "," + str(taskHandler.get_task_id())
        return currentTasksIDs

    def check_for_pending_tasks(self):
        try:
            conn = self.database.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT `id` FROM " + myconfig.tasks_table + " where `status` = 'RUNNING'; ")
            if cur.rowcount > 0:
                self.logger.log_message("RUNNING (according to tasks table): %s" % str(cur.rowcount), "DEBUG")
            if len(self.__queuedTasks) < 10 and cur.rowcount < 5:
                currentTasks = self.get_current_tasks_ids()
                if len(currentTasks) > 0:
                    self.logger.log_message("currentTasks: %s" % currentTasks, "DEBUG")
                    cur.execute("SELECT * FROM " + myconfig.tasks_table
                                + " where `status` = 'PENDING' and `type` = 'PHPSHELL' and (`next_run` is null or "
                                  "`next_run` <=timestamp('" + str(datetime.now())
                                + "')) AND id NOT IN (" + currentTasks + ") ORDER BY `date_added` ASC LIMIT "
                                + str(10 - len(self.__queuedTasks)) + ";")
                else:
                    cur.execute("SELECT * FROM " + myconfig.tasks_table
                                + " where `status` = 'PENDING' and `type` = 'PHPSHELL' and (`next_run` is null or "
                                  "`next_run` <=timestamp('" + str(datetime.now())
                                + "')) ORDER BY `date_added` ASC LIMIT " + str(10 - len(self.__queuedTasks)) + ";")
                if cur.rowcount > 0:
                    self.logger.log_message("Add PENDING Tasks to queue (Count:%s)" % str(cur.rowcount), "DEBUG")
                    for r in cur:
                        self.queue_task(r)
            cur.close()
            del cur
            conn.close()
        except Exception as e:
            self.logger.log_message('(checkForPendingTasks) %s' % e, "ERROR")

    def describe_modules(self):
        self.logger.log_message("\nDESCRIBING TASK HANDLER MODULES:\n", "INFO")
        taskHandlersDefinition = '{"task_manager_config":{"task_handlers":['
        notFirst = False
        for files in os.listdir(myconfig.run_dir + 'task_handlers'):
            if files.endswith(".py"):
                modulename = os.path.splitext(files)[0]
                harvester_module = __import__(modulename, globals={}, locals={}, fromlist=[], level=0)
                class_ = getattr(harvester_module, modulename)
                if notFirst:
                    taskHandlersDefinition += ","
                notFirst = True
                taskHandlersDefinition += class_.__doc__.strip()
        taskHandlersDefinition += "]}}"
        file = open(self.__taskHandlersDefinitionFile, "w+")
        file.write(taskHandlersDefinition)
        file.close()
        self.logger.log_message(taskHandlersDefinition, "INFO")

    def info(self):
        """
        Returns the current information for the daemon
        Used for reporting current status to a web interface

        :return:
        """
        start = datetime.fromtimestamp(self.__startUpTime)
        now = datetime.now()
        dtformat = '%Y-%m-%d %H:%M:%S'

        return {
            'running': True,
            'running_since': start.strftime(dtformat),
            'uptime': (now - start).seconds,
            'counts': {
                'running': len(self.__runningTasks),
                'queued': len(self.__queuedTasks),
                'started': self.__startedTasksCount,
                'completed': self.__completedTasksCount,
                'stopped': self.__stoppedTasksCount,
                'errored': self.__erroredTasksCount,
            }
        }

    @staticmethod
    def setup_env():

        if not os.path.exists(myconfig.data_store_path):
            os.makedirs(myconfig.data_store_path)
            os.chmod(myconfig.data_store_path, 0o777)
        if not os.path.exists(myconfig.log_dir):
            os.makedirs(myconfig.log_dir)
            os.chmod(myconfig.log_dir, 0o777)

    def run(self):
        self.initialise()
        # Starting the web interface as a different thread
        try:
            web_port = getattr(myconfig, 'web_port', 7021)
            web_host = getattr(myconfig, 'web_host', '0.0.0.0')
            http = web_server.new(daemon=self)
            threading.Thread(
                target=http.run,
                kwargs={
                    'host': web_host,
                    'port': web_port,
                    'debug': False
                },
                daemon=True
            ).start()
            self.logger.log_message("\n\nWeb Thread started at port %s \n\n" % web_port)
        except Exception as e:
            self.logger.log_message("error %r" % e)
            pass
        try:
            while True:
                self.manage_tasks()
                time.sleep(myconfig.polling_frequency)
        except (KeyboardInterrupt, SystemExit):
            self.logger.log_message("\n\nSTOPPING...", "INFO")
            # self.shutDown()
        except Exception as e:
            self.logger.log_message("error %r" % e, "ERROR")
            pass

    def initialise(self):
        self.__startUpTime = time.time()
        self.__lastLogCount = 99
        self.database = DataBase()
        self.logger = Logger()
        self.setup_env()
        # self.describeModules()
        self.logger.log_message("\n\nSTARTING TASKS MANAGER...", "INFO")
        atexit.register(self.shut_down)

    def print_logs(self, tCounter):
        if self.__lastLogCounter > 0 or tCounter > 0:
            self.__lastLogCounter = tCounter
            self.logger.log_message(
                'RUNNING: %s WAITING: %s' % (str(len(self.__runningTasks)), str(len(self.__queuedTasks))), "DEBUG")
            self.logger.log_message('RUNNING', "DEBUG")
            for tasksID in list(self.__runningTasks):
                taskHandler = self.__runningTasks[tasksID]
                self.logger.log_message(taskHandler.get_info(), "DEBUG")
            self.logger.log_message('WAITING', "DEBUG")
            for taskHandler in list(self.__queuedTasks):
                self.logger.log_message(taskHandler.get_info(), "DEBUG")
            self.logger.log_message(
                '______________________________________________________________________________________________',
                "DEBUG")
        # else:
        # self.requestMaintenanceTasks()

    def shut_down(self):
        # self.__logger.logMessage("SHUTTING DOWN...")
        loggedUserMsg = os.popen('who').read()
        self.logger.log_message("SHUTTING DOWN...\nLogged In Users:\n%s" % loggedUserMsg, "INFO")


if __name__ == '__main__':
    sys.path.append(myconfig.run_dir + 'task_handlers')
    td = TasksManager()
    if len(sys.argv) == 2:
        if 'run' == sys.argv[1]:
            print("Starting TasksManager in the foreground")
            td.run()
        else:
            print("Unknown command")
            sys.exit(2)
        sys.exit(0)
    else:
        print("Usage: {} run".format(sys.argv[0]))
        sys.exit(2)
