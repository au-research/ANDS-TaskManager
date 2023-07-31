# harvester daemon script in python
# for ANDS registry
# Author: u4187959
# created 12/05/2014
#


import atexit
import os
import sys
import myconfig
import threading
import urllib.parse as urlparse
from datetime import datetime
import time
from collections import deque
from utils import web_server
from utils.Logger import Logger
from utils.Database import DataBase
sys.path.append(myconfig.run_dir)
os.getcwd()
from task_handlers import *


class TasksManager:
    __scheduler = False
    logger = False
    database = False
    __queued_tasks = deque()
    __running_tasks = {}
    __running_threads = {}
    __queued_running_task_id_to_ds_id_dict = {}
    __last_log_counter = 999
    __task_handlers_definition_file = False
    __start_up_time = None
    __started_tasks_count = 0
    __completed_tasks_count = 0
    __errored_tasks_count = 0
    __stopped_tasks_count = 0

    def __init__(self):
        self.__lastLogCount = None
        self.logger = Logger()
        self.database = DataBase()

    def handle_exception(self, tasks_id, exception):
        task_status = 'STOPPED'
        self.__errored_tasks_count += 1
        e_message = repr(exception).replace("'", "").replace('"', "")
        attempts = 0
        while attempts < 3:
            try:
                conn = self.database.get_connection()
                cur = conn.cursor()
                cur.execute("UPDATE %s SET `status` ='%s', `message` = '%s' where `id` = %s" % (
                    myconfig.tasks_table, task_status, e_message, str(tasks_id)))
                conn.commit()
                cur.close()
                del cur
                conn.close()
                break
            except Exception as e:
                attempts += 1
                time.sleep(5)
                self.logger.log_message("(handleException) %s, Retry: %d" % (str(repr(e)), attempts), "ERROR")

    def queue_task(self, task_row):

        task_info = {}
        task_type = 'POKE'
        task_id = task_row[0]
        if task_row[2] is not None:
            task_type = task_row[2]
        task_handler = str(task_type + 'TaskHandler')
        task_info['task_id'] = task_row[0]
        task_info['name'] = task_row[1]
        task_info['type'] = task_type
        task_info['status'] = task_row[3]
        task_info['message'] = task_row[4]
        task_info['date_added'] = task_row[5]
        task_info['next_run'] = task_row[6]
        task_info['last_run'] = task_row[7]
        task_info['priority'] = task_row[8]
        task_info['frequency'] = task_row[9]
        task_info['params'] = task_row[10]

        url_params = urlparse.parse_qs(task_info['params'])
        ds_id = None
        try:
            if isinstance(url_params['ds_id'], list):
                ds_id = url_params['ds_id'][0]
                task_info['ds_id'] = ds_id
        except KeyError:
            pass

        if ds_id is not None and len(self.__queued_running_task_id_to_ds_id_dict) > 0:
            for taskid, dsid in self.__queued_running_task_id_to_ds_id_dict.items():
                if dsid == ds_id:
                    self.logger.log_message(
                        "POSTPONING QUEUEING tasks ID:%s, (Datasource: %s is busy)" % (str(task_id), ds_id), "DEBUG")
                    return False
        try:
            self.logger.log_message("QUEUEING tasks ID:%s, type %s, ds_id: %s" % (str(task_row[0]), task_row[2], ds_id),
                                    "DEBUG")
            task_handler_module = __import__(task_handler, globals={}, locals={}, fromlist=[], level=0)
            class_ = getattr(task_handler_module, task_handler)
            task_processor = class_(task_info, self.logger, self.database)
            self.__queued_running_task_id_to_ds_id_dict[task_id] = ds_id
            self.__queued_tasks.append(task_processor)
        except Exception as e:
            self.logger.log_message(repr(e), "ERROR")
            self.handle_exception(task_id, e)
        return True

    def manage_tasks(self):
        # self.reportToRegistry()
        self.check_for_pending_tasks()
        task_id = None
        # if max hasn't reached add more harvests that are queued
        if len(self.__queued_tasks) > 0 and len(self.__running_tasks) < myconfig.max_thread_count:
            while len(self.__running_tasks) < myconfig.max_thread_count and len(self.__queued_tasks) > 0:
                try:
                    task_processor = self.__queued_tasks.pop()
                    task_id = task_processor.get_task_id()
                    if task_processor.get_status() == "PENDING":
                        self.__running_tasks[task_id] = task_processor
                        t = threading.Thread(target=task_processor.run_task)
                        self.__running_threads[task_id] = t
                        t.start()
                        self.__started_tasks_count = self.__started_tasks_count + 1
                    elif task_processor.is_completed or task_processor.is_stopped:
                        del self.__queued_running_task_id_to_ds_id_dict[task_id]
                except KeyError:
                    self.logger.log_message("tasksID %s already scheduled" % str(task_id), "ERROR")
                    del self.__queued_running_task_id_to_ds_id_dict[task_id]
        self.print_logs(int(len(self.__running_tasks)) + int(len(self.__queued_tasks)))
        # clean up completed harvests
        if len(self.__running_tasks) > 0:
            for task_id in list(self.__running_tasks):
                try:
                    task_processor = self.__running_tasks[task_id]
                    if task_processor.is_completed():
                        self.__completed_tasks_count = self.__completed_tasks_count + 1
                        del task_processor
                        if self.__running_threads[task_id].isAlive():
                            del self.__running_threads[task_id]
                        del self.__running_tasks[task_id]
                        del self.__queued_running_task_id_to_ds_id_dict[task_id]
                    elif task_processor.is_stopped():
                        self.__stopped_tasks_count = self.__stopped_tasks_count + 1
                        del task_processor
                        if self.__running_threads[task_id].isAlive():
                            del self.__running_threads[task_id]
                        del self.__running_tasks[task_id]
                        del self.__queued_running_task_id_to_ds_id_dict[task_id]
                except KeyError:
                    self.logger.log_message("tasksID %s already deleted" % str(task_id), "ERROR")
                    del self.__queued_running_task_id_to_ds_id_dict[task_id]

    def get_current_tasks_ids(self):
        current_tasks_i_ds = ''
        first = True
        for tasksID in list(self.__running_tasks):
            if first:
                first = False
                current_tasks_i_ds = str(tasksID)
            else:
                current_tasks_i_ds = current_tasks_i_ds + "," + str(tasksID)
        for taskHandler in list(self.__queued_tasks):
            if first:
                current_tasks_i_ds = str(taskHandler.get_task_id())
            else:
                current_tasks_i_ds = current_tasks_i_ds + "," + str(taskHandler.get_task_id())
        return current_tasks_i_ds

    def check_for_pending_tasks(self):
        try:
            conn = self.database.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT `id` FROM " + myconfig.tasks_table + " where `status` = 'RUNNING'; ")
            if cur.rowcount > 0:
                self.logger.log_message("RUNNING (according to tasks table): %s" % str(cur.rowcount), "DEBUG")
            if len(self.__queued_tasks) < 10 and cur.rowcount < 5:
                current_tasks = self.get_current_tasks_ids()
                if len(current_tasks) > 0:
                    self.logger.log_message("current_tasks: %s" % current_tasks, "DEBUG")
                    cur.execute("SELECT * FROM " + myconfig.tasks_table
                                + " where `status` = 'PENDING' and `type` = 'PHPSHELL' and (`next_run` is null or "
                                  "`next_run` <=timestamp('" + str(datetime.now())
                                + "')) AND id NOT IN (" + current_tasks + ") ORDER BY `date_added` ASC LIMIT "
                                + str(10 - len(self.__queued_tasks)) + ";")
                else:
                    cur.execute("SELECT * FROM " + myconfig.tasks_table
                                + " where `status` = 'PENDING' and `type` = 'PHPSHELL' and (`next_run` is null or "
                                  "`next_run` <=timestamp('" + str(datetime.now())
                                + "')) ORDER BY `date_added` ASC LIMIT " + str(10 - len(self.__queued_tasks)) + ";")
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
        task_handlers_definition = '{"task_manager_config":{"task_handlers":['
        not_first = False
        for files in os.listdir(myconfig.run_dir + 'task_handlers'):
            if files.endswith(".py"):
                modulename = os.path.splitext(files)[0]
                harvester_module = __import__(modulename, globals={}, locals={}, fromlist=[], level=0)
                class_ = getattr(harvester_module, modulename)
                if not_first:
                    task_handlers_definition += ","
                not_first = True
                task_handlers_definition += class_.__doc__.strip()
        task_handlers_definition += "]}}"
        file = open(self.__task_handlers_definition_file, "w+")
        file.write(task_handlers_definition)
        file.close()
        self.logger.log_message(task_handlers_definition, "INFO")

    def info(self):
        """
        Returns the current information for the daemon
        Used for reporting current status to a web interface

        :return:
        """
        start = datetime.fromtimestamp(self.__start_up_time)
        now = datetime.now()
        dtformat = '%Y-%m-%d %H:%M:%S'

        return {
            'running': True,
            'running_since': start.strftime(dtformat),
            'uptime': (now - start).seconds,
            'counts': {
                'running': len(self.__running_tasks),
                'queued': len(self.__queued_tasks),
                'started': self.__started_tasks_count,
                'completed': self.__completed_tasks_count,
                'stopped': self.__stopped_tasks_count,
                'errored': self.__errored_tasks_count,
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
        self.__start_up_time = time.time()
        self.__lastLogCount = 99
        self.database = DataBase()
        self.logger = Logger()
        self.setup_env()
        # self.describeModules()
        self.logger.log_message("\n\nSTARTING TASKS MANAGER...", "INFO")
        atexit.register(self.shut_down)

    def print_logs(self, t_counter):
        if self.__last_log_counter > 0 or t_counter > 0:
            self.__last_log_counter = t_counter
            self.logger.log_message(
                'RUNNING: %s WAITING: %s' % (str(len(self.__running_tasks)), str(len(self.__queued_tasks))), "DEBUG")
            self.logger.log_message('RUNNING', "DEBUG")
            for tasksID in list(self.__running_tasks):
                task_handler = self.__running_tasks[tasksID]
                self.logger.log_message(task_handler.get_info(), "DEBUG")
            self.logger.log_message('WAITING', "DEBUG")
            for task_handler in list(self.__queued_tasks):
                self.logger.log_message(task_handler.get_info(), "DEBUG")
            self.logger.log_message(
                '______________________________________________________________________________________________',
                "DEBUG")
        # else:
        # self.requestMaintenanceTasks()

    def shut_down(self):
        # self.__logger.logMessage("SHUTTING DOWN...")
        logged_user_msg = os.popen('who').read()
        self.logger.log_message("SHUTTING DOWN...\nLogged In Users:\n%s" % logged_user_msg, "INFO")


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
