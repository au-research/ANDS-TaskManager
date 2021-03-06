# harvester daemon script in python
# for ANDS registry
# Author: u4187959
# created 12/05/2014
#

from datetime import datetime
import sys, os, time, atexit

from signal import SIGTERM, SIGINT
import pymysql
import myconfig
sys.path.append(myconfig.run_dir)
os.getcwd()
import time
from collections import deque
import TaskHandler
import string
import json
import threading
from task_handlers import *
import urllib.parse as urlparse
import web_server


class Logger:
    __fileName = False
    __file = False
    __current_log_time = False
    logLevels = {'ERROR':100,'INFO':50,'DEBUG':10}
    __logLevel = 100
    def __init__(self):
        self.__current_log_time = datetime.now().strftime("%Y-%m-%d")
        self.__fileName = myconfig.log_dir + os.sep + self.__current_log_time + ".log"
        self.__logLevel = self.logLevels[myconfig.log_level]
        self.logMessage("loglevel set to %s:%s" %(str(self.__logLevel), myconfig.log_level), myconfig.log_level)

    def logMessage(self, message, logLevel='DEBUG'):
        # print("LOGGING:%s:%s" %(str(self.logLevels[logLevel]), logLevel))
        if(self.logLevels[logLevel] >= self.__logLevel):
            self.rotateLogFile()
            self.__file = open(self.__fileName, "a", 0o775)
            os.chmod(self.__fileName, 0o775)
            self.__file.write(logLevel + ": " + message + " %s"  % datetime.now() + "\n")
            self.__file.close()

    def rotateLogFile(self):
        if(self.__current_log_time != datetime.now().strftime("%Y-%m-%d")):
            self.__current_log_time = datetime.now().strftime("%Y-%m-%d")
            self.__fileName = myconfig.log_dir + os.sep + self.__current_log_time + ".log"

class Daemon(object):
    """
    Subclass Daemon class and override the run() method.
    """
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile
        self.__logger = Logger()

    def daemonize(self):
        """
        Deamonize, do double-fork magic.
        """
        try:
            pid = os.fork()
            if pid > 0:
                # Exit first parent.
                sys.exit(0)
        except OSError as e:
            message = "Fork #1 failed: {}\n".format(e)
            sys.stderr.write(message)
            sys.exit(1)

        # Decouple from parent environment.
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # Do second fork.
        try:
            pid = os.fork()
            if pid > 0:
                # Exit from second parent.
                sys.exit(0)
        except OSError as e:
            message = "Fork #2 failed: {}\n".format(e)
            sys.stderr.write(message)
            sys.exit(1)

        self.__logger.logMessage('deamon going to background, PID: {}'.format(os.getpid()), "INFO")

        # Redirect standard file descriptors.
        sys.stdout.flush()
        sys.stderr.flush()
        si = open(self.stdin, 'r')
        so = open(self.stdout, 'a+')
        se = open(self.stderr, 'a+')
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # Write pidfile.
        pid = str(os.getpid())
        open(self.pidfile,'w+').write("{}\n".format(pid))

        # Register a function to clean up.
        atexit.register(self.delpid)

    def delpid(self):
        self.__logger.logMessage("\n\nDELETING PID FILE...", "INFO")
        os.remove(self.pidfile)

    def start(self):
        """
        Start daemon.
        """
        # Check pidfile to see if the daemon already runs.
        try:
            pf = open(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = "Pidfile {} already exist. Daemon already running?\n".format(self.pidfile)
            message += "If you're sure that the __runningTasksmanager is not running delete the pid file and try again!\n"
            sys.stderr.write(message)
            sys.exit(1)

        # Start daemon.
        self.__logger.logMessage("\n\nSTARTING __runningTasksMANAGER_DAEMON...", "INFO")
        self.daemonize()
        try:
            atexit.register(Daemon.shutDown)
            self.run()
        except (KeyboardInterrupt, SystemExit):
            self.shutDown()
            self.__logger.logMessage("\n\nSTOPPING...", "INFO")


    def status(self):
        """
        Get status of daemon.
        """
        try:
            pf = open(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            message = "There is no PID file. is the ANDS-__runningTasksManager running?\n"
            sys.stderr.write(message)
            sys.exit(1)

        try:
            procfile = open("/proc/{}/status".format(pid), 'r')
            procfile.close()
            message = "The __runningTasksManager is running with the PID {}\n".format(pid)
            sys.stdout.write(message)
        except IOError:
            message = "There is not a process with the PID {}\n".format(self.pidfile)
            sys.stdout.write(message)

    def stop(self):
        """
        Stop the daemon.
        """
        # Get the pid from pidfile.
        self.__logger.logMessage("\nSTOPPING __runningTasksMANAGER_DAEMON...", "INFO")

        try:
            pf = open(self.pidfile,'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError as e:
            message = str(e) + "\n__runningTasksManager Daemon is not running?\n"
            sys.stderr.write(message)
            sys.exit(1)

        # Try killing daemon process.
        try:
            os.kill(pid, SIGINT)
            self.__logger.logMessage("\nKILLING %s..." %str(pid), "INFO")
            time.sleep(3)
        except OSError as e:
            print(str(e))
            sys.exit(1)

        try:
            if os.path.exists(self.pidfile):
                self.__logger.logMessage("\nDELETING PIDFILE %s..." %self.pidfile, "INFO")
                os.remove(self.pidfile)
        except IOError as e:
            message = str(e) + "\nCan not remove pid file {}".format(self.pidfile)
            sys.stderr.write(message)
            sys.exit(1)

    def restart(self):
        """
        Restart daemon.
        """
        self.stop()
        time.sleep(1)
        self.start()

    def shutDown(self):
        self.__logger.logMessage("\nSHUTTING DOWN ...", "INFO")

    def run(self):

        """You should override this method when you subclass Daemon.

        It will be called after the process has been daemonized by
        start() or restart()."""


class TasksManagerDaemon(Daemon):
    __scheduler = False
    __logger = False
    __database = False
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

    class __Logger:
        __fileName = False
        __file = False
        __current_log_time = False
        logLevels = {'ERROR':100,'INFO':50,'DEBUG':10}
        __logLevel = 100
        def __init__(self):
            self.__current_log_time = datetime.now().strftime("%Y-%m-%d")
            self.__fileName = myconfig.log_dir + os.sep + self.__current_log_time + ".log"

        def __init__(self):
            self.__queued_running_task_id_to_ds_id_dict = {}
            self.__current_log_time = datetime.now().strftime("%Y-%m-%d")
            self.__fileName = myconfig.log_dir + os.sep + self.__current_log_time + ".log"
            self.__logLevel = self.logLevels[myconfig.log_level]
            self.logMessage("loglevel set to %s:%s" %(str(self.__logLevel), myconfig.log_level), myconfig.log_level)

        def logMessage(self, message, logLevel='DEBUG'):
            # print("LOGGING:%s:%s" %(str(self.logLevels[logLevel]), logLevel))
            if(self.logLevels[logLevel] >= self.__logLevel):
                self.rotateLogFile()
                self.__file = open(self.__fileName, "a", 0o777)
                self.__file.write(logLevel + ": " + message + " %s"  % datetime.now() + "\n")
                self.__file.close()

        def rotateLogFile(self):
            if(self.__current_log_time != datetime.now().strftime("%Y-%m-%d")):
                self.__current_log_time = datetime.now().strftime("%Y-%m-%d")
                self.__fileName = myconfig.log_dir + os.sep + self.__current_log_time + ".log"

    class __DataBase:
        __connection = False
        __db_host = ''
        __unix_socket = '/tmp/mysql.sock'
        __db_user = ''
        __db_passwd = ''
        __db = ''

        def __init__(self):
            self.__host = myconfig.db_host
            self.__user = myconfig.db_user
            self.__passwd = myconfig.db_passwd
            self.__db = myconfig.db
            self.__port = myconfig.db_port

        def getConnection(self):
            try:
                self.__connection = pymysql.connect(host=self.__host, user=self.__user, passwd=self.__passwd,
                                                    db=self.__db, port=self.__port)
            except:
                e = sys.exc_info()[1]
                raise RuntimeError("Database Exception %s" %(e))
            return self.__connection

    def handleException(self, tasksID, exception):
        taskStatus = 'STOPPED'
        self.__erroredTasksCount += 1
        eMessage = repr(exception).replace("'", "").replace('"', "")
        attempts = 0
        while attempts < 3:
            try:
                conn = self.__database.getConnection()
                cur = conn.cursor()
                cur.execute("UPDATE %s SET `status` ='%s', `message` = '%s' where `id` = %s" %(myconfig.tasks_table, taskStatus, eMessage, str(tasksID)))
                conn.commit()
                cur.close()
                del cur
                conn.close()
                break
            except Exception as e:
                attempts += 1
                time.sleep(5)
                self.logger.logMessage("(handleException) %s, Retry: %d" %(str(repr(e)), attempts), "ERROR")

    def queueTask(self, taskRow):

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
        except KeyError:
            pass

        if ds_id is not None and len(self.__queued_running_task_id_to_ds_id_dict) > 0:
            for taskid, dsid in self.__queued_running_task_id_to_ds_id_dict.items():
                if dsid == ds_id:
                    self.__logger.logMessage("POSTPONING QUEUEING tasks ID:%s, (Datasource: %s is busy)" % (str(taskID), ds_id), "DEBUG")
                    return False
        try:
            self.__logger.logMessage("QUEUEING tasks ID:%s, type %s, ds_id: %s" % (str(taskRow[0]), taskRow[2], ds_id), "DEBUG")
            task_handler_module = __import__(taskHandler, globals={}, locals={}, fromlist=[], level=0)
            class_ = getattr(task_handler_module, taskHandler)
            taskProcessor = class_(taskInfo, self.__logger, self.__database)
            self.__queued_running_task_id_to_ds_id_dict[taskID] = ds_id
            self.__queuedTasks.append(taskProcessor)
        except ImportError as e:
            self.__logger.logMessage(e, "ERROR")
            self.handleException(taskID, e)
        return True

    def requestMaintenanceTasks(self):
        try:
            taskInfo = {}
            taskID = len(self.__queuedTasks)
            taskHandler = 'MAINTENANCETaskHandler'
            taskInfo['task_id'] = taskID
            taskInfo['name'] = 'maintenance task'
            taskInfo['type'] = 'MAINTENANCE'
            taskInfo['status'] = 'PENDING'
            taskInfo['message'] = 'scheduled by task daemon'
            taskInfo['date_added'] = str(datetime.now())
            taskInfo['next_run'] = str(datetime.now())
            taskInfo['last_run'] = ''
            taskInfo['priority'] = 999
            taskInfo['frequency'] = 'once only'
            taskInfo['params'] = ''
            task_handler_module = __import__(taskHandler, globals={}, locals={}, fromlist=[], level=0)
            class_ = getattr(task_handler_module, taskHandler)
            taskProcessor = class_(taskInfo, self.__logger, self.__database)
            self.__queuedTasks.append(taskProcessor)
        except ImportError as e:
            self.__logger.logMessage(e, "ERROR")
            self.handleException(taskID, e)


    def manageTasks(self):
        # self.reportToRegistry()
        self.checkForPendingTasks()
        #if max hasn't reached add more harvests that are queued
        if len(self.__queuedTasks) > 0 and len(self.__runningTasks) < myconfig.max_thread_count:
            while len(self.__runningTasks) < myconfig.max_thread_count and len(self.__queuedTasks) > 0:
                try:
                    taskProcessor = self.__queuedTasks.pop()
                    taskID = taskProcessor.getTaskId()
                    if taskProcessor.getStatus() == "PENDING":
                        self.__runningTasks[taskID] = taskProcessor
                        t = threading.Thread(target=taskProcessor.runTask)
                        self.__running_threads[taskID] = t
                        t.start()
                        self.__startedTasksCount = self.__startedTasksCount + 1
                    elif taskProcessor.isCompleted or taskProcessor.isStopped:
                        del self.__queued_running_task_id_to_ds_id_dict[taskID]
                except KeyError as e:
                    self.__logger.logMessage("tasksID %s already scheduled" %str(taskID), "ERROR")
                    del self.__queued_running_task_id_to_ds_id_dict[taskID]
        self.printLogs(int(len(self.__runningTasks)) + int(len(self.__queuedTasks)))
        #clean up completed harvests
        if len(self.__runningTasks) > 0:
            for taskID in list(self.__runningTasks):
                try:
                    taskProcessor = self.__runningTasks[taskID]
                    if taskProcessor.isCompleted():
                        self.__completedTasksCount = self.__completedTasksCount + 1
                        del taskProcessor
                        if self.__running_threads[taskID].isAlive() == True:
                            del self.__running_threads[taskID]
                        del self.__runningTasks[taskID]
                        del self.__queued_running_task_id_to_ds_id_dict[taskID]
                    elif taskProcessor.isStopped():
                        self.__stoppedTasksCount = self.__stoppedTasksCount + 1
                        del taskProcessor
                        if self.__running_threads[taskID].isAlive() == True:
                            del self.__running_threads[taskID]
                        del self.__runningTasks[taskID]
                        del self.__queued_running_task_id_to_ds_id_dict[taskID]
                except KeyError as e:
                    self.__logger.logMessage("tasksID %s already deleted" %str(taskID), "ERROR")
                    del self.__queued_running_task_id_to_ds_id_dict[taskID]



    def getCurrentTasksIDs(self):
        currentTasksIDs = '';
        first = True
        for tasksID in list(self.__runningTasks):
            if first:
                first = False
                currentTasksIDs = str(tasksID)
            else:
                currentTasksIDs = currentTasksIDs + "," + str(tasksID)
        for taskHandler in list(self.__queuedTasks):
            if first:
                currentTasksIDs = str(taskHandler.getTaskId())
            else:
                currentTasksIDs = currentTasksIDs + "," + str(taskHandler.getTaskId())
        return currentTasksIDs


    def checkForPendingTasks(self):
        try:
            conn = self.__database.getConnection()
            cur = conn.cursor()
            cur.execute("SELECT `id` FROM " + myconfig.tasks_table + " where `status` = 'RUNNING'; ")
            if cur.rowcount > 0:
                self.__logger.logMessage("RUNNING (according to tasks table): %s" %str(cur.rowcount), "DEBUG")
            if len(self.__queuedTasks) < 10 and cur.rowcount < 5:
                currentTasks = self.getCurrentTasksIDs()
                if len(currentTasks) > 0:
                    self.__logger.logMessage("currentTasks: %s" % currentTasks, "DEBUG")
                    cur.execute("SELECT * FROM "+ myconfig.tasks_table
                            +" where `status` = 'PENDING' and (`next_run` is null or `next_run` <=timestamp('" + str(datetime.now())
                            + "')) AND id NOT IN (" + currentTasks + ") ORDER BY `priority`,`date_added` ASC LIMIT " + str(10 - len(self.__queuedTasks)) + ";")
                else:
                    cur.execute("SELECT * FROM "+ myconfig.tasks_table
                            +" where `status` = 'PENDING' and (`next_run` is null or `next_run` <=timestamp('" + str(datetime.now())
                            + "')) ORDER BY `priority`,`date_added` ASC LIMIT " + str(10 - len(self.__queuedTasks)) + ";")
                if(cur.rowcount > 0):
                    self.__logger.logMessage("Add PENDING Tasks to queue (Count:%s)" %str(cur.rowcount), "DEBUG")
                    for r in cur:
                        self.queueTask(r)
            cur.close()
            del cur
            conn.close()
        except Exception as e:
            self.__logger.logMessage('(checkForPendingTasks) %s' % (e), "ERROR")


    def describeModules(self):
        self.__logger.logMessage("\nDESCRIBING TASK HANDLER MODULES:\n", "INFO")
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
        taskHandlersDefinition +=  "]}}"
        file = open(self.__taskHandlersDefinitionFile, "w+")
        file.write(taskHandlersDefinition)
        file.close()
        self.__logger.logMessage(taskHandlersDefinition, "INFO")

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

    def reportToRegistry(self):
        """
        Report it's status to the registry in the form of writing to the database
        R29 deprecates this functionality in favor of local http server

        :return:
        """
        statusDict = {'last_report_timestamp' : time.time(),
                    'start_up_time' : self.__startUpTime,
                    'tasks_running' : str(len(self.__runningTasks)),
                    'tasks_queued' : str(len(self.__queuedTasks)),
                    'total_tasks_started' : str(self.__startedTasksCount),
                    'tasks_completed' : str(self.__completedTasksCount),
                    'tasks_stopped' : str(self.__stoppedTasksCount),
                    'tasks_errored' : str(self.__erroredTasksCount),
                    }
        attempts = 0
        while attempts < 3:
            try:
                conn = self.__database.getConnection()
                cur = conn.cursor()
                cur.execute("SELECT * FROM configs WHERE `key`='tasks_daemon_status';")
                if(cur.rowcount > 0):
                    cur.execute("UPDATE configs set `value`='%s' where `key`='tasks_daemon_status';" %(json.dumps(statusDict).replace("'", "\\\'")))
                else:
                    cur.execute("INSERT INTO configs (`value`, `type`, `key`) VALUES ('%s','%s','%s');" %(json.dumps(statusDict).replace("'", "\\\'"), 'json', 'tasks_daemon_status'))
                conn.commit()
                cur.close()
                del cur
                conn.close()
                self.__logger.logMessage('Reporting to Registry', "DEBUG")
                break
            except Exception as e:
                attempts += 1
                time.sleep(5)
                self.__logger.logMessage('(reportToRegistry) %s, Retry: %d' %(str(repr(e)), attempts), "ERROR")
        return


    def setupEnv(self):

        if not os.path.exists(myconfig.data_store_path):
            os.makedirs(myconfig.data_store_path)
            os.chmod(myconfig.data_store_path, 0o777)
        if not os.path.exists(myconfig.log_dir):
            os.makedirs(myconfig.log_dir)
            os.chmod(myconfig.log_dir, 0o777)


    def run(self):
        self.initalise()
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
            self.__logger.logMessage("\n\nWeb Thread started at port %s \n\n" % web_port)
        except Exception as e:
            self.__logger.logMessage("error %r" % e)
            pass

        try:
            while True:
                self.manageTasks()
                time.sleep(myconfig.polling_frequency)
        except (KeyboardInterrupt, SystemExit):
            self.__logger.logMessage("\n\nSTOPPING...", "INFO")
            #self.shutDown()
        except Exception as e:
            self.__logger.logMessage("error %r" %(e), "ERROR")
            pass


    def initalise(self):
        self.__startUpTime = time.time()
        self.__lastLogCount = 99
        self.__database = self.__DataBase()
        self.__logger = self.__Logger()
        # self.__taskHandlersDefinitionFile = myconfig.run_dir + "task_handlers_definition.json"
        self.setupEnv()
        # self.describeModules()
        self.__logger.logMessage("\n\nSTARTING TASKS MANAGER...", "INFO")
        atexit.register(self.shutDown)


    def printLogs(self, tCounter):
        if(self.__lastLogCounter > 0 or tCounter > 0):
            self.__lastLogCounter = tCounter
            self.__logger.logMessage('RUNNING: %s WAITING: %s' %(str(len(self.__runningTasks)), str(len(self.__queuedTasks))) , "DEBUG")
            self.__logger.logMessage('RUNNING', "DEBUG")
            for tasksID in list(self.__runningTasks):
                taskHandler = self.__runningTasks[tasksID]
                self.__logger.logMessage(taskHandler.getInfo(), "DEBUG")
            self.__logger.logMessage('WAITING', "DEBUG")
            for taskHandler in list(self.__queuedTasks):
                self.__logger.logMessage(taskHandler.getInfo(), "DEBUG")
            self.__logger.logMessage('______________________________________________________________________________________________', "DEBUG")
        #else:
            #self.requestMaintenanceTasks()



    def shutDown(self):
        #self.__logger.logMessage("SHUTTING DOWN...")
        loggedUserMsg = os.popen('who').read()
        self.__logger.logMessage("SHUTTING DOWN...\nLogged In Users:\n%s" %(loggedUserMsg), "INFO")
        try:
            if os.path.exists(self.pidfile):
                self.__logger.logMessage("\n\nDELETING PIDFILE %s..." %self.pidfile, "INFO")
                os.remove(self.pidfile)
        except IOError as e:
            message = str(e) + "\nCan not remove pid file {}".format(self.pidfile)
            self.__logger.logMessage(message, "ERROR")
        except Exception as e:
            self.__logger.logMessage("error %r" %(e), "ERROR")





if __name__ == '__main__':
    sys.path.append(myconfig.run_dir + 'task_handlers')
    td = TasksManagerDaemon(myconfig.run_dir + 'daemon.pid')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            print ("Starting TasksManager as Daemon")
            td.start()
        elif 'run' == sys.argv[1]:
            print ("Starting TasksManager in the foreground")
            td.run()
        elif 'stop' == sys.argv[1]:
            td.stop()
        elif 'restart' == sys.argv[1]:
            td.restart()
        elif 'status' == sys.argv[1]:
            td.status()
        else:
            print ("Unknown command")
            sys.exit(2)
        sys.exit(0)
    else:
        print ("Usage: {} run|start|stop|restart".format(sys.argv[0]))
        sys.exit(2)


