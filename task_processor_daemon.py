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


class Logger:
    __fileName = False
    __file = False
    __current_log_time = False
    def __init__(self):
        self.__current_log_time = datetime.now().strftime("%Y-%m-%d")
        self.__fileName = myconfig.log_dir + os.sep + self.__current_log_time + ".log"

    def logMessage(self, message):
        self.rotateLogFile()
        self.__file = open(self.__fileName, "a", 0o777)
        self.__file.write(message + " %s"  % datetime.now() + "\n")
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

        self.__logger.logMessage('deamon going to background, PID: {}'.format(os.getpid()))

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
        self.__logger.logMessage("\n\nDELETING PID FILE...")
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
        self.__logger.logMessage("\n\nSTARTING __runningTasksMANAGER_DAEMON...")
        self.daemonize()
        try:
            atexit.register(Daemon.shutDown)
            self.run()
        except (KeyboardInterrupt, SystemExit):
            self.shutDown()
            self.__logger.logMessage("\n\nSTOPPING...")


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
        self.__logger.logMessage("\nSTOPPING __runningTasksMANAGER_DAEMON...")

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
            self.__logger.logMessage("\nKILLING %s..." %str(pid))
            time.sleep(3)
        except OSError as e:
            print(str(e))
            sys.exit(1)

        try:
            if os.path.exists(self.pidfile):
                self.__logger.logMessage("\nDELETING PIDFILE %s..." %self.pidfile)
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
        self.__logger.logMessage("\nSHUTTING DOWN ...")

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
        def __init__(self):
            self.__current_log_time = datetime.now().strftime("%Y-%m-%d")
            self.__fileName = myconfig.log_dir + os.sep + self.__current_log_time + ".log"

        def logMessage(self, message):
            self.rotateLogFile()
            self.__file = open(self.__fileName, "a", 0o777)
            self.__file.write(message + " %s"  % datetime.now() + "\n")
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


        def getConnection(self):
            #if not(self.__connection):
            try:
                self.__connection = pymysql.connect(host=self.__host, user=self.__user, passwd = self.__passwd, db = self.__db)
            except:
                e = sys.exc_info()[1]
                raise RuntimeError("Database Exception %s" %(e))
            return self.__connection

    def handleException(self, tasksID, exception):
        taskStatus = 'STOPPED'
        self.__erroredTasksCount += 1
        eMessage = repr(exception).replace("'", "").replace('"', "")
        try:
            conn = self.__database.getConnection()
        except Exception as e:
            return
        cur = conn.cursor()
        cur.execute("UPDATE %s SET `status` ='%s', `message` = '%s' where `id` = %s" %(myconfig.tasks_table, taskStatus, eMessage, str(tasksID)))
        conn.commit()
        cur.close()
        del cur
        conn.close()

    def queueTask(self, taskRow):
        self.__logger.logMessage("QUEUING tasks Info:::%s, %s" %(str(taskRow[0]), taskRow[2]))
        taskInfo = {}
        taskID = taskRow[0]
        taskHandler = str(taskRow[2] + 'TaskHandler')
        taskInfo['task_id'] = taskRow[0]
        taskInfo['name'] = taskRow[1]
        taskInfo['type'] = taskRow[2]
        taskInfo['status'] = taskRow[3]
        taskInfo['message'] = taskRow[4]
        taskInfo['date_added'] = taskRow[5]
        taskInfo['next_run'] = taskRow[6]
        taskInfo['last_run'] = taskRow[7]
        taskInfo['priority'] = taskRow[8]
        taskInfo['frequency'] = taskRow[9]
        taskInfo['params'] = taskRow[10]

        try:
            task_handler_module = __import__(taskHandler, globals={}, locals={}, fromlist=[], level=0)
            class_ = getattr(task_handler_module, taskHandler)
            taskProcessor = class_(taskInfo, self.__logger, self.__database)
            self.__queuedTasks.append(taskProcessor)
        except ImportError as e:
            self.__logger.logMessage(e)
            self.handleException(taskID, e)

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
            self.__logger.logMessage(e)
            self.handleException(taskID, e)


    def manageTasks(self):
        self.checkForPendingTasks()
        self.reportToRegistry()
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
                except KeyError as e:
                    self.__logger.logMessage("tasksID %s already scheduled" %str(taskID))
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
                    elif taskProcessor.isStopped():
                        self.__stoppedTasksCount = self.__stoppedTasksCount + 1
                        del taskProcessor
                        if self.__running_threads[taskID].isAlive() == True:
                            del self.__running_threads[taskID]
                        del self.__runningTasks[taskID]
                except KeyError as e:
                    self.__logger.logMessage("tasksID %s already deleted" %str(taskID))
        #if max hasn't reached add more harvests that are WAITING



    def checkForPendingTasks(self):
        if len(self.__queuedTasks) < 10:
            try:
                conn = self.__database.getConnection()
            except Exception as e:
                return
            cur = conn.cursor()
            cur.execute("SELECT * FROM "+ myconfig.tasks_table +" where `status` = 'PENDING' and (`next_run` is null or `next_run` <=timestamp('" + str(datetime.now()) + "')) ORDER BY `priority`,`next_run` ASC LIMIT " + str(10 - len(self.__queuedTasks)) + ";")
            self.__logger.logMessage("Add PENDING Tasks to queue (Count:%s)" %str(cur.rowcount))
            if(cur.rowcount > 0):
                self.__logger.logMessage("Add PENDING Tasks to queue (Count:%s)" %str(cur.rowcount))
                for r in cur:
                    self.queueTask(r)
            cur.close()
            del cur
            conn.close()


    def describeModules(self):
        self.__logger.logMessage("\nDESCRIBING TASK HANDLER MODULES:\n")
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
        self.__logger.logMessage(taskHandlersDefinition)


    def reportToRegistry(self):
        statusDict = {'last_report_timestamp' : time.time(),
                    'start_up_time' : self.__startUpTime,
                    'tasks_running' : str(len(self.__runningTasks)),
                    'tasks_queued' : str(len(self.__queuedTasks)),
                    'total_tasks_started' : str(self.__startedTasksCount),
                    'tasks_completed' : str(self.__completedTasksCount),
                    'tasks_stopped' : str(self.__stoppedTasksCount),
                    'tasks_errored' : str(self.__erroredTasksCount),
                    }
        try:
            conn = self.__database.getConnection()
        except Exception as e:
            return
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


    def setupEnv(self):

        if not os.path.exists(myconfig.data_store_path):
            os.makedirs(myconfig.data_store_path)
            os.chmod(myconfig.data_store_path, 0o777)
        if not os.path.exists(myconfig.log_dir):
            os.makedirs(myconfig.log_dir)
            os.chmod(myconfig.log_dir, 0o777)


    def run(self):
        self.__startUpTime = time.time()
        self.__lastLogCount = 99
        self.__database = self.__DataBase()
        self.__logger = self.__Logger()
        self.__taskHandlersDefinitionFile = myconfig.run_dir + os.sep + "task_handlers_definition.json"
        self.setupEnv()
        self.describeModules()
        self.__logger.logMessage("\n\nSTARTING TASKS MANAGER...")
        atexit.register(self.shutDown)
        try:
            while True:
                self.manageTasks()
                time.sleep(myconfig.polling_frequency)
        except (KeyboardInterrupt, SystemExit):
            self.__logger.logMessage("\n\nSTOPPING...")
            #self.shutDown()
        except Exception as e:
            self.__logger.logMessage("error %r" %(e))
            pass


    def printLogs(self, tCounter):
        if(self.__lastLogCounter > 0 or tCounter > 0):
            self.__lastLogCounter = tCounter
            self.__logger.logMessage('RUNNING: %s WAITING: %s' %(str(len(self.__runningTasks)), str(len(self.__queuedTasks))))
            self.__logger.logMessage('R...')
            for tasksID in list(self.__runningTasks):
                taskHandler = self.__runningTasks[tasksID]
                self.__logger.logMessage(taskHandler.getInfo())
            self.__logger.logMessage('W...')
            for taskHandler in list(self.__queuedTasks):
                self.__logger.logMessage(taskHandler.getInfo())
            self.__logger.logMessage('______________________________________________________________________________________________')
        else:
            self.requestMaintenanceTasks()



    def shutDown(self):
        #self.__logger.logMessage("SHUTTING DOWN...")
        loggedUserMsg = os.popen('who').read()
        self.__logger.logMessage("SHUTTING DOWN...\nLogged In Users:\n%s" %(loggedUserMsg))
        try:
            if os.path.exists(self.pidfile):
                self.__logger.logMessage("\n\nDELETING PIDFILE %s..." %self.pidfile)
                os.remove(self.pidfile)
        except IOError as e:
            message = str(e) + "\nCan not remove pid file {}".format(self.pidfile)
            self.__logger.logMessage(message)
        except Exception as e:
            self.__logger.logMessage("error %r" %(e))





if __name__ == '__main__':
    sys.path.append(myconfig.run_dir + 'task_handlers')
    td = TasksManagerDaemon(myconfig.run_dir + '/daemon.pid')
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


