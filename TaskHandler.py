try:
    import urllib.request as urllib2
except:
    import urllib2
import time
import os
import json
from subprocess import Popen, PIPE
import myconfig
from signal import SIGTERM, SIGINT

class PHPShell:
    data = None
    cmd = None
    taskId = None
    method = None
    wd = None
    pid = None

    def __init__(self,  taskId):
        self.cmd = "php index.php api task"
        self.taskId = taskId
        self.wd = myconfig.php_shell_working_dir

    def stop(self):
        shellCommand = self.cmd
        self.method = "stop"
        shellCommand += " " + self.method + " " + self.taskId
        try:
            proc = Popen(shellCommand, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True,
                         cwd=self.wd)
            (output, error) = proc.communicate()
            if proc.returncode != 0 or error != "":
                raise Exception("Stopping the Task stopped by error code: %s, message: %s" %(str(proc.returncode), error))
        except Exception as e:
            raise Exception(str(e))

    def getPID(self):
        return self.pid

    def run(self):
        shellCommand = self.cmd
        self.method = "exe"
        shellCommand += " " + self.method + " " + self.taskId
        try:
            proc = Popen(shellCommand, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True,
                         cwd=self.wd)
            self.pid = proc.pid
            (output, error) = proc.communicate()
            if proc.returncode != 0 or error != "":
                raise Exception("Task (%s) stopped by error code: %s, message: %s" %(str(self.taskId), str(proc.returncode), error))
        except Exception as e:
            raise Exception(str(e))



class Request:
    data = False
    url = False

    def __init__(self, url):
        self.url = url

    def getData(self):
        try:
            req = urllib2.Request(self.url)
            f = urllib2.urlopen(req,timeout=60)
            self.data = f.read()
            return self.data
        except Exception as e:
            raise RuntimeError(str(e) + " Error while trying to connect to: " + self.url)


    def postData(self, data):
        try:
            req = urllib2.Request(self.url)
            f = urllib2.urlopen(req, data, timeout=30)
            self.data = f.read()
            return self.data
        except Exception as e:
            raise RuntimeError(str(e) + " Error while trying to connect to: " + self.url)

    def postCompleted(self):
        try:
            req = urllib2.Request(self.url)
            f = urllib2.urlopen(req, timeout=30)
            self.data = f.read()
            return self.data
        except Exception as e:
            pass


class TaskHandler():
    startUpTime = 0
    pageCount = 0
    recordCount = 0
    tasksInfo = None
    data = None
    logger = None
    database = None
    outputFilePath = None
    outputDir = None
    __status = 'PENDING'
    listSize = 'unknown'
    message = ''
    errorLog = ""
    errored = False
    stopped = False
    completed = False

    def __init__(self, tasksInfo, logger, database):
        self.startUpTime = int(time.time())
        self.tasksInfo = tasksInfo
        self.logger = logger
        self.database = database
        self.updateTasksInfo()

    def getPID(self):
        return None

    def runTask(self):
        self.doSimpleRequest()
        self.finish()


    def doSimpleRequest(self):
        if self.stopped:
            return
        try:
            # self.setStatus('SIMPLE REQUEST INITIATED')
            self.logger.logMessage("SIMPLE REQUEST: %s" %(myconfig.response_url + str(self.tasksInfo['task_id']) + '/?api_key=api'), "DEBUG")
            getRequest = Request(myconfig.response_url + str(self.tasksInfo['task_id']) + '/?api_key=api')
            self.data = getRequest.getData()
            self.logger.logMessage("SIMPLE REQUEST RESPONSE: %s" %(str(self.data)), "DEBUG")
            del getRequest
        except Exception as e:
            self.handleExceptions(e, False)

    def updateTasksInfo(self, updateMessage = False):
        if(not updateMessage):
            self.checkTasksStatus()
        if self.stopped:
            return
        try:
            conn = self.database.getConnection()
        except Exception as e:
            self.logger.logMessage("Database Connection Error: %s" %(str(repr(e))), "ERROR")
            return
        cur = conn.cursor()
        upTime = int(time.time()) - self.startUpTime
        statusDict = {'status':self.__status,
                      'message':self.message,
                      'error':{'log':str.strip(self.errorLog), 'errored': self.errored},
                      'completed':str(self.completed),
                      'output':{'file': self.outputFilePath, 'dir': self.outputDir},
                      'progress':{'time':str(upTime),'start':str(self.startUpTime), 'end':''}
                    }
        try:
            if updateMessage:
                cur.execute("UPDATE %s SET `status` ='%s', `message` = '%s' where `id` = %s" % (
                myconfig.tasks_table, self.__status, json.dumps(statusDict).replace("'", "\\\'"), str(self.tasksInfo['task_id'])))
            else:
                cur.execute("UPDATE %s SET `status` ='%s' where `id` = %s" % (
                myconfig.tasks_table, self.__status, str(self.tasksInfo['task_id'])))
            conn.commit()
            del cur
            conn.close()
        except Exception as e:
            self.logger.logMessage("Database Error: (updateTasksInfo %s" %(str(repr(e))), "ERROR")

    def checkTasksStatus(self):
        if self.stopped:
            return
        try:
            conn = self.database.getConnection()
        except Exception as e:
            return
        try:
            cur = conn.cursor()
            cur.execute("SELECT status FROM %s where `id` =%s and `status` like '%s';" %(myconfig.tasks_table, str(self.tasksInfo['task_id']), "STOPPED%"))
            if(cur.rowcount > 0):
                self.__status = cur.fetchone()[0]
                self.stopped = True
                self.logger.logMessage("TASK ID:("+ str(self.tasksInfo['task_id']) +") STOPPED WHILE RUNNING", "INFO")
            cur.execute("SELECT status FROM %s where `id` =%s and `status` like '%s';" %(myconfig.tasks_table, str(self.tasksInfo['task_id']), "COMPLETED%"))
            if(cur.rowcount > 0):
                self.__status = cur.fetchone()[0]
                self.stopped = True
                self.logger.logMessage("TASK ID:("+ str(self.tasksInfo['task_id']) +") GOT COMPLETED WHILE RUNNING", "INFO")
            cur.close()
            del cur
            conn.close()
        except Exception as e:
            self.logger.logMessage("Database Error: (checkTasksStatus) %s" %(str(repr(e))), "ERROR")


    def getStatus(self):
        self.checkTasksStatus()
        return self.__status

    def getTaskId(self):
        return self.tasksInfo['task_id']

    def getInfo(self):
        self.checkTasksStatus()
        self.checkRunTime()
        upTime = int(time.time()) - self.startUpTime
        return "STATUS: %s, UP TIME: %s, METHOD: %s, TASK ID: %s, NAME: %s " %(self.__status, str(upTime), self.tasksInfo['type'], str(self.tasksInfo['task_id']),self.tasksInfo['name'])

    def finish(self):
        self.completed = True
        self.__status= 'COMPLETED'
        if(self.errorLog != ''):
            self.logger.logMessage("Task ID:%s COMPLETED WITH SOME ERRORS:%s" %(str(self.tasksInfo['task_id']),self.errorLog), "INFO")
            self.updateTasksInfo()
        self.stopped = True

    def isCompleted(self):
        return self.completed

    def isStopped(self):
        return self.stopped

    def stop(self):
        if self.stopped:
            return
        self.logger.logMessage("STOPPING Task ID: %s WITH STATUS: %s" %(str(self.tasksInfo['task_id']), self.__status), "INFO")
        self.updateTasksInfo()
        self.stopped = True

    def rescheduleTask(self):
        self.__status= 'PENDING'
        self.message = "Rescheduling Tasks"
        self.logger.logMessage("Tasks: %s status: %s" %(str(self.tasksInfo['task_id']) ,self.__status), "INFO")
        try:
            self.updateTasksInfo()
            self.stopped = True
        except Exception as e:
            self.logger.logMessage("CAN NOT RESCHEDULE TasksID: %s ERROR: %s" %(str(self.tasksInfo['task_id']), str(repr(e))), "ERROR")


    def setStatus(self, status, message="no message"):
        self.__status = status
        self.message = message
        self.updateTasksInfo()

    def checkRunTime(self):
        if self.stopped:
            return
        upTime = int(time.time()) - self.startUpTime
        if upTime > myconfig.max_up_seconds_per_task:
            self.errorLog = 'Task TOOK LONGER THAN %s minutes' %(str(myconfig.max_up_seconds_per_task/60)) + self.errorLog
            try:
                if self.getPID() is not None:
                    os.kill(self.getPID(), SIGTERM)
                self.stop()
            except Exception as e:
                self.handleExceptions(e, True)
            self.handleExceptions(exception={'message':'Tasks TOOK LONGER THAN %s minutes' %(str(myconfig.max_up_seconds_per_task/60))})


    def handleExceptions(self, exception, terminate=True):
        self.errored = True
        if terminate:
            self.__status= 'STOPPED'
            #self.message= repr(exception).replace("'", "").replace('"', "")
            self.errorLog = self.errorLog  + str(exception).replace('\n',',').replace("'", "").replace('"', "") + ", "
            self.updateTasksInfo(True)
            self.stopped = True
        else:
            self.errorLog = self.errorLog + str(exception).replace('\n',',').replace("'", "").replace('"', "") + ", "
