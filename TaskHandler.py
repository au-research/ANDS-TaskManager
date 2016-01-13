try:
    import urllib.request as urllib2
except:
    import urllib2
import os
import json
from xml.dom.minidom import parseString
from datetime import datetime
import time
from xml.dom.minidom import Document
import numbers
import subprocess
import myconfig

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
    __tasksInfo = None
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
        self.__tasksInfo = tasksInfo
        self.logger = logger
        self.database = database
        self.updateTasksInfo()



    def runTask(self):
        self.doSimpleRequest()
        self.finish()


    def doSimpleRequest(self):
        if self.stopped:
            return
        try:
            # self.setStatus('SIMPLE REQUEST INITIATED')
            getRequest = Request(myconfig.response_url + str(self.__tasksInfo['task_id']) + '/?api_key=api')
            self.data = getRequest.getData()
            del getRequest
        except Exception as e:
            self.handleExceptions(e)

    def updateTasksInfo(self):
        self.checkTasksStatus()
        if self.stopped:
            return
        try:
            conn = self.database.getConnection()
        except Exception as e:
            self.logger.logMessage("Database Connection Error: %s" %(str(repr(e))))
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
            cur.execute("UPDATE %s SET `status` ='%s', `message` ='%s' where `id` = %s" %(myconfig.tasks_table, self.__status, json.dumps(statusDict).replace("'", "\\\'"), str(self.__tasksInfo['task_id'])))
            conn.commit()
            del cur
            conn.close()
        except Exception as e:
            self.logger.logMessage("Database Error: (updateTasksInfo %s" %(str(repr(e))))

    def checkTasksStatus(self):
        if self.stopped:
            return
        try:
            conn = self.database.getConnection()
        except Exception as e:
            return
        try:
            cur = conn.cursor()
            cur.execute("SELECT status FROM %s where `id` =%s and `status` like '%s';" %(myconfig.tasks_table, str(self.__tasksInfo['task_id']), "STOPPED%"))
            if(cur.rowcount > 0):
                self.__status = cur.fetchone()[0]
                self.stopped = True
                self.logger.logMessage("TASKS STOPPED WHILE RUNNING")
            cur.execute("SELECT status FROM %s where `id` =%s and `status` like '%s';" %(myconfig.tasks_table, str(self.__tasksInfo['task_id']), "COMPLETED%"))
            if(cur.rowcount > 0):
                self.__status = cur.fetchone()[0]
                self.stopped = True
                self.logger.logMessage("TASK COMPLETED")
            cur.close()
            del cur
            conn.close()
        except Exception as e:
            self.logger.logMessage("Database Error: (checkTasksStatus) %s" %(str(repr(e))))


    def getStatus(self):
        self.checkTasksStatus()
        return self.__status

    def getTaskId(self):
        return self.__tasksInfo['task_id']

    def getInfo(self):
        self.checkTasksStatus()
        self.checkRunTime()
        upTime = int(time.time()) - self.startUpTime
        return "STATUS: %s, UP TIME: %s, METHOD: %s, TASK ID: %s, NAME: %s " %(self.__status, str(upTime), self.__tasksInfo['type'], str(self.__tasksInfo['task_id']),self.__tasksInfo['name'])

    def finish(self):
        self.completed = True
        self.__status= 'COMPLETED'
        if(self.errorLog != ''):
            self.logger.logMessage("Task ID:%s COMPLETED WITH SOME ERRORS:%s" %(str(self.__tasksInfo['task_id']),self.errorLog))
        # self.updateTasksInfo()
        self.stopped = True

    def isCompleted(self):
        return self.completed

    def isStopped(self):
        return self.stopped

    def stop(self):
        if self.stopped:
            return
        self.logger.logMessage("STOPPING Task ID: %s WITH STATUS: %s" %(str(self.__tasksInfo['task_id']), self.__status))
        self.updateTasksInfo()
        self.stopped = True

    def rescheduleTask(self):
        self.__status= 'PENDING'
        self.message = "Rescheduling Tasks"
        self.logger.logMessage("Tasks: %s status: %s" %(str(self.__tasksInfo['task_id']) ,self.__status))
        try:
            self.updateTasksInfo()
            self.stopped = True
        except Exception as e:
            self.logger.logMessage("CAN NOT RESCHEDULE TasksID: %s ERROR: %s" %(str(self.__tasksInfo['task_id']), str(repr(e))))


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
            self.handleExceptions(exception={'message':'Tasks TOOK LONGER THAN %s minutes' %(str(myconfig.max_up_seconds_per_task/60))})


    def handleExceptions(self, exception, terminate=True):
        self.errored = True
        if terminate:
            self.__status= 'STOPPED'
            #self.message= repr(exception).replace("'", "").replace('"', "")
            self.errorLog = self.errorLog  + str(exception).replace('\n',',').replace("'", "").replace('"', "") + ", "
            self.updateTasksInfo()
            self.stopped = True
        else:
            self.errorLog = self.errorLog + str(exception).replace('\n',',').replace("'", "").replace('"', "") + ", "
