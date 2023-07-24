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
from utils.SlackUtils import SlackUtils


def update_slack_channel(message, data_source_id, log_level):
    slack_util = SlackUtils(myconfig.slack_channel_webhook_url, myconfig.slack_channel_id)
    slack_util.post_message(message, data_source_id, log_level)


class TaskHandler:
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
        self.update_tasks_info()

    def update_tasks_info(self, updateMessage=False):
        if not updateMessage:
            self.check_tasks_status()
        if self.stopped:
            return
        attempts = 0
        while attempts < 3:
            try:
                conn = self.database.get_connection()
                cur = conn.cursor()
                upTime = int(time.time()) - self.startUpTime
                statusDict = {'status': self.__status,
                              'message': self.message,
                              'error': {'log': str.strip(self.errorLog), 'errored': self.errored},
                              'completed': str(self.completed),
                              'output': {'file': self.outputFilePath, 'dir': self.outputDir},
                              'progress': {'time': str(upTime), 'start': str(self.startUpTime), 'end': ''}
                              }
                if updateMessage:
                    cur.execute("UPDATE %s SET `status` ='%s', `message` = '%s' where `id` = %s" % (
                        myconfig.tasks_table, self.__status, json.dumps(statusDict).replace("'", "\\\'"),
                        str(self.tasksInfo['task_id'])))
                else:
                    cur.execute("UPDATE %s SET `status` ='%s' where `id` = %s" % (
                        myconfig.tasks_table, self.__status, str(self.tasksInfo['task_id'])))
                conn.commit()
                del cur
                conn.close()
                break
            except Exception as e:
                attempts += 1
                time.sleep(5)
                self.logger.log_message("(updateTasksInfo) %s, Retry: %d" % (str(repr(e)), attempts), "ERROR")

    def check_tasks_status(self):
        if self.stopped:
            return
        attempts = 0
        while attempts < 3:
            try:
                conn = self.database.get_connection()
                cur = conn.cursor()
                cur.execute("SELECT status FROM %s where `id` =%s and `status` like '%s';" % (
                    myconfig.tasks_table, str(self.tasksInfo['task_id']), "STOPPED%"))
                if cur.rowcount > 0:
                    self.__status = cur.fetchone()[0]
                    self.stopped = True
                    self.logger.log_message("TASK ID:(" + str(self.tasksInfo['task_id']) + ") STOPPED", "INFO")
                cur.execute("SELECT status FROM %s where `id` =%s and `status` like '%s';" % (
                    myconfig.tasks_table, str(self.tasksInfo['task_id']), "COMPLETED%"))
                if cur.rowcount > 0:
                    self.__status = cur.fetchone()[0]
                    self.stopped = True
                    self.logger.log_message(
                        "TASK ID:(" + str(self.tasksInfo['task_id']) + ") GOT COMPLETED WHILE RUNNING", "INFO")
                cur.close()
                del cur
                conn.close()
                break
            except Exception as e:
                attempts += 1
                time.sleep(5)
                self.logger.log_message("(checkTasksStatus) %s, Retry: %d" % (str(repr(e)), attempts), "ERROR")

    def get_status(self):
        self.check_tasks_status()
        return self.__status

    def get_pid(self):
        return self.get_pid()

    def get_task_id(self):
        return self.tasksInfo['task_id']

    def get_info(self):
        self.check_tasks_status()
        self.check_run_time()
        upTime = int(time.time()) - self.startUpTime
        return "STATUS: %s, UP TIME: %s, METHOD: %s, TASK ID: %s, NAME: %s " % (
            self.__status, str(upTime), self.tasksInfo['type'], str(self.tasksInfo['task_id']), self.tasksInfo['name'])

    def finish(self):
        self.completed = True
        self.__status = 'COMPLETED'
        if self.errorLog != '':
            self.logger.log_message(
                "Task ID:%s COMPLETED WITH SOME ERRORS:%s" % (str(self.tasksInfo['task_id']), self.errorLog), "INFO")
            self.update_tasks_info()
        self.stopped = True

    def is_completed(self):
        return self.completed

    def is_stopped(self):
        return self.stopped

    def stop(self):
        if self.stopped:
            return
        self.logger.log_message("STOPPING Task ID: %s WITH STATUS: %s"
                                % (str(self.tasksInfo['task_id']), self.__status), "INFO")
        self.update_tasks_info(True)
        self.stopped = True

    def reschedule_task(self):
        self.__status = 'PENDING'
        self.message = "Rescheduling Tasks"
        self.logger.log_message("Tasks: %s status: %s" % (str(self.tasksInfo['task_id']), self.__status), "INFO")
        try:
            self.update_tasks_info()
            self.stopped = True
        except Exception as e:
            self.logger.log_message(
                "CAN NOT RESCHEDULE TasksID: %s ERROR: %s" % (str(self.tasksInfo['task_id']), str(repr(e))), "ERROR")

    def set_status(self, status, message="no message"):
        self.__status = status
        self.message = message
        self.update_tasks_info()

    def check_run_time(self):
        if self.stopped:
            return
        upTime = int(time.time()) - self.startUpTime
        if upTime > myconfig.max_up_seconds_per_task:
            self.errorLog = 'Task TOOK LONGER THAN %s minutes' % (
                str(myconfig.max_up_seconds_per_task / 60)) + self.errorLog
            try:

                if self.get_pid() is not None:
                    os.kill(self.get_pid(), SIGTERM)
                self.stop()
            except Exception as e:
                self.handle_exceptions(e, True)
            self.handle_exceptions(exception={
                'message': 'Tasks TOOK LONGER THAN %s minutes' % (str(myconfig.max_up_seconds_per_task / 60))})

    def handle_exceptions(self, exception, terminate=True):
        self.errored = True
        if terminate:
            self.__status = 'STOPPED'
            # self.message= repr(exception).replace("'", "").replace('"', "")
            self.errorLog = self.errorLog + str(exception).replace('\n', ',').replace("'", "").replace('"', "") + ", "
            self.update_tasks_info(True)
            self.stopped = True
            update_slack_channel(str(exception).replace('\n', ',').replace("'", "").replace('"', ""), self.tasksInfo['ds_id'], "ERROR")
        else:
            self.errorLog = self.errorLog + str(exception).replace('\n', ',').replace("'", "").replace('"', "") + ", "
