from TaskHandler import *

class PHPSHELLTaskHandler(TaskHandler):
    """
       {
            "id": "PHPSHELL__runningTasksHandler",
            "title": "THE PHP SHELL __runningTasks handler",
            "description": "call PHP from the command line instead of request",
            "params": [
                {"name": "task_id", "required": "true"},
            ]
      }
    """
    phpShell = None

    def runTask(self):
        self.phpShell = PHPShell(str(self.tasksInfo['task_id']))
        try:
            self.phpShell.run()
        except Exception as e:
            self.handleExceptions(e, True)
            self.phpShell.stop()
            self.logger.logMessage("ERROR WHILE RUNNING COMMAND %s" %(e), "ERROR")
        self.finish()

    def getPID(self):
        return self.phpShell.getPID()