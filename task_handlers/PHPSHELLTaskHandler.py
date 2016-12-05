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

    def runTask(self):
        try:
            phpShell = PHPShell(str(self.tasksInfo['task_id']))
            phpShell.run()
            self.finish()
        except subprocess.CalledProcessError as e:
            self.logger.logMessage("ERROR WHILE RUNNING COMMAND %s " %(e.output.decode()))
        except Exception as e:
            self.logger.logMessage("ERROR WHILE RUNNING COMMAND %s" %(e))