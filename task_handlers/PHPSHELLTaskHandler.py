from utils.PHPShell import PHPShell
from utils.TaskHandler import TaskHandler


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

    def run_task(self):
        self.phpShell = PHPShell(str(self.tasksInfo['task_id']))
        try:
            self.phpShell.run()
        except Exception as e:
            self.handle_exceptions(e, True)
            self.phpShell.stop()
            self.logger.log_message("ERROR WHILE RUNNING COMMAND %s" % e, "ERROR")
        self.finish()

    def get_pid(self):
        return self.phpShell.get_pid()

    def get_shell(self):
        return self.phpShell
