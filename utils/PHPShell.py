try:
    import urllib.request as urllib2
except:
    import urllib2
from subprocess import Popen, PIPE
import myconfig


class PHPShell:
    data = None
    cmd = None
    taskId = None
    method = None
    wd = None
    pid = None

    def __init__(self, task_id):
        self.cmd = "php index.php api task"
        self.taskId = task_id
        self.wd = myconfig.php_shell_working_dir

    def stop(self):
        shell_command = self.cmd
        self.method = "stop"
        shell_command += " " + self.method + " " + self.taskId
        try:
            proc = Popen(shell_command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True,
                         cwd=self.wd)
            (output, error) = proc.communicate()
            if proc.returncode != 0 or error != "":
                raise Exception(
                    "Stopping the Task stopped by error code: %s, message: %s" % (str(proc.returncode), error))
        except Exception as e:
            raise Exception(str(e))

    def get_pid(self):
        return self.pid

    def run(self):
        shell_command = self.cmd
        self.method = "exe"
        shell_command += " " + self.method + " " + self.taskId
        try:
            proc = Popen(shell_command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True,
                         cwd=self.wd)
            self.pid = proc.pid
            (output, error) = proc.communicate()
            if proc.returncode != 0 or error != "":
                raise Exception("Task (%s) stopped by error code: %s, message: %s"
                                % (str(self.taskId), str(proc.returncode), error))
        except Exception as e:
            raise Exception(str(e))
