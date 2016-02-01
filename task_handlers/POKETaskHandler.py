from TaskHandler import *

class POKETaskHandler(TaskHandler):
    """
       {
            "id": "POKE__runningTasksHandler",
            "title": "THE POKE (HIT and RUN) __runningTasks handler",
            "description": "simple hit a URL and record response code if not 200",
            "params": [
                {"name": "uri", "required": "true"},
            ]
      }
    """
    def runTask(self):
        self.doSimpleRequest()
        #self.finish()