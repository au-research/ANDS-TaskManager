from TaskHandler import *

class MAINTENANCETaskHandler(TaskHandler):
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
        self.doMainytenanceRequest()
        self.finish()

    def doMainytenanceRequest(self):
        if self.stopped:
            return
        try:
            # self.setStatus('SIMPLE REQUEST INITIATED')
            getRequest = Request(myconfig.maintenance_request_url + '/?api_key=api')
            self.data = getRequest.getData()
            self.logger.logMessage("Maintenance task result: %s" %str(self.data))
            del getRequest
        except Exception as e:
            self.handleExceptions(e)