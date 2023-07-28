import sys
import requests
from requests.adapters import HTTPAdapter
import urllib3
import myconfig


class SlackUtils:
    webhook_url = None
    channel_id = None
    logLevels = {"RESUME": 101, 'ERROR': 100, 'INFO': 50, 'DEBUG': 10}
    logLevel = 100

    def __init__(self, webhook_url, channel_id):
        self.retryCount = 3
        urllib3.disable_warnings()
        self.webhook_url = webhook_url
        self.channel_id = channel_id
        self.logLevel = self.logLevels[myconfig.slack_channel_notification_level]

    def post_message(self, text, task_info, message_type='INFO'):
        """
        Send Messages to the configured Slack channel
        """
        # if not set fake the 200 response code
        if not self.webhook_url:
            return 200
        if self.logLevels[message_type] < self.logLevel:
            return 200
        colour = "#00AA00"
        if message_type == 'ERROR':
            colour = "#AA0000"
        elif message_type == 'RESUME':
            colour = "#FFBF00"
        elif message_type == 'DEBUG':
            colour = "#0000AA"
        http_adapter = HTTPAdapter(max_retries=self.retryCount)
        session = requests.Session()
        session.mount(self.webhook_url, http_adapter)
        action_text = "View the <" + myconfig.slack_registry_datasource_view_url + str(task_info['ds_id']) \
                      + "|DataSource> for more details"
        if message_type == "RESUME":
            action_text = "Click <" + myconfig.slack_api_registry_task_url + str(task_info['task_id']) \
                          + "/resume|here> to Resume Task"

        data = {
            "channel": self.channel_id,
            "text": myconfig.slack_app_name + " " + message_type,
            "attachments": [
                {
                    "type": "mrkdwn",
                    "text": text,
                    "color": colour
                },
                {
                    "type": "mrkdwn",
                    "text": action_text,
                    "color": colour
                }
            ]
        }
        try:
            header = {'User-Agent': 'ARDC Taskmanager'}
            response = session.post(self.webhook_url, json=data, headers=header, verify=False)
            response.raise_for_status()
            session.close()
            return response.status_code
        except Exception:
            e = sys.exc_info()[1]
            session.close()
            return repr(e)


