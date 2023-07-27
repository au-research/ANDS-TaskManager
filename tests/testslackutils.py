import unittest
import myconfig

from utils.SlackUtils import SlackUtils


class TestSlackUtils(unittest.TestCase):

    def test_post_message(self):
        slackutil = SlackUtils(myconfig.slack_channel_webhook_url, myconfig.slack_channel_id)
        task_info = {"ds_id": 4, "task_id": 92}
        slackutil.post_message("Debug message form Taskmanager", task_info, "DEBUG")
        slackutil.post_message("Info message from Taskmanager", task_info, "INFO")
        slackutil.post_message("Error message form Taskmanager", task_info, "ERROR")
        slackutil.post_message("Task took longer than 2 hours", task_info, "RESUME")
