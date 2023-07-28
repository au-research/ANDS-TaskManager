import unittest
import myconfig
import requests
from utils.SlackUtils import SlackUtils

"""
https://rapidapi.com/KegenGuyll/api/dad-jokes/
free (50 pd)
"""
def get_joke_message():
    url = "https://dad-jokes.p.rapidapi.com/random/joke"

    if not(hasattr(myconfig, "X_RapidAPI_Key")) or myconfig.X_RapidAPI_Key.strip() == '':
        return "No X_RapidAPI_Key, No Jokes for you!"

    headers = {
        "X-RapidAPI-Key": myconfig.X_RapidAPI_Key,
        "X-RapidAPI-Host": "dad-jokes.p.rapidapi.com"
    }
    try:
        response = requests.get(url, headers=headers)
        joke = response.json()
        setup = joke['body'][0]["setup"]
        punchline = joke['body'][0]["punchline"]
        return "\n" + setup + '\n:thinking_face:\n' + punchline + ":zany_face:"
    except Exception:
        return "No Joke for you!"


class TestSlackUtils(unittest.TestCase):

    def test_post_message(self):
        slack_util = SlackUtils(myconfig.slack_channel_webhook_url, myconfig.slack_channel_id)
        task_info = {"ds_id": 4, "task_id": 92}
        message = get_joke_message()
        response = slack_util.post_message("Unit-Test Debug msg:" + message, task_info, "DEBUG")
        self.assertTrue(response == 200)
        message = get_joke_message()
        response = slack_util.post_message("Unit-Test Info msg:" + message, task_info, "INFO")
        self.assertTrue(response == 200)
        message = get_joke_message()
        response = slack_util.post_message("Unit-Test Error msg:" + message, task_info, "ERROR")
        self.assertTrue(response == 200)
        message = get_joke_message()
        response = slack_util.post_message("Unit-Test Resume msg:" + message, task_info, "RESUME")
        self.assertTrue(response == 200)


