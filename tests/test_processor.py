import unittest
import myconfig
from task_processor_daemon import TasksManager


class TestProcessor(unittest.TestCase):

    def test_creation(self):
        tm = TasksManager()
        task_info = [136, "Harvester initiated import - a-services(503000) - 4B8960A42308C5904B4C402C454CE6EFC25D56DF",
                     "PHPSHELL", "PENDING", "ModuleNotFoundError(No module named PHPSHELLTaskHandler,)",
                     "2023-07-26 02:54:41", "0000-00-00 00:00:00", "null", "null",
                     "class=import&ds_id=503000&batch_id=4B8960A42308C5904B4C402C454CE6EFC25D56DF&harvest_id=3&source=harvester",
                     "{\"log_path\":null}"]
        tm.queue_task(task_row=task_info)
        #tm.manage_tasks()
