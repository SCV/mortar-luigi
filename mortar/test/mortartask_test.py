import unittest, luigi
from luigi import configuration
from mock import patch

from mortar.api.v2 import API

from mortar.luigi.mortartask import MortarTask, MortarProjectTask

PROJECT_NAME = 'projectName'

class TestMortarProjectTask(MortarProjectTask):
    def is_control_script(self):
        return False

    def script(self):
        return 'my_pig_script'

    def script_output(self):
        return 'NO'

class TestMortarTaskProject(unittest.TestCase):
    @patch("luigi.configuration")
    def TestMortarProjectInConfig(self, mock_config):
        mock_config.get_config.return_value.get.return_value = PROJECT_NAME
        t = TestMortarProjectTask()
        self.assertEquals(PROJECT_NAME, t.project())
    def TestMortarProjectException(self):
        t = TestMortarProjectTask()
        self.assertRaises(RuntimeError, lambda: t.project())
