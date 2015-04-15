import unittest, luigi
from luigi import configuration
from mock import patch

from mortar.api.v2 import clusters


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

    def test_get_usable_cluster(self):
        t = TestMortarProjectTask()

        free_persistent = self._make_cluster()
        busy_persistent = self._make_cluster(num_jobs=2)
        busy_single_job = self._make_cluster(num_jobs=1, ctype=clusters.CLUSTER_TYPE_SINGLE_JOB)
        small = self._make_cluster(size=2)
        starting = self._make_cluster(status=clusters.CLUSTER_STATUS_STARTING)

        self.assertEquals([], 
                          t._get_usable_clusters(self._mock_api([]), 3))

        all_cluster_api = self._mock_api([free_persistent, busy_persistent, busy_single_job, small, starting])
        self.assertEquals([free_persistent],
                          t._get_usable_clusters(all_cluster_api, 3))

        t.share_running_cluster = True
        self.assertEquals([free_persistent, busy_persistent],
                          t._get_usable_clusters(all_cluster_api, 3))

    def _mock_api(self, clusters):
        return {'clusters': {'clusters': clusters }}

    def _make_cluster(self, size=5, status=clusters.CLUSTER_STATUS_RUNNING, 
                      ctype=clusters.CLUSTER_TYPE_PERSISTENT, num_jobs=0):
        return {
            'status_code': status,
            'cluster_type_code': ctype,
            'size': size,
            'running_jobs': [ 'foo-%s' % (i) for i in range(num_jobs) ]
        }