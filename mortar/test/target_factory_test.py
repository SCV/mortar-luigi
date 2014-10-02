import mock
import os
import tempfile
import unittest
from mortar.luigi import target_factory

class TestTargetFactory(unittest.TestCase):

    def setUp(self):
        self.data = 'someoutputdata'
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.tmp.write(self.data)
        self.tmp.close()

    def tearDown(self):
        if self.tmp:
            os.remove(self.tmp.name)
            self.tmp = None

    @mock.patch('mortar.luigi.target_factory.S3Target')
    def test_get_target_s3(self, mock_s3_target_cls):
        mock_s3_target = mock.Mock()
        mock_s3_target_cls.return_value = mock_s3_target
        s3_target = target_factory.get_target('s3://mybucket/mypath')
        self.assertEquals(mock_s3_target, s3_target)

    def test_get_target_local(self):
        local_target = target_factory.get_target(self.tmp.name)
        reread_data = local_target.open().read()
        self.assertEquals(self.data, reread_data)

    def test_get_target_file(self):
        file_target = target_factory.get_target('file://%s' % self.tmp.name)
        reread_data = file_target.open().read()
        self.assertEquals(self.data, reread_data)        