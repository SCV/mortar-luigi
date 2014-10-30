import ConfigParser
import cStringIO as StringIO
import os.path
import tempfile
import unittest
import uuid

import luigi
import mock


from mortar.luigi import dbms

class TestExtractFromMySQL(unittest.TestCase):
    
    @mock.patch("mortar.luigi.dbms.luigi.configuration")
    def test_extract(self, mock_config):
        """
        Tests that the mysql extraction runs correctly.
        """
        output_file = os.path.join(tempfile.gettempdir(), 'extract-%s.txt' % uuid.uuid4().hex)
        conf = ConfigParser.ConfigParser()
        conf.add_section('mysql')
        conf.set('mysql', 'dbname', 'mydb')
        conf.set('mysql', 'host',   'myhost')
        conf.set('mysql', 'port', '3306')
        conf.set('mysql', 'user', 'myuser')
        conf.set('mysql', 'password', 'my_password')
        mock_config.get_config.return_value = conf
        with mock.patch("mortar.luigi.dbms.subprocess") as subprocess:
            subprocess_return = mock.Mock()
            subprocess_return.stdout = StringIO.StringIO()
            subprocess_return.stderr = StringIO.StringIO()
            subprocess_return.communicate.return_value = (None, None)
            subprocess_return.returncode = 0
            subprocess.Popen.return_value = subprocess_return

            t = dbms.ExtractFromMySQL(table='foo', output_path=output_file)
            self.assertFalse(t.output()[0].exists())
            luigi.build([t], local_scheduler=True)
            self.assertTrue(t.output()[0].exists())

            os.remove(output_file)


