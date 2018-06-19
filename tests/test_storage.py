import unittest
import atexit
import shutil
import tempfile
from pywps import FORMATS
from pywps.inout.storage import DummyStorage, STORE_TYPE
from pywps.inout.storage.file import FileStorage
from pywps.inout.storage.db.pg import PgStorage
from pywps import ComplexOutput
import os

TEMP_DIRS=[]

def clear():
    """Delete temporary files
    """
    for d in TEMP_DIRS:
        shutil.rmtree(d)

atexit.register(clear)

def get_vector_file():

    return os.path.join(os.path.dirname(__file__), "data", "gml", "point.gml")

class DummyStorageTestCase(unittest.TestCase):
    """Storage test case
    """

    def setUp(self):
        global TEMP_DIRS
        tmp_dir = tempfile.mkdtemp()
        TEMP_DIRS.append(tmp_dir)

        self.storage = DummyStorage()

    def tearDown(self):
        pass

    def test_dummy_storage(self):
        assert isinstance(self.storage, DummyStorage)


    def test_store(self):
        vector_output = ComplexOutput('vector', 'Vector output',
                             supported_formats=[FORMATS.GML])
        vector_output.file = get_vector_file()
        assert not self.storage.store("some data")


class FileStorageTestCase(unittest.TestCase):
    """FileStorage tests
    """

    def setUp(self):
        global TEMP_DIRS
        tmp_dir = tempfile.mkdtemp()
        TEMP_DIRS.append(tmp_dir)

        self.storage = FileStorage()

    def tearDown(self):
        pass

    def test_file_storage(self):
        assert isinstance(self.storage, FileStorage)


    def test_store(self):
        vector_output = ComplexOutput('vector', 'Vector output',
                             supported_formats=[FORMATS.GML])
        vector_output.file = get_vector_file()

        self.assertEqual(len(self.storage.store(vector_output)), 3) 
        self.assertEqual(self.storage.store(vector_output)[0], STORE_TYPE.PATH)
        self.assertIsInstance(self.storage.store(vector_output)[1], str)
        self.assertIsInstance(self.storage.store(vector_output)[2], str)


class PgStorageTestCase(unittest.TestCase):
    """PgStorage test
    """

    def setUp(self):
        global TEMP_DIRS
        tmp_dir = tempfile.mkdtemp()
        TEMP_DIRS.append(tmp_dir)

        self.storage = PgStorage()

    def tearDown(self):
        pass

    def test_file_storage(self):
        assert isinstance(self.storage, PgStorage)


    def test_store(self):
        vector_output = ComplexOutput('vector', 'Vector output',
                             supported_formats=[FORMATS.GML])
        vector_output.file = get_vector_file()
        vector_output.output_format = FORMATS.GML


        self.assertEqual(self.storage.store(vector_output)[0], STORE_TYPE.DB)
