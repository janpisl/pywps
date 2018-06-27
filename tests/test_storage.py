import unittest
import atexit
import shutil
import tempfile
from pywps import FORMATS
from pywps.inout.storage import DummyStorage, STORE_TYPE
from pywps.inout.storage.file import FileStorage
from pywps.inout.storage.db.pg import PgStorage
from pywps.inout.storage.db.sqlite import SQLiteStorage
from pywps.inout.storage.db import DbStorage
from pywps import ComplexOutput
import os
from pywps import configuration

TEMP_DIRS=[]

def clear():
    """Delete temporary files
    """
    for d in TEMP_DIRS:
        shutil.rmtree(d)

atexit.register(clear)

def get_vector_file():

    return os.path.join(os.path.dirname(__file__), "data", "gml", "point.gml")

def get_raster_file():

    return os.path.join(os.path.dirname(__file__), "data", "geotiff", "dem.tiff")


def get_other_file():

    return os.path.join(os.path.dirname(__file__), "data", "other", "test.txt")

def get_connstr():

    dbsettings = "db"
    target = "dbname={} user={} password={} host={} port={}".format(
        configuration.get_config_value(dbsettings, "dbname"),
        configuration.get_config_value(dbsettings, "user"),
        configuration.get_config_value(dbsettings, "password"),
        configuration.get_config_value(dbsettings, "host"),
        configuration.get_config_value(dbsettings, "port")
    )
    return target

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

        store_file = self.storage.store(vector_output)        
        assert len(store_file) == 3
        assert store_file[0] == STORE_TYPE.PATH
        assert isinstance(store_file[1], str)
        assert isinstance(store_file[2], str)


class PgStorageTestCase(unittest.TestCase):
    """PgStorage test
    """

    def setUp(self):
        global TEMP_DIRS
        tmp_dir = tempfile.mkdtemp()
        TEMP_DIRS.append(tmp_dir)
        self.storage = PgStorage()

        configuration.CONFIG.set("server", "store_type", "db")
        #when add_section('db') -> duplicate error, section db exists ; if not -> no section db ; section created in configuration.py
        #configuration.CONFIG.add_section('db')
        configuration.CONFIG.set("db", "db_type", "pg")
        configuration.CONFIG.set("db", "dbname", "pisl")
        configuration.CONFIG.set("db", "user", "pisl")
        configuration.CONFIG.set("db", "password", "password")
        configuration.CONFIG.set("db", "host", "localhost")
        configuration.CONFIG.set("db", "port", "5432")
        configuration.CONFIG.set("db", "schema_name", "testovaci_schema")
        
        #this does not work:
        self.storage.target = get_connstr()

        self.storage.schema_name = configuration.get_config_value("db", "schema_name")
        self.storage.dbname = configuration.get_config_value("db", "dbname")

    def tearDown(self):
        pass

    def test_pg_storage(self):
        assert isinstance(self.storage, PgStorage)


    def test_store_vector(self):
        vector_output = ComplexOutput('vector', 'Vector output',
                             supported_formats=[FORMATS.GML])
        vector_output.file = get_vector_file()
        vector_output.output_format = FORMATS.GML
        store_vector = self.storage.store(vector_output)
        assert len(store_vector) == 3
        assert store_vector[0] == STORE_TYPE.DB
        assert isinstance(store_vector[1], str)
        assert isinstance(store_vector[2], str)


    def test_store_raster(self):
        raster_output = ComplexOutput('raster', 'Raster output',
                             supported_formats=[FORMATS.GEOTIFF])
        raster_output.file = get_raster_file()
        raster_output.output_format = FORMATS.GEOTIFF

        store_raster = self.storage.store(raster_output)
        assert len(store_raster) == 3
        assert store_raster[0] == STORE_TYPE.DB
        assert isinstance(store_raster[1], str)
        assert isinstance(store_raster[2], str)


    def test_store_other(self):
        other_output = ComplexOutput('txt', 'Other output',
                             supported_formats=[FORMATS.TEXT])
        other_output.file = get_other_file()
        other_output.output_format = FORMATS.TEXT


        store_other = self.storage.store(other_output)
        assert len(store_other) == 3
        assert store_other[0] == STORE_TYPE.DB
        assert isinstance(store_other[1], str)
        assert isinstance(store_other[2], str)


class SQLiteStorageTestCase(unittest.TestCase):
    """PgStorage test
    """

    def setUp(self):
        global TEMP_DIRS
        tmp_dir = tempfile.mkdtemp()
        TEMP_DIRS.append(tmp_dir)

        self.storage = SQLiteStorage()
        configuration.CONFIG.set("db", "dblocation", "/mnt/c/Users/Jan/Documents/GitHub/test9.sqlite")
        self.storage.dblocation = configuration.get_config_value("db", "dblocation")


    def tearDown(self):
        pass

    def test_sqlite_storage(self):
        assert isinstance(self.storage, SQLiteStorage)


    def test_store_vector(self):
        vector_output = ComplexOutput('vector', 'Vector output',
                             supported_formats=[FORMATS.GML])
        vector_output.file = get_vector_file()
        vector_output.output_format = FORMATS.GML
        store_vector = self.storage.store(vector_output)
        assert len(store_vector) == 3
        assert store_vector[0] == STORE_TYPE.DB
        assert isinstance(store_vector[1], str)
        assert isinstance(store_vector[2], str)


    def test_store_raster(self):
        raster_output = ComplexOutput('raster', 'Raster output',
                             supported_formats=[FORMATS.GEOTIFF])
        raster_output.file = get_raster_file()
        raster_output.output_format = FORMATS.GEOTIFF

        store_raster = self.storage.store(raster_output)
        assert len(store_raster) == 3
        assert store_raster[0] == STORE_TYPE.DB
        assert isinstance(store_raster[1], str)
        assert isinstance(store_raster[2], str)


    def test_store_other(self):
        other_output = ComplexOutput('txt', 'Other output',
                             supported_formats=[FORMATS.TEXT])
        other_output.file = get_other_file()
        other_output.output_format = FORMATS.TEXT


        store_other = self.storage.store(other_output)
        assert len(store_other) == 3
        assert store_other[0] == STORE_TYPE.DB
        assert isinstance(store_other[1], str)
        assert isinstance(store_other[2], str)


