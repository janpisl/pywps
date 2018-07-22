import unittest
import atexit
import shutil
import tempfile
import sqlalchemy
from sqlalchemy import create_engine, inspect
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


def get_text_file():

    return os.path.join(os.path.dirname(__file__), "data", "other", "test.txt")

def get_csv_file():

    return os.path.join(os.path.dirname(__file__), "data", "other", "corn.csv")




def set_test_configuration():

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

        set_test_configuration()


        dbsettings = "db"
        self.dbname = configuration.get_config_value(dbsettings, "dbname")
        self.user = configuration.get_config_value(dbsettings, "user")
        self.password = configuration.get_config_value(dbsettings, "password")
        self.host = configuration.get_config_value(dbsettings, "host")
        self.port = configuration.get_config_value(dbsettings, "port")


        
        self.storage.target = "dbname={} user={} password={} host={} port={}".format(
            self.dbname, self.user, self.password, self.host, self.port
        )


        self.storage.schema_name = configuration.get_config_value("db", "schema_name")
        self.storage.dbname = configuration.get_config_value("db", "dbname")

    def tearDown(self):
        pass

    def test_pg_storage(self):
        assert isinstance(self.storage, PgStorage)


    def test_store_vector(self):

        import mappyfile
        import psycopg2


        d = mappyfile.open('/mnt/c/Users/Jan/Documents/GitHub/pywps/pywps/inout/storage/db/mapserver/template.map')


        d["NAME"] = "testpg1"


        d["WEB"]["METADATA"]["wms_title"] = "testpg2"
        d["WEB"]["METADATA"]["wms_onlineresource"] = "http://127.0.0.1/cgi-bin/mapserv?map=/mnt/c/Users/Jan/Documents/GitHub/pywps/pywps/inout/storage/db/mapserver/pg.map"

        d["LAYERS"][0]["NAME"] = "test"
        d["LAYERS"][0]["CONNECTIONTYPE"] = "postgis"
        d["LAYERS"][0]["CONNECTION"] = self.storage.target
        d["LAYERS"][0]["DATA"] = "wkb_geometry from " + "buff_out" #TODO: replace hard coded "buff_out"
        d["LAYERS"][0]["CLASSES"][0]["NAME"] = "testpg4" 

        # Connect to an existing database
        conn = psycopg2.connect(self.storage.target) 
        cur = conn.cursor()

        # extent
        cur.execute("SELECT ST_Extent(wkb_geometry) FROM buff_out;") #TODO: replace hard coded "buff_out"
        fextent = cur.fetchone()
        # reformat bbox (from tuple to string, remove brackets, replace "," with a " ") so it can be used in the mapfile
        fextent_formatted = fextent[0][fextent[0].find("(")+1:fextent[0].find(")")].replace(",", " ")
        d["EXTENT"] = fextent_formatted

        # projection (returns 0)
        cur.execute("SELECT Find_SRID('public', 'buff_out', 'wkb_geometry');")
        fSRID = str(cur.fetchone()[0])
        #if SRID is missing, use default value (WGS84)
        if fSRID is None or fSRID == "0":
            fSRID = "4326"
        d["PROJECTION"] =  ["init=epsg:" + fSRID] 
        d["WEB"]["METADATA"]["wms_srs"] = "EPSG:" + fSRID 


        # geometry
        cur.execute("SELECT ST_AsText(wkb_geometry) from buff_out;")
        fgeom = cur.fetchone()
        d["LAYERS"][0]["type"] = fgeom[0].split("(", 1)[0]


        # Make the changes to the database persistent
        conn.commit()
        # Close communication with the database
        cur.close()


        fn = r"/mnt/c/Users/Jan/Documents/GitHub/test_pg_vector.map"
        mappyfile.save(d, fn)


        vector_output = ComplexOutput('vector', 'Vector output',
                             supported_formats=[FORMATS.GML])
        vector_output.file = get_vector_file()
        vector_output.output_format = FORMATS.GML
        store_vector = self.storage.store(vector_output)



        assert len(store_vector) == 3
        assert store_vector[0] == STORE_TYPE.DB
        assert isinstance(store_vector[1], str)
        assert isinstance(store_vector[2], str)
        
        # Parse reference into dbname, schema and table
        reference = store_vector[2].split(".")

        db_url = "postgresql://{}:{}@{}:{}/{}".format(
            reference[0], self.password, self.host, self.port, self.user
        )
        engine = create_engine(db_url)
        # check if table exists
        ins = inspect(engine)
        assert (reference[2] in ins.get_table_names(schema=reference[1]))


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

        # Parse reference into dbname, schema and table
        reference = store_raster[2].split(".")

        db_url = "postgresql://{}:{}@{}:{}/{}".format(
            reference[0], self.password, self.host, self.port, self.user
        )
        engine = create_engine(db_url)
        # check if table exists
        ins = inspect(engine)
        assert (reference[2] in ins.get_table_names(schema=reference[1]))


    def test_store_other(self):
        text_output = ComplexOutput('txt', 'Plain text output',
                             supported_formats=[FORMATS.TEXT])
        text_output.file = get_text_file()
        text_output.output_format = FORMATS.TEXT

        store_text = self.storage.store(text_output)

        assert len(store_text) == 3
        assert store_text[0] == STORE_TYPE.DB
        assert isinstance(store_text[1], str)
        assert isinstance(store_text[2], str)

        # Parse reference into dbname, schema and table
        reference = store_text[2].split(".")

        db_url = "postgresql://{}:{}@{}:{}/{}".format(
            reference[0], self.password, self.host, self.port, self.user
        )
        engine = create_engine(db_url)
        # check if table exists
        ins = inspect(engine)
        assert (reference[2] in ins.get_table_names(schema=reference[1]))


        csv_output = ComplexOutput('csv', 'CSV output',
                             supported_formats=[FORMATS.CSV])
        csv_output.file = get_csv_file()
        csv_output.output_format = FORMATS.CSV

        store_csv = self.storage.store(csv_output)

        assert len(store_csv) == 3
        assert store_csv[0] == STORE_TYPE.DB
        assert isinstance(store_csv[1], str)
        assert isinstance(store_csv[2], str)

        # Parse reference into dbname, schema and table
        reference = store_csv[2].split(".")

        db_url = "postgresql://{}:{}@{}:{}/{}".format(
            reference[0], self.password, self.host, self.port, self.user
        )
        engine = create_engine(db_url)
        # check if table exists
        ins = inspect(engine)
        assert (reference[2] in ins.get_table_names(schema=reference[1]))


class SQLiteStorageTestCase(unittest.TestCase):
    """SQLiteStorage test
    """

    def setUp(self):
        global TEMP_DIRS
        tmp_dir = tempfile.mkdtemp()
        TEMP_DIRS.append(tmp_dir)

        self.storage = SQLiteStorage()
        self.storage.target = tempfile.mktemp(suffix='.sqlite', prefix='pywpsdb-')


    def tearDown(self):
        # Delete temp file if exists
        try:
            os.remove(self.storage.target)
        except:
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

        # Parse reference into path to db and table
        reference = store_vector[2].rsplit(".", 1)

        db_url = "sqlite:///{}".format(reference[0])
        engine = create_engine(db_url)
        # check if table exists
        ins = inspect(engine)
        assert (reference[1] in ins.get_table_names())


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

        # Parse reference into path to db and table
        reference = store_raster[2].rsplit(".", 1)

        db_url = "sqlite:///{}".format(reference[0])
        engine = create_engine(db_url)
        # check if table exists
        ins = inspect(engine)

        assert (reference[1] + "_rasters") in ins.get_table_names()



    def test_store_other(self):

        # Test text output
        text_output = ComplexOutput('txt', 'Plain text output',
                             supported_formats=[FORMATS.TEXT])
        text_output.file = get_text_file()
        text_output.output_format = FORMATS.TEXT

        store_text = self.storage.store(text_output)

        assert len(store_text) == 3
        assert store_text[0] == STORE_TYPE.DB
        assert isinstance(store_text[1], str)
        assert isinstance(store_text[2], str)

        # Parse reference into path to db and table
        reference = store_text[2].rsplit(".", 1)

        db_url = "sqlite:///{}".format(reference[0])
        engine = create_engine(db_url)
        # check if table exists
        ins = inspect(engine)
        assert (reference[1] in ins.get_table_names())


        # Test CSV output
        csv_output = ComplexOutput('csv', 'CSV output',
                             supported_formats=[FORMATS.CSV])
        csv_output.file = get_csv_file()
        csv_output.output_format = FORMATS.CSV

        store_csv = self.storage.store(csv_output)

        assert len(store_csv) == 3
        assert store_csv[0] == STORE_TYPE.DB
        assert isinstance(store_csv[1], str)
        assert isinstance(store_csv[2], str)

        # Parse reference into path to db and table
        reference = store_csv[2].rsplit(".", 1)

        db_url = "sqlite:///{}".format(reference[0])

        engine = create_engine(db_url)
        # check if table exists
        ins = inspect(engine)
        assert (reference[1] in ins.get_table_names())

