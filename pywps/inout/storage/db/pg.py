##################################################################
# Copyright 2018 Open Source Geospatial Foundation and others    #
# licensed under MIT, Please consult LICENSE.txt for details     #
##################################################################

import logging
from pywps import configuration as config
from .. import DbStorageAbstract, STORE_TYPE
import psycopg2


LOGGER = logging.getLogger('PYWPS')

class PgStorage(DbStorageAbstract):

    def __init__(self):
        # TODO: more databases in config file
        # create connection string
        dbsettings = "db"
        self.dbname = config.get_config_value(dbsettings, "dbname")
        self.target = "dbname={} user={} password={} host={} port={}".format(
            self.dbname,
            config.get_config_value(dbsettings, "user"),
            config.get_config_value(dbsettings, "password"),
            config.get_config_value(dbsettings, "host"),
            config.get_config_value(dbsettings, "port")
        )

        self.schema_name = self._create_schema()


    def _create_schema(self):
        """ Generates random schema name, connects to PostGIS database and creates schema 
        """
        import random
        import string

        # random schema consisting of letters and digits 
        N = 10
        schema_name = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(N))
        # process based schema (TODO)
        # schema_name = '{}_{}'.format(identifier.lower(),
        #                              str(uuid).lower()
        # )

        # connect to a database and create schema 
        try:
            conn = psycopg2.connect(self.target)
        except:
            raise Exception ("Database connection has not been established.")
        cur = conn.cursor()
        query = 'CREATE SCHEMA IF NOT EXISTS "{}";'.format(schema_name)
        try:
            cur.execute(query)
        except:
            raise Exception("The query did not run succesfully.")
        conn.commit()
        cur.close()
        conn.close()

        return schema_name            


    def store_vector_output(self, file_name, identifier):
        """ Opens output file, connects to PostGIS database and copies data there
        """ 
        from osgeo import ogr
        # connect to a database and copy output there
        LOGGER.debug("Connect string: {}".format(self.target))
        dsc_in = ogr.Open(file_name)

        if dsc_in is None:
            raise Exception("Reading data failed.")
        dsc_out = ogr.Open("PG:" + self.target)
        if dsc_out is None:
            raise Exception("Database connection has not been established.")
        layer = dsc_out.CopyLayer(dsc_in.GetLayer(), identifier,
                                  ['OVERWRITE=YES',
                                   'SCHEMA={}'.format(self.schema_name)]
        )
        if layer is None:
            raise Exception("Writing output data to the database failed.")
        # returns process identifier (defined within the process)
        return identifier


    def store_raster_output(self, file_name, identifier):

        from subprocess import call

        try:
            call(["raster2pgsql", file_name, self.schema_name +  "." + identifier, "|", "psql", "-d", self.dbname])
        except:
            raise Exception("Writing output data to the database failed.")

        return identifier


    def store_other_output(self, file_name, identifier, uuid):

        import sqlalchemy
        from sqlalchemy.schema import CreateSchema
            
        db = sqlalchemy.create_engine('postgresql+psycopg2://pisl:password@localhost:5432/pisl')


        create_table = "CREATE TABLE IF NOT EXISTS {} (uuid text, data bytea, time_stamp datetime)".format(self.schema_name.identifier)
        insert_into_table = "INSERT INTO {} (uuid, data, time_stamp) VALUES ({}, {}, {})".format(self.schema_name.identifier, uuid, file_name, time_stamp)

        db.execute(create_table)  
        db.execute(insert_into_table)

        return identifier


    def store(self, output):
        """ Creates reference that is returned to the client (database name, schema name, table name)
        """

        assert(output.output_format.data_type in (0,1,2))

        if output.output_format.data_type == 0:
            self.store_vector_output(output.file, output.identifier)
        elif output.output_format.data_type == 1:
            self.store_raster_output(output.file, output.identifier)
        else:
            self.store_other_output(output.file, output.identifier, output.uuid)


        url = '{}.{}.{}'.format(self.dbname, self.schema_name, output.identifier)
        # returns value for database storage defined in the STORE_TYPE class,        
        # name of the output file and a reference
        return (STORE_TYPE.DB, output.file, url)

