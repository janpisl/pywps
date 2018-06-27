##################################################################
# Copyright 2018 Open Source Geospatial Foundation and others    #
# licensed under MIT, Please consult LICENSE.txt for details     #
##################################################################

import logging
from pywps import configuration as config
from .. import DbStorageAbstract, STORE_TYPE


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

        self.schema_name = config.get_config_value(dbsettings, "schema_name")  


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

        from subprocess import call, run, Popen, PIPE

        command1 = ["raster2pgsql", "-a", file_name, self.schema_name +  "." + identifier]
        p = Popen(command1,stdout=PIPE)
        command2 = ["psql", "-h", "localhost", "-p", "5432", "-d", self.dbname]
        run(command2,stdin=p.stdout)


        return identifier


    def store_other_output(self, file_name, identifier, uuid):

        import sqlalchemy
        from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey, LargeBinary, DateTime, func

        file = open(file_name)
        print(file, type(file))
        engine = sqlalchemy.create_engine('postgresql://pisl:password@localhost:5432/pisl')

        metadata = MetaData()

        #print(file_name, type(file_name))

        user = Table('test2', metadata,
            Column('user_id', Integer, primary_key=True),
            Column("uuid", String(64)),
            Column('data', LargeBinary),
            Column("timestamp", DateTime(timezone=True), server_default=func.now()))

        metadata.create_all(engine)


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

