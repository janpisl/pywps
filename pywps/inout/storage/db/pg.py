##################################################################
# Copyright 2018 Open Source Geospatial Foundation and others    #
# licensed under MIT, Please consult LICENSE.txt for details     #
##################################################################

import logging
from pywps import configuration as config
from pywps.exceptions import NoApplicableCode
from .. import DbStorageAbstract, STORE_TYPE
from pywps.inout.formats import DATA_TYPE
from . import DbStorage

LOGGER = logging.getLogger('PYWPS')

class PgStorage(DbStorage):

    def __init__(self):
        # TODO: more databases in config file
        # create connection string
        dbsettings = "db"
        self.dbname = config.get_config_value(dbsettings, "dbname")
        self.user = config.get_config_value(dbsettings, "user")
        self.password = config.get_config_value(dbsettings, "password")
        self.host = config.get_config_value(dbsettings, "host")
        self.port = config.get_config_value(dbsettings, "port")


        self.target = "dbname={} user={} password={} host={} port={}".format(
            self.dbname, self.user, self.password, self.host, self.port
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
            raise NoApplicableCode("Database connection has not been established.")
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

        # Convert raster to an SQL query
        command1 = ["raster2pgsql", "-a", file_name, self.schema_name +  "." + identifier]
        p = Popen(command1,stdout=PIPE)
        # Apply the SQL query
        command2 = ["psql", "-h", "localhost", "-p", "5432", "-d", self.dbname]
        run(command2,stdin=p.stdout)


        return identifier


    def store_other_output(self, file_name, identifier, uuid):

        from pywps import configuration as config  
        import sqlalchemy
        from sqlalchemy import Column, Integer, String, LargeBinary, DateTime, func, create_engine
        from sqlalchemy.ext.declarative import declarative_base  
        from sqlalchemy.orm import sessionmaker

        base = declarative_base()

        engine = sqlalchemy.create_engine('postgresql://{}:{}@{}:{}/{}'.format(
            self.dbname,self.password,self.host,self.port,self.user
            )
        )

        # Create table
        class Other_output(base):  
            __tablename__ = identifier
            __table_args__ = {'schema' : self.schema_name}

            primary_key = Column(Integer, primary_key=True)
            uuid = Column(String(64))
            data = Column(LargeBinary)
            timestamp = Column(DateTime(timezone=True), server_default=func.now())

        Session = sessionmaker(engine)  
        session = Session()

        base.metadata.create_all(engine)

        # Open file as binary
        with open(file_name, "rb") as data:
            out = data.read()

            # Add data to table
            output = Other_output(uuid=uuid, data=out)  
            session.add(output)  
            session.commit()


        return identifier


