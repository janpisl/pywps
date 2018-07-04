##################################################################
# Copyright 2018 Open Source Geospatial Foundation and others    #
# licensed under MIT, Please consult LICENSE.txt for details     #
##################################################################

import logging
from abc import ABCMeta, abstractmethod
from pywps import configuration as config
from pywps.inout.storage import db
from pywps.inout.formats import DATA_TYPE
from .. import STORE_TYPE
from .. import StorageAbstract
import sqlalchemy


LOGGER = logging.getLogger('PYWPS')


class DbStorage(StorageAbstract):

    def __init__(self):
        # get db_type from configuration 
        try:
            self.db_type = config.get_config_value('db', 'db_type').lower()
        except KeyError:
            raise exception("Database type has not been specified")

        self.initdb()


    @staticmethod
    def get_db_type():
        from . import sqlite
        from . import pg
        # create an instance of the appropriate class
        db_type = config.get_config_value('db', 'db_type').lower()
        if db_type == "pg":
            storage = pg.PgStorage()
        elif db_type == "sqlite":
            storage = sqlite.SQLiteStorage()
        else:
            raise Exception("Unknown database type: '{}'".format(self.db_type))

        return storage

 
    def initdb(self):

        from sqlalchemy.schema import CreateSchema

        dbsettings = "db"
        if self.db_type == "pg":
            connstr = 'postgresql://{}:{}@{}:{}/{}'.format(
                config.get_config_value(dbsettings, "user"),
                config.get_config_value(dbsettings, "password"),
                config.get_config_value(dbsettings, "host"),
                config.get_config_value(dbsettings, "port"),
                config.get_config_value(dbsettings, "dbname")
            )
        elif self.db_type == "sqlite":
            connstr = 'sqlite:///{}'.format(
                config.get_config_value(dbsettings, "dblocation"),

            )
        else:
            raise Exception("Unknown database type: '{}'".format(self.db_type))


        engine = sqlalchemy.create_engine(connstr)

        schema_name = config.get_config_value('db', 'schema_name')

        #Create schema; if it already exists, skip this
        try:
            engine.execute(CreateSchema(schema_name))
        # programming error - schema already exists; operational error - sqlite syntax error (schema)
        except (sqlalchemy.exc.ProgrammingError, sqlalchemy.exc.OperationalError):
            pass


    def store(self, output):
        """ Creates reference that is returned to the client
        """

        DATA_TYPE.is_valid_datatype(output.output_format.data_type)

        if output.output_format.data_type is DATA_TYPE.VECTOR:
            self.store_vector_output(output.file, output.identifier)
        elif output.output_format.data_type is DATA_TYPE.RASTER:
            self.store_raster_output(output.file, output.identifier)
        elif output.output_format.data_type is DATA_TYPE.OTHER:
            self.store_other_output(output.file, output.identifier, output.uuid)
        else:
            # This should never happen
            raise Exception("Unknown data type")

        if isinstance(self, sqlite.SQLiteStorage):
            url = '{}.{}'.format(self.dblocation, output.identifier)
            print(url)
        elif isinstance(self, pg.PgStorage):
            url = '{}.{}.{}'.format(self.dbname, self.schema_name, output.identifier)

        # returns value for database storage defined in the STORE_TYPE class,        
        # name of the output file and a reference
        return (STORE_TYPE.DB, output.file, url)


    def store_other_output(self, file_name, identifier, uuid):

        from pywps import configuration as config  
        import sqlalchemy
        from sqlalchemy import Column, Integer, String, LargeBinary, DateTime, func, create_engine
        from sqlalchemy.ext.declarative import declarative_base  
        from sqlalchemy.orm import sessionmaker

        base = declarative_base()

        if isinstance(self, sqlite.SQLiteStorage):
            engine = sqlalchemy.create_engine("sqlite:///{}".format(self.dblocation))
        elif isinstance(self, pg.PgStorage):
            engine = sqlalchemy.create_engine('postgresql://{}:{}@{}:{}/{}'.format(
            self.dbname,self.password,self.host,self.port,self.user
                )
            )   

        # Create table
        class Other_output(base):  
            __tablename__ = identifier
            if isinstance(self, pg.PgStorage):
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