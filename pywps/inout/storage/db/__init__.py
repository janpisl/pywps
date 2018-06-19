##################################################################
# Copyright 2018 Open Source Geospatial Foundation and others    #
# licensed under MIT, Please consult LICENSE.txt for details     #
##################################################################


import logging
from abc import ABCMeta, abstractmethod
from pywps import configuration as config
from .. import StorageAbstract
from . import sqlite
from . import pg
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


    def get_db_type(self):

        # create an instance of the appropriate class
        if self.db_type == "pg":
            storage = pg.PgStorage()
        elif self.db_type == "sqlite":
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


