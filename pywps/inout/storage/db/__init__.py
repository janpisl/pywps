##################################################################
# Copyright 2018 Open Source Geospatial Foundation and others    #
# licensed under MIT, Please consult LICENSE.txt for details     #
##################################################################

#import sys
#sys.path.append("/mnt/c/Users/Jan/Documents/GitHub/pywps")

import logging
from abc import ABCMeta, abstractmethod
from pywps import configuration as config
from .. import StorageAbstract
from . import sqlite
from . import pg


LOGGER = logging.getLogger('PYWPS')


class DbStorage(StorageAbstract):

    def __init__(self):

       self.storage = self.get_db_type()


    def store(self, output):

        assert(self.storage is not None)
        self.storage.store(output)


    def get_db_type(self):
        # get db_type from configuration 
        try:
            db_type = config.get_config_value('db', 'db_type')
        except KeyError:
            raise exception("Database type has not been specified")

        # create an instance of the appropriate class
        if db_type.lower() == "pg":
            storage = pg.PgStorage()
        elif db_type.lower() == "sqlite":
            storage = sqlite.SQLiteStorage()
        else:
            raise exception("Unknown database type: '{}'".format(db_type))

        return storage


