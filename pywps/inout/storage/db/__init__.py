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


    @staticmethod
    def get_db_type():
        # get db_type from configuration 
        try:
            db_type = config.get_config_value('db', 'db_type')
        except KeyError:
            raise exception("Database type has not been specified")

        # create an instance of the appropriate class
        if db_type == "PG":
            storage = pg.PgStorage()
        elif db_type == "SQLITE":
            storage = sqlite.SQLiteStorage()
        else:
            raise exception("Unknown database type: '{}'".format(db_type))

        return storage


