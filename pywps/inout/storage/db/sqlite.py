##################################################################
# Copyright 2018 Open Source Geospatial Foundation and others    #
# licensed under MIT, Please consult LICENSE.txt for details     #
##################################################################

import logging
from pywps import configuration as config
from .. import STORE_TYPE
from pywps.inout.formats import DATA_TYPE
from pywps.exceptions import NoApplicableCode
from . import DbStorage


LOGGER = logging.getLogger('PYWPS')


class SQLiteStorage(DbStorage):

    def __init__(self):

        self.dblocation = config.get_config_value("db", "dblocation")


    def store_vector_output(self, file_name, identifier):
        """ Open output file, connect to SQLite database and copiy data there
        """ 
        from osgeo import ogr

        drv = ogr.GetDriverByName("SQLite")
        dsc_out = drv.CreateDataSource(self.dblocation)


        # connect to a database and copy output there
        LOGGER.debug("Path to the database file: {}".format(self.dblocation))
        dsc_in = ogr.Open(file_name)
        if dsc_in is None:
            raise Exception("Reading data failed.")
        if dsc_out is None:
            raise NoApplicableCode("Database connection has not been established.")
        layer = dsc_out.CopyLayer(dsc_in.GetLayer(), identifier,
                                  ['OVERWRITE=YES'])

        if layer is None:
            raise Exception("Writing output data to the database failed.")
        
        dsc_out.Destroy()
        dsc_in.Destroy()

        # returns process identifier (defined within the process)
        return identifier


    def store_raster_output(self, file_name, identifier):

        import subprocess
        from subprocess import call

        call(["gdal_translate", "-of", "Rasterlite", file_name, "RASTERLITE:" + self.dblocation + ",table=" + identifier])

        # returns process identifier (defined within the process)
        return identifier
