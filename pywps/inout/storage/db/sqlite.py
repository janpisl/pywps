##################################################################
# Copyright 2018 Open Source Geospatial Foundation and others    #
# licensed under MIT, Please consult LICENSE.txt for details     #
##################################################################

import logging
from pywps import configuration as config
from .. import DbStorageAbstract, STORE_TYPE



LOGGER = logging.getLogger('PYWPS')


class SQLiteStorage(DbStorageAbstract):

    def __init__(self):

        import sqlite3

        dbsettings = "db"
        self.dblocation = config.get_config_value(dbsettings, "dblocation")


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
            raise Exception("Database file could not be opened.")
        layer = dsc_out.CopyLayer(dsc_in.GetLayer(), identifier,
                                  ['OVERWRITE=YES'])

        if layer is None:
            raise Exception("Writing output data to the database failed.")
        
        dsc_out.Destroy()
        dsc_in.Destroy()

        # returns process identifier (defined within the process)
        return identifier


    def store_raster_output(self, file_name, identifier):

        from osgeo import gdal

        dsc_in = gdal.Open(file_name)
        gt =dsc_in.GetGeoTransform()
        pixelSizeX = int(gt[1])
        pixelSizeY =int(-gt[5])
        drv = gdal.GetDriverByName("SQLite")
        dsc_out = drv.Create(self.dblocation, xsize=pixelSizeY, ysize=pixelSizeY) 

        # connect to a database and copy output there
        LOGGER.debug("Path to the database file: {}".format(self.dblocation))
        if dsc_in is None:
            raise Exception("Reading data failed.")
        if dsc_out is None:
            raise Exception("Database file could not be opened.")

        layer = dsc_out.CopyLayer(dsc_in.GetLayer(), identifier,
                                  ['OVERWRITE=YES'])

        if layer is None:
            raise Exception("Writing output data to the database failed.")
        
        dsc_out.Destroy()
        dsc_in.Destroy()

        # returns process identifier (defined within the process)
        return identifier

    def store(self, output):
        """ Creates reference that is returned to the client (database name, schema name, table name)
        """
        assert(output.output_format.data_type in (0,1))

        if output.output_format.data_type == 0:
            self.store_vector_output(output.file, output.identifier)
        else:
            self.store_raster_output(output.file, output.identifier)
        url = '{}.{}'.format(self.dblocation, output.identifier)
        # returns value for database storage defined in the STORE_TYPE class,        
        # name of the output file and a reference
        return (STORE_TYPE.DB, output.file, url)