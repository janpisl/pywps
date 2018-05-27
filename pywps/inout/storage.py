##################################################################
# Copyright 2018 Open Source Geospatial Foundation and others    #
# licensed under MIT, Please consult LICENSE.txt for details     #
##################################################################

import logging
import os
from abc import ABCMeta, abstractmethod
from pywps._compat import urljoin
from pywps.exceptions import NotEnoughStorage
from pywps import configuration as config

LOGGER = logging.getLogger('PYWPS')


class STORE_TYPE:
    PATH = 0
# TODO: cover with tests


class StorageAbstract(object):
    """Data storage abstract class
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def store(self, output):
        """
        :param output: of type IOHandler
        :returns: (type, store, url) where
            type - is type of STORE_TYPE - number
            store - string describing storage - file name, database connection
            url - url, where the data can be downloaded
        """
        pass


class DummyStorage(StorageAbstract):
    """Dummy empty storage implementation, does nothing

    Default instance, for non-reference output request

    >>> store = DummyStorage()
    >>> assert store.store
    """

    def __init__(self):
        """
        """

    def store(self, ouput):
        pass


class FileStorage(StorageAbstract):
    """File storage implementation, stores data to file system

    >>> import ConfigParser
    >>> config = ConfigParser.RawConfigParser()
    >>> config.add_section('FileStorage')
    >>> config.set('FileStorage', 'target', './')
    >>> config.add_section('server')
    >>> config.set('server', 'outputurl', 'http://foo/bar/filestorage')
    >>>
    >>> store = FileStorage()
    >>>
    >>> class FakeOutput(object):
    ...     def __init__(self):
    ...         self.file = self._get_file()
    ...     def _get_file(self):
    ...         tiff_file = open('file.tiff', 'w')
    ...         tiff_file.close()
    ...         return 'file.tiff'
    >>> fake_out = FakeOutput()
    >>> (type, path, url) = store.store(fake_out)
    >>> type == STORE_TYPE.PATH
    True
    """

    def __init__(self):
        """
        """
        self.target = config.get_config_value('server', 'outputpath')
        self.output_url = config.get_config_value('server', 'outputurl')

    def store(self, output):
        import math
        import shutil
        import tempfile
        import uuid

        file_name = output.file
        request_uuid = output.uuid or uuid.uuid1()

        file_block_size = os.stat(file_name).st_blksize
        # get_free_space delivers the numer of free blocks, not the available size!
        avail_size = get_free_space(self.target) * file_block_size
        file_size = os.stat(file_name).st_size

        # calculate space used according to block size
        actual_file_size = math.ceil(file_size / float(file_block_size)) * file_block_size

        if avail_size < actual_file_size:
            raise NotEnoughStorage('Not enough space in {} to store {}'.format(self.target, file_name))

        # create a target folder for each request
        target = os.path.join(self.target, str(request_uuid))
        if not os.path.exists(target):
            os.makedirs(target)

        # build output name
        (prefix, suffix) = os.path.splitext(file_name)
        if not suffix:
            suffix = output.data_format.extension
        (file_dir, file_name) = os.path.split(prefix)
        output_name = file_name + suffix
        # build tempfile in case of duplicates
        if os.path.exists(os.path.join(target, output_name)):
            output_name = tempfile.mkstemp(suffix=suffix, prefix=file_name + '_',
                                           dir=target)[1]

        full_output_name = os.path.join(target, output_name)
        LOGGER.info('Storing file output to %s', full_output_name)
        shutil.copy2(output.file, full_output_name)

        just_file_name = os.path.basename(output_name)

        # make sure base url ends with '/'
        baseurl = self.output_url.rstrip('/') + '/'
        baseurl += str(request_uuid) + '/'
        url = urljoin(baseurl, just_file_name)
        LOGGER.info('File output URI: %s', url)

        return (STORE_TYPE.PATH, output_name, url)


class DbStorage(StorageAbstract)

    def __init__(self):

        self.storage = get_db_type()


    def store(self):
        assert(self.storage is not None)
        self.storage.store()


    def get_db_type(self):
        # get db_type from configuration 
        try:
            db_type = config.get_config_value('db', 'db_type')
        except KeyError:
            raise exception("Database type has not been specified")

        # create an instance of the appropriate class
        if db_type == "PG":
            storage = self._PgStorage()
        elif db_type == "MSServer":
            storage = self._MSServerStorage()
        elif db_type == "Oracle":
            storage = self._OracleStorage()
        else:
            raise exception("Unknown database type: '{}'".format(db_type))

        return storage


    class _PgStorage()

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
            import psycopg2
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


        def _store_output(self, file_name, identifier):
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


        def store(self, output):
            """ Creates reference that is returned to the client (database name, schema name, table name)
            """
            self._store_output(output.file, output.identifier)
            url = '{}.{}.{}'.format(self.dbname, self.schema_name, output.identifier)
            # returns value for database storage defined in the STORE_TYPE class,        
            # name of the output file and a reference
            return (STORE_TYPE.DB, output.file, url)
            

    class _MSServerStorage()

        def store():
            ''' 
            to be implemented
            '''         
            pass


    class _OracleStorage()

        def store():
            ''' 
            to be implemented
            '''
            pass


def get_free_space(folder):
    """ Return folder/drive free space (in bytes)
    """
    import platform

    if platform.system() == 'Windows':
        import ctypes

        free_bytes = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(ctypes.c_wchar_p(folder), None, None, ctypes.pointer(free_bytes))
        free_space = free_bytes.value
    else:
        free_space = os.statvfs(folder).f_bfree

    LOGGER.debug('Free space: %s', free_space)
    return free_space
