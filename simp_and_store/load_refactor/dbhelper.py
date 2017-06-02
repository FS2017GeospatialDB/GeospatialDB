'''Dbhelper module, this module contains the abstraction of storing data into database'''

from uuid import uuid1 as timeuuid
import ctypes
import atexit
import geojson
import geohelper
from cassandra.cluster import Cluster
from cassandra.util import HIGHEST_TIME_UUID
import cfgparser


TRUNCATE_TABLE_WHEN_START = True

# TODO: the following parameters need to be added to config.yml
# CLUSTER_LIST = ['192.168.1.10']
CLUSTER_LIST = None

CLUSTER = Cluster(CLUSTER_LIST)
SESSION = CLUSTER.connect('global')

PS_INSERT = '''INSERT INTO slave (level, s2_id, time, osm_id, json) VALUES (?, ?, ?, ?, ?)'''
PREPARED_INSERT = SESSION.prepare(PS_INSERT)


def to_64bit(number):
    '''wrap-up for c type long'''
    return ctypes.c_long(number).value


def insert_by_covering(cellid, feature):
    '''Given the covering region, store the given feature into the database'''
    osm_id = feature['id']
    s2_id = to_64bit(cellid.id())
    feature_str = geojson.dumps(feature)
    insert_by_covering.handle = SESSION.execute_async(
        PREPARED_INSERT, (cellid.level(), s2_id, HIGHEST_TIME_UUID, osm_id, feature_str))


def insert_by_bboxes(bboxes, feature):
    '''Given the bboxes of the feature and the feature itself, store the given feature into the
    database'''
    for bbox in bboxes:
        coverings = geohelper.get_covering(bbox)
        for covering in coverings:
            insert_by_covering(covering, feature)


def __initialize():
    '''DO NOT CALL THIS FUNCTION. Initializing the module. '''
    if TRUNCATE_TABLE_WHEN_START:
        SESSION.execute('TRUNCATE slave')


def __before_exit():
    '''Wait to ensure that all insertion has been into the table'''
    if insert_by_covering.handle is not None:
        print 'Waiting for database to finish up...'
        insert_by_covering.handle.result()


def __load_config():
    cfg = cfgparser.load_module('dbhelper')
    global TRUNCATE_TABLE_WHEN_START
    TRUNCATE_TABLE_WHEN_START = cfg['truncate table when start']



###########################################
##############  INITIALIZE ################
insert_by_covering.handle = None
atexit.register(__before_exit)
__initialize()
