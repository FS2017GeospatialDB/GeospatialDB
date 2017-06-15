'''Dbhelper module, this module contains the abstraction of storing data into database'''

import ctypes
import uuid
import atexit
import geojson
import geohelper
from s2 import *
from cassandra.cluster import Cluster
from cassandra.util import HIGHEST_TIME_UUID
import cfgparser

################# CONFIGURATIONS ############
CFG = cfgparser.load_module('dbhelper')
TRUNCATE_TABLE_WHEN_START = CFG['truncate table when start']
CLUSTER_LIST = CFG['list of node']
KEYSPACE = CFG['key space']
CLUSTER = Cluster(CLUSTER_LIST)
SESSION = CLUSTER.connect(KEYSPACE)

PS_INSERT = '''INSERT INTO slave (level, s2_id, time, osm_id, json, is_cut) VALUES (?,?,?,?,?,?)'''
MASTER_INSERT = '''INSERT INTO master (osm_id, json) VALUES (?, ?)'''
PREPARED_INSERT = SESSION.prepare(PS_INSERT)
PREPARED_MASTER_INSERT = SESSION.prepare(MASTER_INSERT)
#############################################


def to_64bit(number):
    '''wrap-up for c type long'''
    return ctypes.c_long(number).value


def insert_by_covering(cellid, feature, is_cut):
    '''Given the covering region, store the given feature into the database'''
    osm_id = __get_osm_id(feature)

    s2_id = to_64bit(cellid.id())
    feature_str = geojson.dumps(feature)

    # insert a null in current timestamp as a placeholder. For hist query
    insert_by_covering.handle0 = SESSION.execute_async(
        PREPARED_INSERT, (cellid.level(), s2_id, uuid.uuid1(), osm_id, None, is_cut))
    insert_by_covering.handle1 = SESSION.execute_async(
        PREPARED_INSERT, (cellid.level(), s2_id, HIGHEST_TIME_UUID, osm_id, feature_str, is_cut))


def insert_by_bboxes(bboxes, feature):
    '''Given the bboxes of the feature and the feature itself, store the given feature into the
    database'''
    for bbox in bboxes:
        coverings = geohelper.get_covering(bbox)
        for covering in coverings:
            insert_by_covering(covering, feature, False)


def insert_by_cut_feature(cut_feature_set):
    '''Given the cut feature dictionary, insert pieces to the corresponding place'''
    for cellid, feature in cut_feature_set.iteritems():
        insert_by_covering(S2CellId(cellid), feature, True)


def insert_master(feature):
    '''Insert the feature into the master'''
    osm_id = __get_osm_id(feature)
    insert_master.handle = SESSION.execute_async(
        PREPARED_MASTER_INSERT, (osm_id, geojson.dumps(feature)))


def __get_osm_id(feature):
    oid = feature['properties']['osm_id']
    if oid is None:
        oid = feature['properties']['osm_way_id']
    typee = feature['geometry']['type']
    osm_id = typee + "/" + oid
    return osm_id


def __initialize():
    '''DO NOT CALL THIS FUNCTION. Initializing the module. '''
    if TRUNCATE_TABLE_WHEN_START:
        SESSION.execute('TRUNCATE slave')
        SESSION.execute('TRUNCATE master')


def __before_exit():
    #TODO: solve timeout issue
    '''Wait to ensure that all insertion has been into the table'''
    print 'Waiting for database to finish up...'
    if insert_by_covering.handle0 is not None:
        insert_by_covering.handle0.result()
    if insert_by_covering.handle1 is not None:
        insert_by_covering.handle1.result()
    if insert_master.handle is not None:
        insert_master.handle.result()
    CLUSTER.shutdown()


###########################################
##############  INITIALIZE ################
insert_by_covering.handle0 = None
insert_by_covering.handle1 = None
insert_master.handle = None
atexit.register(__before_exit)
__initialize()
