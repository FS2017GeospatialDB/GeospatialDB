'''Dbhelper module, this module contains the abstraction of storing data into database'''

import time
import ctypes
import atexit
import geojson
import geohelper
import cassandra

from s2 import *
from cassandra.cluster import Cluster
from cassandra.util import HIGHEST_TIME_UUID
import cfgparser

'''Only purpose is to track progress of loading'''
# from track import count

CFG = cfgparser.load_module('dbhelper')


TRUNCATE_TABLE_WHEN_START = CFG['truncate table when start']
CLUSTER_LIST = CFG['list of node']
KEYSPACE = CFG['key space']
CLUSTER = Cluster(["127.0.0.1"])
SESSION = CLUSTER.connect(KEYSPACE)
PS_INSERT = '''INSERT INTO slave (level, s2_id, time, osm_id, json, is_cut) VALUES (?, ?, ?, ?, ?, ?)'''
PREPARED_INSERT = SESSION.prepare(PS_INSERT)
MASTER_INSERT = '''INSERT INTO master (osm_id, json) VALUES (?, ?)'''
PREPARED_MASTER_INSERT = SESSION.prepare(MASTER_INSERT)

'''def connect_to_cluster()
    global CLUSTER
    global SESSION
    global PREPARED_INSERT
    global PREPARED_MASTER_INSERT
    for node in CLUSTER_LIST:
        try:
            CLUSTER = Cluster([node])
            SESSION = CLUSTER.connect(KEYSPACE)
	    PREPARED_INSERT = SESSION.prepare(PS_INSERT)
	    PREPARED_MASTER_INSERT = SESSION.prepare(MASTER_INSERT)
        except:
            pass'''


def to_64bit(number):
    '''wrap-up for c type long'''
    return ctypes.c_long(number).value


def insert_by_covering(cellid, feature, is_cut):
    '''Given the covering region, store the given feature into the database'''
    oid = feature['properties']['osm_id']
    if oid is None: oid = feature['properties']['osm_way_id']
    typee = feature['geometry']['type']
    osm_id = typee + "/" + oid

    s2_id = to_64bit(cellid.id())
    feature_str = geojson.dumps(feature)

    insert_by_covering.handle0 = SESSION.execute_async(
        PREPARED_INSERT, (cellid.level(), s2_id, cassandra.util.uuid_from_time(int(time.time()), 0, 0), osm_id, None, is_cut))
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
    oid = feature['properties']['osm_id']
    if oid is None: oid = feature['properties']['osm_way_id']
    typee = feature['geometry']['type']
    osm_id = typee + "/" + oid
    insert_master.handle = SESSION.execute_async(PREPARED_MASTER_INSERT, (osm_id, geojson.dumps(feature)))

def __initialize():
    '''DO NOT CALL THIS FUNCTION. Initializing the module. '''
    if TRUNCATE_TABLE_WHEN_START:
        SESSION.execute('TRUNCATE slave')


def __before_exit():
    '''Wait to ensure that all insertion has been into the table'''
    if insert_by_covering.handle0 is not None or insert_by_covering.handle1 is not None:
        print 'Waiting for database to finish up...'
        if insert_by_convering.handle0 is not None:
            insert_by_covering.handle0.result()
        elif insert_by_covering.handle1 is not None:
            insert_by_covering.handle1.result()
	elif insert_master.handle is not None:
	    insert_master.handle.result()
    CLUSTER.shutdown()



###########################################
##############  INITIALIZE ################
insert_by_covering.handle0 = None
insert_by_covering.handle1 = None
insert_master.handle = None
atexit.register(__before_exit)
__initialize()