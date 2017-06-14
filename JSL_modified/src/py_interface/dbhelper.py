'''Dbhelper module, this module contains the abstraction of storing data into database'''

import ctypes
import atexit
import geojson
import geohelper
from uuid import uuid1 as TIME_UUID
from s2 import *
from cassandra.cluster import Cluster
from cassandra.util import HIGHEST_TIME_UUID
import cfgparser

CFG = cfgparser.load_module('dbhelper')


TRUNCATE_TABLE_WHEN_START = CFG['truncate table when start']
CLUSTER_LIST = CFG['list of node']
KEYSPACE = CFG['key space']
CLUSTER = Cluster(CLUSTER_LIST, port=9041)
SESSION = CLUSTER.connect(KEYSPACE)
PS_INSERT = '''INSERT INTO slave (level, s2_id, time, osm_id, json, is_cut) VALUES (?, ?, ?, ?, ?, ?)'''
PS_QUERY_MASTER = '''SELECT json from master where osm_id = ?'''
PS_NEW_MASTER = '''INSERT INTO master (osm_id, json) values (?,?)'''
PS_MODIFY_MASTER = '''UPDATE master set json = ? where osm_id = ?'''
PS_DELETE_MASTER = '''DELETE FROM master where osm_id = ?'''
PS_DELETE_SLAVE = '''DELETE FROM SLAVE where level = ? and s2_id = ? and time = ? and osm_id = ?  '''

PREPARED_INSERT = SESSION.prepare(PS_INSERT)

def execute(statement, arguments):
    PREPARED_EXECUTE = SESSION.prepare(statement)
    SESSION.execute(PREPARED_EXECUTE, arguments)

def to_64bit(number): 
    '''wrap-up for c type long'''
    return ctypes.c_long(number).value

def get_feature_from_master(osm_id):
    PREPARED_QUERY_MASTER = SESSION.prepare(PS_QUERY_MASTER)
    result = SESSION.execute(PREPARED_QUERY_MASTER, (osm_id,))
    for row in result:
        return row.json

def insert_feature_raw(cell_level, cell_s2_id, timestamp, osm_id, feature_str, is_cut):
    insert_by_covering.handle = SESSION.execute_async(
            PREPARED_INSERT, (cell_level, to_64bit(cell_s2_id), timestamp, osm_id, feature_str, is_cut))


def insert_by_covering(cellid, feature, is_cut, highest_timestamp):
    '''Given the covering region, store the given feature into the database'''
    osm_id = feature['id']
    s2_id = to_64bit(cellid.id())
    feature_str = geojson.dumps(feature)
    if highest_timestamp:
        insert_by_covering.handle = SESSION.execute_async(
            PREPARED_INSERT, (cellid.level(), s2_id, HIGHEST_TIME_UUID, osm_id, feature_str, is_cut))
    else:
        insert_by_covering.handle = SESSION.execute_async(
            PREPARED_INSERT, (cellid.level(), s2_id, TIME_UUID(), osm_id, feature_str, is_cut))



def insert_by_bboxes(bboxes, feature, highest_timestamp=True):
    '''Given the bboxes of the feature and the feature itself, store the given feature into the
    database'''
    for bbox in bboxes:
        coverings = geohelper.get_covering(bbox)
        for covering in coverings:
            insert_by_covering(covering, feature, False, highest_timestamp)


def insert_by_cut_feature(cut_feature_set):
    '''Given the cut feature dictionary, insert pieces to the corresponding place'''
    for cellid, feature in cut_feature_set.iteritems():
        insert_by_covering(S2CellId(cellid), feature, True, True)


def __initialize():
    '''DO NOT CALL THIS FUNCTION. Initializing the module. '''
    if TRUNCATE_TABLE_WHEN_START:
        SESSION.execute('TRUNCATE slave')


def __before_exit():
    '''Wait to ensure that all insertion has been into the table'''
    if insert_by_covering.handle is not None:
        print 'Waiting for database to finish up...'
        insert_by_covering.handle.result()
    CLUSTER.shutdown()



###########################################
##############  INITIALIZE ################
insert_by_covering.handle = None
atexit.register(__before_exit)
__initialize()
