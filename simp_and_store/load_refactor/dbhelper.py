'''Dbhelper module, this module contains the abstraction of storing data into database'''

from uuid import uuid1 as timeuuid
import geojson
import geohelper
import atexit
from cassandra.cluster import Cluster

# CLUSTER_LIST = ['192.168.1.10']
CLUSTER_LIST = None

CLUSTER = Cluster(CLUSTER_LIST)
SESSION = CLUSTER.connect('global')

PS_INSERT = '''INSERT INTO slave (level, s2_id, time, osm_id, json) VALUES (?, ?, ?, ?, ?)'''
PREPARED_INSERT = SESSION.prepare(PS_INSERT)


def insert_by_covering(cellid, feature):
    '''Given the covering region, store the given feature into the database'''
    osm_id = feature['id']
    feature_str = geojson.dumps(feature)
    insert_by_covering.handle = SESSION.execute_async(
        PREPARED_INSERT, (cellid.level(), cellid.id(), timeuuid(), osm_id, feature_str))

def insert_by_bboxes(bboxes, feature):
    '''Given the bboxes of the feature and the feature itself, store the given feature into the
    database'''
    for bbox in bboxes:
        coverings = geohelper.get_covering(bbox)
        for covering in coverings:
            insert_by_covering(covering, feature)


def initialize():
    '''DO NOT CALL THIS FUNCTION. Initializing the module. '''
    SESSION.execute('TRUNCATE slave')


def before_exit():
    '''Wait to ensure that all insertion has been into the table'''
    print 'Waiting for database to finish up...'
    insert_by_covering.handle.result()


###########################################
##############  INITIALIZE ################
insert_by_covering.handle = None
initialize()
atexit.register(before_exit)
