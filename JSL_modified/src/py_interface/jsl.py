'''jsl.py osmid json'''

import sys
import time
import slicing
import ctypes
import geojson
from s2 import *
import s2sphere
import cassandra
from uuid import uuid1 as TIME_UUID

from cassandra.cluster import Cluster

import dbhelper
import geohelper

# determine if sufficient arguments are given
if len(sys.argv) != 4:
    sys.exit(1)

OSM_ID = sys.argv[1]
JSON_STR = sys.argv[2]
PROCEDURE = sys.argv[3]
JSON = geojson.loads(JSON_STR)


# new, modify, delete

PT_LIST = geohelper.get_pt_list(JSON)
BBOXES = geohelper.get_bboxes(PT_LIST)
    # insert empty feature into the database @ current timestamp
    
if PROCEDURE == 'new':
    dbhelper.insert_by_bboxes(BBOXES, "", False)
    for BBOX in BBOXES:
        COVERINGS = geohelper.get_covering(BBOX)
        for S2CELL in COVERINGS:
            dbhelper.insert_feature_raw(
                S2CELL.level(), S2CELL.id(), TIME_UUID(), OSM_ID, JSON_STR, False)
        n = geohelper.get_covering_level_from_bboxes(BBOXES)
        # for cutting
        top_left = BBOX[0]
        bottom_right = BBOX[1]
        ll_top_left = S2LatLng.FromDegrees(top_left[0], top_left[1])
        ll_bottom_right = S2LatLng.FromDegrees(
            bottom_right[0], bottom_right[1])
        llrect = S2LatLngRect.FromPointPair(ll_top_left, ll_bottom_right)

        for cutting_lv in range(n + 1, 13 + 1):
            coverer = S2RegionCoverer()
            coverer.set_max_level(cutting_lv)
            coverer.set_min_level(cutting_lv)
            covering = coverer.GetCovering(llrect)
            for S2CELL in covering:
                dbhelper.insert_feature_raw(
                    S2CELL.level(), S2CELL.id(), TIME_UUID(), OSM_ID, JSON_STR, False)
elif PROCEDURE == 'modify':
    


def load_by_duplication(feature):
    '''Using the duplication method to store features into the appoprate level.
    Function behavior is controlled by global variable RUN_DUPLICATION. Insertion
    only start iff RUN_DUPLICATION is True. Change the behavior in config.yml'''
    if RUN_DUPLICATION:
        pt_list = geohelper.get_pt_list(feature)
        bboxes = geohelper.get_bboxes(pt_list)
        dbhelper.insert_by_bboxes(bboxes, feature)


def propogate():
    # Connect to the Database

    # Prepare the Statements
    master_select_ps = session.prepare('''
        SELECT * FROM master
    ''')
    slave_select_ps = session.prepare('''
        SELECT json FROM slave
        WHERE level=? AND s2_id=? AND time=? AND osm_id=?
    ''')
    slave_insert_ps = session.prepare('''
        INSERT INTO slave(level, s2_id, time, osm_id, json)
        VALUES (?, ?, ?, ?, ?)
    ''')

    # Loop over Master Features
    for row in session.execute(master_select_ps):
        try:
            # Extract Row Identification
            json = geojson.loads(row.json)
            osm_id = row.osm_id
            level = 16  # Fix This!!!!!

            # Find Feature's Covering Region
            print
            print ">> ", osm_id
            jsons = []
            if json['geometry']['type'] == 'Point':
                jsons = slice.slicePoint(json, level)
            elif json['geometry']['type'] == 'MultiPoint':
                jsons = slice.sliceMultiPoint(json, level)
            elif json['geometry']['type'] == 'LineString':
                jsons = slice.sliceLineString(json, level)
            elif json['geometry']['type'] == 'Polygon':
                jsons = slice.slicePolygon(json, level)
            else:
                continue

            # Perform Version Update
            for cellID, cellJson in jsons.items():
                # Convert to 64-bit Signed Integer
                cellID = (cellID + 2**63) % 2**64 - 2**63
                cellJson = geojson.dumps(cellJson)
                results = session.execute(
                    slave_select_ps, (level, cellID, cassandra.util.HIGHEST_TIME_UUID, osm_id))

                if len(results.current_rows) > 1:
                    print osm_id, " Error: More than one row returned (", results.current_rows, ")"

                elif not results:
                    print osm_id, " is new - Updating..."
                    session.execute(slave_insert_ps, (level, cellID,
                                                      cassandra.util.HIGHEST_TIME_UUID, osm_id, cellJson))

                elif results[0].json != cellJson:
                    print osm_id, " is dirty - Updating..."
                    session.execute(slave_insert_ps, (level, cellID, cassandra.util.uuid_from_time(
                        int(time.time()), 0, 0), osm_id, results[0].json))
                    session.execute(slave_insert_ps, (level, cellID,
                                                      cassandra.util.HIGHEST_TIME_UUID, osm_id, cellJson))

        except NotImplementedError:
            print "Not implemented. Hold the line."
    # Cleanup
    cluster.shutdown()


if __name__ == '__main__':
    propogate()
