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
from cassandra.util import HIGHEST_TIME_UUID
from cassandra.cluster import Cluster

import dbhelper 
import geohelper
import main

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
    for BBOX in BBOXES:
        COVERINGS = geohelper.get_covering(BBOX)
        for S2CELL in COVERINGS:
            dbhelper.insert_feature_raw(
                S2CELL.level(), S2CELL.id(), TIME_UUID(), OSM_ID, "", False)
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
                    S2CELL.level(), S2CELL.id(), TIME_UUID(), OSM_ID, "", False)
    main.load_by_duplication(JSON)
    main.load_by_cutting(JSON)
    dbhelper.execute(dbhelper.PS_NEW_MASTER, (OSM_ID, JSON_STR))
elif PROCEDURE == 'modify':
    OLD_JSON_STR = dbhelper.get_feature_from_master(OSM_ID)
    #OLD_JSON = geojson.dumps(OLD_JSON_STR)
    for BBOX in BBOXES:
        COVERINGS = geohelper.get_covering(BBOX)
        for S2CELL in COVERINGS:
            dbhelper.insert_feature_raw(
                S2CELL.level(), S2CELL.id(), TIME_UUID(), OSM_ID, OLD_JSON_STR, False)
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
                    S2CELL.level(), S2CELL.id(), TIME_UUID(), OSM_ID, OLD_JSON_STR, False)
    main.load_by_duplication(JSON)
    main.load_by_cutting(JSON)
    dbhelper.execute(dbhelper.PS_MODIFY_MASTER, (JSON_STR, OSM_ID))
elif PROCEDURE == 'delete':
    OLD_JSON_STR = dbhelper.get_feature_from_master(OSM_ID)
    #OLD_JSON = geojson.dumps(OLD_JSON_STR)
    for BBOX in BBOXES:
        COVERINGS = geohelper.get_covering(BBOX)
        for S2CELL in COVERINGS:
            dbhelper.execute(dbhelper.PS_DELETE_SLAVE, (S2CELL.level(), S2CELL.id(), HIGHEST_TIME_UUID, OSM_ID))
            dbhelper.insert_feature_raw(
                S2CELL.level(), S2CELL.id(), TIME_UUID(), OSM_ID, OLD_JSON_STR, False)
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
                dbhelper.execute(dbhelper.PS_DELETE_SLAVE, (S2CELL.level(), S2CELL.id(), HIGHEST_TIME_UUID, OSM_ID))
                dbhelper.insert_feature_raw(
                    S2CELL.level(), S2CELL.id(), TIME_UUID(), OSM_ID, OLD_JSON_STR, False)
    dbhelper.execute(dbhelper.PS_DELETE_MASTER, (OSM_ID, ))
else:
    sys.exit(1)
