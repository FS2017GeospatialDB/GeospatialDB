import sys
import ctypes
import geojson
import s2sphere
import uuid
import time
from simplify import simplify
from sets import Set
from cassandra.cluster import Cluster


def load():
    data = None
    with open(sys.argv[1], 'r') as in_file:
        data = geojson.loads(in_file.read())

    cluster = Cluster()
    session = cluster.connect('global')
    level = 16

    ins_statement = session.prepare('''INSERT INTO slave (level, s2_id, time, osm_id, json)
					VALUES (?, ?, ?, ?, ?)''')

    s = Set()
    for feature in data['features']:
        osm_id = feature['id']
        json = geojson.dumps(feature)

        featuretype = feature['geometry']['type']
        featurecoord = feature['geometry']['coordinates']

        # A list of every lon,lat pairs a feature has
        ptlist = []

        if (featuretype == "Point"):
            ptlist = case1(featurecoord)
        if (featuretype == "LineString" or featuretype == "MultiPoint"):
            ptlist = case2(featurecoord)
        if (featuretype == "Polygon" or featuretype == "MultiLineString"):
            ptlist = case3(featurecoord)
        if (featuretype == "MultiPolygon"):
            ptlist = case4(featurecoord)

        # Set to track distinct S2 values
        s2set = Set()
		# 16242 without simp
        sptlist = simplify(ptlist, 0.01, True)
        for pt in ptlist:
            latlng = s2sphere.LatLng.from_degrees(pt[1], pt[0])
            cell = s2sphere.CellId.from_lat_lng(latlng).parent(level)
            s2set.add(ctypes.c_long(cell.id()).value)

        for s2 in s2set:
            session.execute(ins_statement, (level, s2,
                                            uuid.uuid1(), osm_id, json))
	
    cluster.shutdown()


def case1(featurecoord):
    ptlist = []
    ptlist.append(featurecoord)
    return ptlist


def case2(featurecoord):
    ptlist = []
    for coords in featurecoord:
        ptlist.append(coords)
    return ptlist


def case3(featurecoord):
    ptlist = []
    for coords in featurecoord:
        for cs in coords:
            ptlist.append(cs)
    return ptlist


def case4(featurecoord):
    ptlist = []
    for coords in featurecoord:
        for cs in coords:
            for c in cs:
                ptlist.append(c)
    return ptlist


t = time.time()
load()
print(time.time() - t)
