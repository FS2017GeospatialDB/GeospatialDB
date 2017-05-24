import geojson
import re
import uuid
import ctypes
import s2sphere
from math import floor
from cassandra.cluster import Cluster

# prepare data
fstring = open('map.json', 'r').read()
data = geojson.loads(fstring)

# regex to match node (simple struct)
# as the entire project is not finished, we now can
# only deal with "node"
patt = 'node'
regx = re.compile(patt)

ps_features = """
INSERT INTO features (level, s2id, time, id, geojson)
VALUES (%s,%s,%s,%s,%s)
"""

# Connect to the Database
cluster = Cluster()
session = cluster.connect('global')

# Clear Existing Data
print '#####################ERASE THE DATABASE NOW######################'
session.execute('TRUNCATE FEATURES')

def is_node(coord):
    """Take the coord attr of the feature and determine if that is a node"""
    return not isinstance(coord[0], list)

# Iterate through Features
for feature in data['features']:
    uid = uuid.uuid4()
    tid = uuid.uuid1()
    jsonFeature = geojson.dumps(feature)

    coord = feature['geometry']['coordinates']
    while isinstance(coord[0], list):
        coord = coord[0]
    latlng = s2sphere.LatLng.from_degrees(coord[1], coord[0])
    print 'latitude: ', coord[1], '\tlongitude: ', coord[0]

    # Best Solution?
    cell = s2sphere.CellId.from_lat_lng(latlng)
    cell_lv16 = cell.parent(16)
    cell_lv12 = cell.parent(12)
    cell_lv8 = cell.parent(8)
    cell_lv4 = cell.parent(4)

    # Convert Magic Python Type to Signed 64-bit Int
    cellID = ctypes.c_long(cell.id()).value
    cellID_16 = ctypes.c_long(cell_lv16.id()).value
    cellID_12 = ctypes.c_long(cell_lv12.id()).value
    cellID_8 = ctypes.c_long(cell_lv8.id()).value
    cellID_4 = ctypes.c_long(cell_lv4.id()).value

    session.execute(ps_features, (16, cell_lv16, tid, uid, jsonFeature))

#    else:
#       a = 0
#       print 'found a complex structure!'
#       print feature['geometry']['coordinates']

cluster.shutdown()
