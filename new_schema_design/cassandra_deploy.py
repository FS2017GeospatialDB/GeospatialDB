from cassandra.cluster import Cluster
import geojson
import re
import uuid
import ctypes
import s2sphere
from math import floor

# prepare data
fstring = open('map.json', 'r').read()
data = geojson.loads(fstring)

# regex to match node (simple struct)
# as the entire project is not finished, we now can
# only deal with "node"
patt = 'node'
regx = re.compile(patt)

ps_node_lv19 = """
INSERT INTO NODE_PLEVEL19 (id, part_lv19, time, feature)
VALUES (%s,%s,%s,%s)
"""
ps_node_lv15 = """
INSERT INTO NODE_PLEVEL15 (id, part_lv15, part_lv16, time, feature)
VALUES (%s,%s,%s,%s,%s)
"""
ps_node_lv11 = """
INSERT INTO NODE_PLEVEL11 (id, part_lv11, part_lv13, time, feature)
VALUES (%s,%s,%s,%s,%s)
"""

# Connect to the Database
cluster = Cluster()
session = cluster.connect('global')

# Clear Existing Data
print '#####################ERASE THE DATABASE NOW######################'
session.execute('TRUNCATE NODE_PLEVEL19')
session.execute('TRUNCATE NODE_PLEVEL15')
session.execute('TRUNCATE NODE_PLEVEL11')

# Iterate through Features
for feature in data['features']:
    tuid = uuid.uuid1()
    jsonFeature = geojson.dumps(feature)
    if regx.match(feature['id']):  # then it is a simple feature (node)
        coord = feature['geometry']['coordinates']
        latlng = s2sphere.LatLng.from_degrees(coord[1], coord[0])
        print 'latitude: ', coord[1], '\tlongitude: ', coord[0]

        # Best Solution?
        cell = s2sphere.CellId.from_lat_lng(latlng)
        cell_lv19 = s2sphere.CellId.from_lat_lng(latlng).parent(19)
        cell_lv16 = s2sphere.CellId.from_lat_lng(latlng).parent(16)
        cell_lv15 = s2sphere.CellId.from_lat_lng(latlng).parent(15)
        cell_lv13 = s2sphere.CellId.from_lat_lng(latlng).parent(13)
        cell_lv11 = s2sphere.CellId.from_lat_lng(latlng).parent(11)

        # Convert Magic Python Type to Signed 64-bit Int
        cellID = ctypes.c_long(cell.id()).value
        cellID_11 = ctypes.c_long(cell_lv11.id()).value
        cellID_13 = ctypes.c_long(cell_lv13.id()).value
        cellID_15 = ctypes.c_long(cell_lv15.id()).value
        cellID_16 = ctypes.c_long(cell_lv16.id()).value
        cellID_19 = ctypes.c_long(cell_lv19.id()).value

        session.execute(
            ps_node_lv19, (cellID, cellID_19, tuid, jsonFeature))
        session.execute(
            ps_node_lv15, (cellID, cellID_15, cellID_16, tuid, jsonFeature))
        session.execute(
            ps_node_lv11, (cellID, cellID_11, cellID_13, tuid, jsonFeature))

    else:
        a = 0
        # print 'found a complex structure!'
        # print feature['geometry']['coordinates']

cluster.shutdown()