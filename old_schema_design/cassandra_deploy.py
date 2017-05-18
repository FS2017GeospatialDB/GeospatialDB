from cassandra.cluster import Cluster
import geojson
import re
import uuid

# prepare data
fin = open('map.json', 'r').read()
data = geojson.loads(fin)

# regex to match node (simple struct)
patt = 'node'
reg = re.compile(patt)

# if reg.match(str1):
#     print 'match'

ps_node = """
INSERT INTO NODE (id, time, lat, lon, movable, version, feature)
VALUES (%s,%s,%s,%s,%s,%s,%s)
"""
# id        uuid,
# time      timeuuid,
# lat       float,
# lon       float,
# movable   boolean,
# version   int,
# feature   blob,

cluster = Cluster()
session = cluster.connect('colorado')
session.execute('TRUNCATE node')


for feature in data['features']:
    # print feature
    if reg.match(feature['id']):  # then it is a simple feature
        print 'found a node!'
        coord = feature['geometry']['coordinates']
        ver = feature['properties']['version']
        print coord
        session.execute(ps_node, 
        (uuid.uuid4(), uuid.uuid1(), coord[0], coord[1], True, int(ver), 
        'asd'#geojson.dumps(feature)
        ))
    else:
        a = 0
        # print 'found a complex structure!'
        # print feature['geometry']['coordinates']


cluster.shutdown()
