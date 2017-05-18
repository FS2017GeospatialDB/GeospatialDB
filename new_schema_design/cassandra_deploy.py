from cassandra.cluster import Cluster
import geojson
import re
import uuid
import s2sphere
from math import floor
# add function to module


def stToIJ(s, MAX):
    return max(0, min(MAX - 1, int(floor(MAX * s))))


# prepare data
fstring = open('map.json', 'r').read()
data = geojson.loads(fstring)

# regex to match node (simple struct)
# as the entire project is not finished, we now can
# only deal with "node"
patt = 'node'
regx = re.compile(patt)


# CREATE TABLE NODE_PLEVEL15(
#     id          decimal,
#     part_lv14   decimal,
#     time        timeuuid,
#     feature     text,
#     PRIMARY KEY (part_lv14, id, time)
# );
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

cluster = Cluster()
session = cluster.connect('global')
session.execute('TRUNCATE NODE_PLEVEL19')
session.execute('TRUNCATE NODE_PLEVEL15')
session.execute('TRUNCATE NODE_PLEVEL11')

# some pre-calc
MAX = s2sphere.CellId.MAX_LEVEL
print MAX
p2_19 = 1 << (MAX - 19)
p2_16 = 1 << (MAX - 16)
p2_15 = 1 << (MAX - 15)
p2_13 = 1 << (MAX - 13)
p2_11 = 1 << (MAX - 11)

for feature in data['features']:
    tuid = uuid.uuid1()
    jsonFeature = geojson.dumps(feature)
    if regx.match(feature['id']):  # then it is a simple feature
        coord = feature['geometry']['coordinates']
        latlng = s2sphere.LatLng.from_degrees(coord[0], coord[1])
        #latlng = s2sphere.LatLng.from_degrees(45, 179.99)
        cell = s2sphere.CellId.from_lat_lng(latlng)

        point = latlng.to_point()
        face, u, v = s2sphere.xyz_to_face_uv(point)
        s = s2sphere.CellId.uv_to_st(u)
        t = s2sphere.CellId.uv_to_st(v)
        i = s2sphere.CellId.st_to_ij(s)
        j = s2sphere.CellId.st_to_ij(t)
        cell_lv19 = s2sphere.CellId.from_face_ij(
            face, int(floor(i / p2_19)), int(floor(j / p2_19)))
        cell_lv16 = s2sphere.CellId.from_face_ij(
            face, int(floor(i / p2_16)), int(floor(j / p2_19)))
        cell_lv15 = s2sphere.CellId.from_face_ij(
            face, int(floor(i / p2_15)), int(floor(j / p2_19)))
        cell_lv13 = s2sphere.CellId.from_face_ij(
            face, int(floor(i / p2_13)), int(floor(j / p2_19)))
        cell_lv11 = s2sphere.CellId.from_face_ij(
            face, int(floor(i / p2_11)), int(floor(j / p2_19)))

        # print 'i ', s2sphere.CellId.st_to_ij(s), 'j ', s2sphere.CellId.st_to_ij(t)
        # one = 1
        # cell_lv19 = s2sphere.CellId.from_face_ij(
        #     face, stToIJ(s, one << 19), stToIJ(t, one << 19))
        # cell_lv16 = s2sphere.CellId.from_face_ij(
        #     face, stToIJ(s, 1 << 16), stToIJ(t, 1 << 16))
        # cell_lv15 = s2sphere.CellId.from_face_ij(
        #     face, stToIJ(s, 1 << 15), stToIJ(t, 1 << 15))
        # cell_lv13 = s2sphere.CellId.from_face_ij(
        #     face, stToIJ(s, 1 << 13), stToIJ(t, 1 << 13))
        # cell_lv11 = s2sphere.CellId.from_face_ij(
        #     face, stToIJ(s, 1 << 11), stToIJ(t, 1 << 11))

        session.execute(ps_node_lv19,
                        (cell.id(), cell_lv19.id(), tuid,
                         jsonFeature))
        session.execute(ps_node_lv15,
                        (cell.id(), cell_lv15.id(), cell_lv16.id(), tuid,
                         jsonFeature))
        session.execute(ps_node_lv11,
                        (cell.id(), cell_lv11.id(), cell_lv13.id(), tuid,
                         jsonFeature))
    else:
        a = 0
        # print 'found a complex structure!'
        # print feature['geometry']['coordinates']


cluster.shutdown()
