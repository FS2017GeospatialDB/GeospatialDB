import time
import slice
import ctypes
import geojson
import s2sphere
import cassandra

from cassandra.cluster import Cluster

MAX_DATE = (1 << 47)

def propogate():
    # Connect to the Database
    cluster = Cluster()
    session = cluster.connect('global')

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
            level = 16                          #Fix This!!!!!

            # Find Feature's Covering Region
            print
            print ">> ",osm_id
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
            for cellID,cellJson in jsons.items():
                cellID = ctypes.c_long(cellID).value
                cellJson = geojson.dumps(cellJson)
                results = session.execute(slave_select_ps, (level, cellID, cassandra.util.HIGHEST_TIME_UUID, osm_id))

                if len(results.current_rows) > 1:
                    print osm_id, " Error: More than one row returned (", results.current_rows, ")"

                elif not results:
                    print osm_id, " is new - Updating..."
                    session.execute(slave_insert_ps, (level, cellID, cassandra.util.HIGHEST_TIME_UUID, osm_id, cellJson))
                
                elif results[0].json != cellJson:
                    print osm_id, " is dirty - Updating..."
                    session.execute(slave_insert_ps, (level, cellID, cassandra.util.uuid_from_time(int(time.time()), 0, 0), osm_id, results[0].json))
                    session.execute(slave_insert_ps, (level, cellID, cassandra.util.HIGHEST_TIME_UUID, osm_id, cellJson))

        except NotImplementedError:
            print "Not implemented. Hold the line."
    # Cleanup
    cluster.shutdown()

if __name__ == '__main__':
    propogate()