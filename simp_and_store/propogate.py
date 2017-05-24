import time
import ctypes
import geojson
import s2sphere
import cassandra;

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

        # Extract Row Identification
        json = geojson.loads(row.json)
        osm_id = row.osm_id
        level = 16                          #Fix This!!!!!

        # Find Feature's Covering Region
        region = []
        coords = json['geometry']['coordinates']
        if json['geometry']['type'] == 'Point':
            coords = [coords]
        else:
            continue
        
        for coord in coords:
            latlng = s2sphere.LatLng.from_degrees(coord[1], coord[0])
            cell = s2sphere.CellId.from_lat_lng(latlng).parent(level)
            region.append(ctypes.c_long(cell.id()).value)
        region = list(set(region))

        # Perform Version Update
        for cellID in region:
            results = session.execute(slave_select_ps, (level, cellID, cassandra.util.HIGHEST_TIME_UUID, osm_id))
            if len(results.current_rows) > 1:
                print osm_id, " Error: More than one row returned (", results.current_rows, ")"

            elif not results:
                print osm_id, " is new - Updating..."
                session.execute(slave_insert_ps, (level, cellID, cassandra.util.HIGHEST_TIME_UUID, osm_id, row.json))
            
            elif results[0].json != row.json:
                print osm_id, " is dirty - Updating..."
                session.execute(slave_insert_ps, (level, cellID, cassandra.util.uuid_from_time(int(time.time()), 0, 0), osm_id, results[0].json))
                session.execute(slave_insert_ps, (level, cellID, cassandra.util.HIGHEST_TIME_UUID, osm_id, row.json))

    # Cleanup
    cluster.shutdown()

if __name__ == '__main__':
    propogate()