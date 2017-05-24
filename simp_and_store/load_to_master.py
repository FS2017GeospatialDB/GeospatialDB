'''Main file of loading the data'''

import sys
import ctypes
import geojson
from cassandra.cluster import Cluster


def print_usage_and_exit(status):
    '''Print Usage Instructions'''
    print "Usage:", sys.argv[0], "spatialData.json\n"
    sys.exit(status)


def load():
    '''Load the data into the master'''
    # Read the JSON
    data = None
    with open(sys.argv[1], 'r') as in_file:
        data = geojson.loads(in_file.read())

    # # Connect to the Database
    cluster = Cluster()
    session = cluster.connect('global')

    # # Prepare a Statement
    master_insert_ps = session.prepare('''
        INSERT INTO master (osm_id, json) VALUES (?, ?)
    ''')

    # # Perform Insertions (or Updates)
    for feature in data['features']:
        osm_id = feature['id']
        json = geojson.dumps(feature)
        session.execute(master_insert_ps, (osm_id, json))

    # # Cleanup
    cluster.shutdown()


if __name__ == '__main__':
    # Check Command-Line Arguments
    if len(sys.argv) != 2:
        print_usage_and_exit(-1)
    load()
