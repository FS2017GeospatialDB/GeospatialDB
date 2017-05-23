import sys
import ctypes
import geojson
from cassandra.cluster import Cluster

# Print Usage Instructions
def printUsageAndExit(statusCode):
    print "Usage: ", sys.argv[0], " [spatialData.json]"
    print
    sys.exit(statusCode)

# Fill the Database
def load():

    # Read the JSON
    data = None
    with open(sys.argv[1], 'r') as inFile:
        data = geojson.loads(inFile.read())

    # Connect to the Database
    cluster = Cluster()
    session = cluster.connect('global')

    # Prepare a Statement
    master_insert_ps = session.prepare('''
        INSERT INTO master (osm_id, json) VALUES (?, ?)
    ''')

    # Perform Insertions (or Updates)
    for feature in data['features']:
        osm_id = feature['id']
        json = geojson.dumps(feature)
        session.execute(master_insert_ps, (osm_id, json))

    # Cleanup
    cluster.shutdown()

if __name__ == '__main__':

    # Check Command-Line Arguments
    if len(sys.argv) != 2:
        printUsageAndExit(-1)

    load()