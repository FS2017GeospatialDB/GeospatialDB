import sys
import time
import ijson
import slice
import ctypes
import logging
import cassandra

from cassandra.cluster import Cluster

# Setup Logging
logging.basicConfig(filename="/var/tmp/load_features.log", level=logging.INFO)

# Print Usage Instructions
def printUsageAndExit(statusCode):
    print "Usage: ", sys.argv[0], " [spatialData.json]"
    print
    sys.exit(statusCode)

# Fill the Database
def load(filename):
    """
    Load a GeoJSON file (specified by filename) into the Cassandra database.

    Dialect: GeoJSON converted from OSM by ogr2ogr
    """

    # Read the JSON
    logging.info("Opening JSON (%s)...", filename)
    with open(filename, 'r') as inFile:
        features = ijson.items(inFile, 'features.item')

        # Connect to the Database
        logging.info("Connecting to Cluster...")
        cluster = Cluster()
        session = cluster.connect('global')

        # Prepared Statements
        master_select_ps = session.prepare('''
            SELECT * FROM master WHERE osm_id=?
        ''')
        master_insert_ps = session.prepare('''
            INSERT INTO master (osm_id, json) VALUES (?, ?)
        ''')
        slave_insert_ps = session.prepare('''
            INSERT INTO slave(level, s2_id, time, osm_id, json)
            VALUES (?, ?, ?, ?, ?)
        ''')

        # Perform Insertions
        for feature in features:
            start = time.time()

            # Extract ID
            osm_id = feature['osm_id'] if feature['osm_id'] is not None else feature['osm_way_id']
            if osm_id is None:
                logging.info("Can't Find ID in JSON: %s", feature)
                continue
            osm_id = osm_id + feature['geometry']['type']

            # Check if ID already exists
            results = session.execute(master_select_ps, (osm_id))
            if len(results.current_rows) > 1:
                logging.info("ID %s already exists in database. Skipping...", osm_id)
                continue

            # Insert Feature into Master
            logging.info("Inserting %s into Master...", osm_id)
            json = ijson.dumps(feature)
            session.execute(master_insert_ps, (osm_id, json))

            # Insert Feature into Slave
            logging.info("Inserting %s into Slave...", osm_id)
            level = 12
            pieces = slice.slice(json, level)
            for cellID,cellJson in pieces.items():
                cellID = (cellID + 2**63) % 2**64 - 2**63                       # Convert to 64-bit Signed Integer
                cellJson = ijson.dumps(cellJson)
                
                session.execute(slave_insert_ps, (level, cellID, cassandra.util.uuid_from_time(int(time.time()), 0, 0), osm_id))
                session.execute(slave_insert_ps, (level, cellID, cassandra.util.HIGHEST_TIME_UUID, osm_id, cellJson))

            # Done
            end = time.time()
            logging.info("Finished with %s in %f seconds...", osm_id, (end - start))

        # Cleanup
        cluster.shutdown()

if __name__ == '__main__':

    # Check Command-Line Arguments
    if len(sys.argv) != 2:
        printUsageAndExit(-1)

    load(sys.argv[1])
