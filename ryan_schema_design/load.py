import sys
import time
import json
import ijson
import slice
import ctypes
import logging
import cassandra

from cassandra.cluster import Cluster
ranges = {4: 1e-1, 8: 1e-2, 12: -1e-11}

# Setup Logging
logging.basicConfig(filename="/var/tmp/load_features.log", filemode="w", level=logging.INFO)

# Print Usage Instructions
def printUsageAndExit(statusCode):
    print "Usage: ", sys.argv[0], " [spatialData1.json ... spatialDataN.json]"
    print
    sys.exit(statusCode)

# Fill the Database
def load(fileList):
    """
    Load a GeoJSON file/files (specified by fileList) into the Cassandra database.

    Dialect: GeoJSON converted from OSM by ogr2ogr
    """

    # Connect to the Database
    logging.info("Connecting to Cluster...")
    session = None
    hosts = ['127.0.0.1', '138.67.187.226', '138.67.187.225', '138.67.187.149', '138.67.187.228', '138.67.186.219',
           '138.67.187.218', '138.67.187.137', '138.67.186.191', '138.67.187.223', '138.67.186.221', '138.67.187.205']
    for host in hosts:
        try:
            cluster = Cluster([host])
            session = cluster.connect('global')
            break
        except:
            logging.info("Failed to connect to host %s...", host)
    
    if session is None:
        logging.info("Failed to connect to any host in %s. Exiting...", hosts)
        return -2

    # Prepared Statements
    master_select_ps = session.prepare('''
        SELECT * FROM master WHERE osm_id=?
    ''')
    master_insert_ps = session.prepare('''
        INSERT INTO master (osm_id, json) VALUES (?, ?)
    ''')
    slave_insert_ps = session.prepare('''
        INSERT INTO slave(level, s2_id, time, osm_id, is_cut, json)
        VALUES (?, ?, ?, ?, ?, ?)
    ''')

    # Read the JSONs
    logging.info("Received JSON list: %s", fileList)
    for filename in fileList:
        logging.info("Opening JSON (%s)...", filename)
        with open(filename, 'r') as inFile:
            for feature in jsonItems(inFile, 'features.item'):
                start = time.time()

                # Extract ID
                osm_id = feature['id']
                feat_range = slice.enclosed_range(feature)

                # Fix Timeout Issue
                while True:
                    try:
                        # Check if ID already exists
                        if session.execute(master_select_ps, (osm_id,)):
                            logging.info("ID %s already exists in database. Skipping...", osm_id)
                            break

                        # Insert Feature into Master
                        logging.info("Inserting %s into Master...", osm_id)
                        myjson = json.dumps(feature)
                        session.execute(master_insert_ps, (osm_id, myjson))

                        # Insert Feature into Slave
                        logging.info("Inserting %s into Slave...", osm_id)
                        for level in ranges.keys():

                            # Only put visible features into high levels
                            if feat_range < ranges[level]:
                               continue
                                
                            pieces = slice.slice_feature(feature, level)
                            for cellID,cellJson in pieces.items():
                                cellID = (cellID + 2**63) % 2**64 - 2**63                       # Convert to 64-bit Signed Integer
                                cellJson = json.dumps(cellJson)
                                
                                session.execute(slave_insert_ps, (level, cellID, cassandra.util.uuid_from_time(int(time.time()), 0, 0), osm_id, True))
                                session.execute(slave_insert_ps, (level, cellID, cassandra.util.HIGHEST_TIME_UUID, osm_id, True, cellJson))
                        break
                    except Exception as e:
                        print "Exception!", e

                # Done
                end = time.time()
                logging.info("Finished with %s in %f seconds...", osm_id, (end - start))
        logging.info("Closing %s...", filename)

    # Cleanup
    logging.info("Done! %d files processed...", len(fileList))
    cluster.shutdown()

def jsonItems(file, prefix):
    items = ijson.parse(file)
    try:
        while True:
            current, event, value = next(items)
            if current == prefix:
                builder = ijson.common.ObjectBuilder()
                end_event = event.replace('start', 'end')
                while (current, event) != (prefix, end_event):
                    if event == 'number':
                        value = float(value)
                    builder.event(event, value)
                    current, event, value = next(items)
                yield builder.value
    except StopIteration:
        pass 

if __name__ == '__main__':

    # Check Command-Line Arguments
    if len(sys.argv) < 2:
        printUsageAndExit(-1)

    load(sys.argv[1:])
