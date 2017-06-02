#!/usr/bin/python2.7

'''Main class to load the geojson file to the database'''

import sys
from timeit import default_timer as timer
import geojson
import dbhelper
import geohelper
import cfgparser

# TODO: make these config into config.yml
# DON'T CHANGE THE VALUE HERE DIRECTLY! The value will be overwritten by cfgparser
# For detailed program behavior tunning, goto config.yml
RUN_DUPLICATION = True
RUN_CUTTING = True


def print_usage(stat):
    '''Print the usage and exit'''
    print "Usage:", __file__, "spatialData.json\n"
    sys.exit(stat)


def load_geojson(filename):
    '''Given the filename, return the geojson obj'''
    start = timer()
    in_file = open(filename, 'r').read()
    end = timer()
    print 'Reading file finished in %.5fs' % (end - start)

    start = timer()
    obj = geojson.loads(in_file)
    end = timer()
    print 'Loading json finished in %.5fs' % (end - start)
    return obj


def load_by_duplication(feature):
    '''Using the duplication method to store features into the appoprate level.
    Function behavior is controlled by global variable RUN_DUPLICATION. Insertion
    only start iff RUN_DUPLICATION is True. Change the behavior in config.yml'''
    if RUN_DUPLICATION:
        pt_list = geohelper.get_pt_list(feature)
        bboxes = geohelper.get_bboxes(pt_list)
        dbhelper.insert_by_bboxes(bboxes, feature)


def load_by_cutting(feature):
    # TODO: implement this
    '''Using the cutting method to store the features into the database for level
    (n+1) to base level. Where n+1 is the child level of the feature, base level is
    defined in config.yml. Function behavior is controlled by global variable
    RUN_DUPLICATION. Insertion only start iff RUN_DUPLICATION is True. Change
    the behavior in config.yml'''
    if RUN_CUTTING:
        pt_list = geohelper.get_pt_list(feature)
        bboxes = geohelper.get_bboxes(pt_list)
        level = geohelper.get_level(bboxes)


def run(filename):
    '''Execute entrance. Given the geojson filename, load the file to the database'''
    print 'Loading feature files...'
    features = load_geojson(filename)['features']

    print 'Storing to database...'
    start = timer()
    for feature in features:
        load_by_duplication(feature)
        load_by_cutting(feature)
    end = timer()
    print 'Done!'
    print 'Storing to db finished in %.5fs' % (end - start)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print_usage(1)
    run(sys.argv[1])
