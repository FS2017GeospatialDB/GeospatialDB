#!/usr/bin/python

'''Main class to load the geojson file to the database'''

import sys
from timeit import default_timer as timer
import geojson
import dbhelper
import geohelper


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


def run(filename):
    '''Execute entrance. Given the geojson filename, load the file to the database'''
    print 'Loading feature files...'
    features = load_geojson(filename)['features']

    print 'Storing to database...'
    start = timer()
    for feature in features:
        # if geohelper.get_geo_type(feature) == 'Point':
        pt_list = geohelper.get_pt_list(feature)
        bboxes = geohelper.get_bboxes(pt_list)
        dbhelper.insert_by_bboxes(bboxes, feature)
    end = timer()
    print 'Done!'
    print 'Storing to db finished in %.5fs' % (end - start)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print_usage(1)
    run(sys.argv[1])
