#!/usr/bin/python2.7

'''Main class to load the geojson file to the database'''

import sys
from timeit import default_timer as timer
import ijson
import geojson
import dbhelper
import geohelper
import cfgparser
import slicing

'''Only purpose is to track progress of loading'''
from track import count

# DON'T CHANGE THE VALUE HERE DIRECTLY! The value will be overwritten by cfgparser
# For detailed program behavior tunning, goto config.yml
RUN_DUPLICATION = False
RUN_CUTTING = False


def print_usage(stat):
    '''Print the usage and exit'''
    print "Usage:", __file__, "spatialData.json\n"
    sys.exit(stat)


def load_by_duplication(feature):
    '''Using the duplication method to store features into the appoprate level.
    Function behavior is controlled by global variable RUN_DUPLICATION. Insertion
    only start iff RUN_DUPLICATION is True. Change the behavior in config.yml'''
    if RUN_DUPLICATION:
        pt_list = geohelper.get_pt_list(feature)
        bboxes = geohelper.get_bboxes(pt_list)
        dbhelper.insert_by_bboxes(bboxes, feature)


def load_by_cutting(feature):
    '''Using the cutting method to store the features into the database for level
    (n+1) to base level. Where n+1 is the child level of the feature, base level is
    defined in config.yml. This function uses geohelper.get_covering_level_from_bboxes
    to find the root level (best approximation) of the feature. For best result, use
    with load_by_duplication together. Function behavior is controlled by global variable
    RUN_CUTTING. Insertion only start iff RUN_CUTTING is True. Change the behavior in config.yml'''
    if RUN_CUTTING:
        pt_list = geohelper.get_pt_list(feature)
        bboxes = geohelper.get_bboxes(pt_list)
        n = geohelper.get_covering_level_from_bboxes(bboxes)
        # for n+1 to base level. The situation may be n+1 is even larger than
        # base level, for specific base level required (e.g. lv13).
        # So an if statement here guarentees the loading of the feature to
        # at least lv n+1 once.
        # Second edition: we don't actually need to store the feature at least
        # once. Because we are using both cutting and duplication methond the
        # same time. Duplication guarentees to store the feature, thus no need
        # to store an unnecessary copy here again.
        # if n + 1 > load_by_cutting.base_level:
        #     cut_feature = slicing.slice_feature(feature, n + 1)
        #     dbhelper.insert_by_cut_feature(cut_feature)
        # else:
        #     # end + 1 because we want base level inclusive
        #     for cutting_lv in range(n + 1, load_by_cutting.base_level + 1):
        #         cut_feature = slicing.slice_feature(feature, cutting_lv)
        #         dbhelper.insert_by_cut_feature(cut_feature)

        # end + 1 because we want base level inclusive
        for cutting_lv in range(n + 1, load_by_cutting.base_level + 1):
            cut_feature = slicing.slice_feature(feature, cutting_lv)
            dbhelper.insert_by_cut_feature(cut_feature)


def load_into_master(feature):
    '''Load the original copy of the feature into the master copy table'''
    dbhelper.insert_master(feature)


def run(file_list):
    '''Execute entrance. Given the geojson filename, load the file to the database'''
    print 'Loading feature files...'
    for filename in file_list:
        with open(filename, 'r') as file:
            #features = load_geojson(filename)['features']
            print 'Storing to database...'
            start = timer()
            for feature in jsonItems(file, 'features.item'):
                load_into_master(feature)
                load_by_duplication(feature)
                load_by_cutting(feature)

            end = timer()
            print 'Done!'
            print 'Storing to db finished in %.5fs' % (end - start)
            count()


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


def __load_config():
    cfg = cfgparser.load()
    global RUN_DUPLICATION
    global RUN_CUTTING
    global CFG
    RUN_DUPLICATION = cfg['main']['duplication method']
    RUN_CUTTING = cfg['main']['cutting method']
    load_by_cutting.base_level = cfg['geohelper']['base level']


__load_config()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_usage(1)
    run(sys.argv[1:])
