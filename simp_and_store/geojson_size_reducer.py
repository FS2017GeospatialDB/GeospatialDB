#!/usr/bin/python

import re
import sys
from timeit import default_timer as timer
import simplejson
import geojson


def print_usage(stat):
    print 'Example: ./geojson_size_reducer input.json > output.json'
    print'Or ./geojson_size_reducer input.json true > output.json to butify'
    sys.exit(stat)


def load_geojson(filename):
    '''Given the filename, return the geojson obj'''
    in_file = open(filename, 'rb').read()
    return geojson.loads(in_file)


def run(filename, enlarge='false'):
    geojson_obj = load_geojson(filename)
    if re.match(r"true", enlarge, re.IGNORECASE):
        geojson_str = geojson.dumps(geojson_obj, indent=2)
    else:
        geojson_str = geojson.dumps(geojson_obj)
    print geojson_str


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print_usage(1)
    if len(sys.argv) == 3:
        run(sys.argv[1], sys.argv[2])
    else:
        run(sys.argv[1])
