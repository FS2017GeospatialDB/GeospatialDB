import sys
from timeit import default_timer as timer
import simplejson
import geojson


def print_usage(stat):
    sys.exit(stat)

def load_geojson(filename):
    '''Given the filename, return the geojson obj'''
    in_file = open(filename, 'r').read()
    return geojson.loads(in_file)

def run(filename):
    geojson_obj = load_geojson(filename)
    geojson_str = geojson.dumps(geojson_obj)
    print geojson_str


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print_usage(1)
    run(sys.argv[1])
