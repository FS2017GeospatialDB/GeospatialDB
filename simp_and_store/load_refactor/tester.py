import sys
import geojson
import geohelper

def load_geojson(filename):
    '''Given the filename, return the geojson obj'''
    in_file = open(filename, 'r').read()
    return geojson.loads(in_file)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "json file given"
    geojson_obj = load_geojson(sys.argv[1])
    for feature in geojson_obj['features']:
        pt_list = geohelper.get_pt_list(feature)
        bboxes = geohelper.get_bboxes(pt_list, True)

