'''Geo helper class'''

import math


def is_correct_lng_range(lng_ranges, pt_list, percent=.3):
    '''Check if the coordinates fall in the lng_range. Percent to check can be defined as
    the last argument

    lng_range: list[tuple(min_lng, max_lng)]'''

    def in_range(lng, lng_range):
        '''Test if the lat is in the given range'''
        return True if lng >= lng_range[0] and lng <= lng_range[1] else False

    num_to_check = int(math.ceil(percent * len(pt_list)))
    i = 0
    for pt in pt_list:
        lng = pt[0]
        for lng_range in lng_ranges:
            if not in_range(lng, lng_range):
                return False
        i = i + 1
        if i == num_to_check:
            break
    return True


def find_xtrem_coord(pt_list):
    '''Find the min&max lat&lng for the feature'''
    min_lat = min_lng = 181
    max_lat = max_lng = -181
    for pt in pt_list:
            # geojson store lng first, then lat...
        lat = pt[1]
        lng = pt[0]
        if lat > max_lat:
            max_lat = lat
        if lat < min_lat:
            min_lat = lat
        if lng > max_lng:
            max_lng = lng
        if lng < min_lng:
            min_lng = lng
    return min_lat, max_lat, min_lng, max_lng


def get_bboxes(pt_list, return_lat_first=True):
    '''There are some cases that across the prime merdian (and +-180 deg). To solve that problem,
    one workaround solution is to return a list of bboxes'''

    def format_output(lng_ranges):
        '''Based on desired lat, lng order, return the list of bbox'''
        bboxes = []
        for lng_range in lng_ranges:
            if return_lat_first:
                top_left = (max_lat, lng_range[0])
                bottom_right = (min_lat, lng_range[1])
                bboxes.append((top_left, bottom_right))
            else:
                top_left = (lng_range[0], max_lat)
                bottom_right = (lng_range[1], min_lat)
                bboxes.append((top_left, bottom_right))
        return bboxes

    min_lat, max_lat, min_lng, max_lng = find_xtrem_coord(pt_list)

    # Because there is no warp-around issue for lat, we only need to
    # differentiate lng
    if min_lng < 0 and max_lng > 0:
        # find the smallest distance of two different cases first.
        abs_dist_pass_0 = max_lng - min_lng
        abs_dist_pass_180 = 2 * 180 - max_lng + min_lng
        if abs_dist_pass_0 < abs_dist_pass_180:
            # try if the smallest distance is correct. It should be the case for most
            # of the time, because normally no feature would occupy more than 180 deg
            # But if a feature is over 180 deg, this assmuption breaks. So additional
            # check required.
            # Check to see if the coordinates of the feature fall into the range. If
            # not, then we simply choose the wrong one, need to get an opposite
            # range
            lng_range = [(min_lng, max_lng)]
            if is_correct_lng_range(lng_range, pt_list):
                return format_output(lng_range)
            else:
                print 'A MISS!'
                lng_range = [(max_lng, 180 - 1e13),
                             (-180 + 1e13, min_lng)]
                return format_output(lng_range)
        else:
            lng_range = [(max_lng, 180 - 1e13),
                         (-180 + 1e13, min_lng)]
            if is_correct_lng_range(lng_range, pt_list):
                return format_output(lng_range)
            else:
                print 'A MISS!'
                lng_range = [(min_lng, max_lng)]
                return format_output(lng_range)
    else:
        lng_range = [(min_lng, max_lng)]
        return format_output(lng_range)
    assert False, "Should never reach here"
    return []


def get_pt_list(feature):
    '''Get the point list of the single feature. Regardless of it is "multi-" feature or not,
    return the data in the same format:

    list[list(lng, lat)], the order is specified by GeoJson'''

    feature_type = feature['geometry']['type']
    feature_coord = feature['geometry']['coordinates']

    def case_point():
        return [feature_coord]

    def case_linestr_mulpt():
        return feature_coord

    def case_polygon_mullinestr():
        result = []
        for coords in feature_coord:
            result.extend(coords)
        return result

    def case_mulpolygon():
        result = []
        for polygon in feature_coord:
            for coords in polygon:
                result.extend(coords)
        return result

    if feature_type == "Point":
        return case_point()
    if feature_type == "LineString" or feature_type == "MultiPoint":
        return case_linestr_mulpt()
    if feature_type == "Polygon" or feature_type == "MultiLineString":
        return case_polygon_mullinestr()
    if feature_type == "MultiPolygon":
        return case_mulpolygon()
