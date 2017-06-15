'''Geo helper class'''

import math
import geojson
from s2 import *
import kAvgArea
import kAvgDiag
import cfgparser

# pre-define the base and max level of the s2 covering.
# Note: this parameter here are just place holder, they
# will be overwritten by cfgparser.
# CHANGE THE BEHAVIOR IN 'cfgparser' instead
BASE_LEVEL = MIN_LEVEL = NUM_COVERING_LIMIT = -1


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


def get_type(feature):
    '''Return the feature\'s geo type'''
    return feature['geometry']['type']


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

    list[(lng, lat)], the order is specified by GeoJson. Found an utility in geojson library,
    this function becomes an alias of that function'''
    result = []
    for thing in geojson.utils.coords(feature):
        result.append(thing)
    return result


def get_coverer_bad(bbox):
    '''BAD FUNCTION!!! THIS FUNCTION IS BROKEN, DO NOT UES.
    Given the boundary box, find the most reasonable region coverer of the area'''
    top_left = bbox[0]
    bottom_right = bbox[1]
    ll_top_left = S2LatLng.FromDegrees(top_left[0], top_left[1])
    ll_bottom_right = S2LatLng.FromDegrees(bottom_right[0], bottom_right[1])
    # digonal_distance = ll_top_left.GetDistance(ll_bottom_right).abs()

    # According to the official documentation of S2, S2LatLngRect(&p, &p)
    # specifies the first point as the lower-left coner. As we didn't follow
    # the specification strictly in our earlier function, the only solution is
    # to use another constructor to initialize the llrect
    llrect = S2LatLngRect.FromPointPair(ll_top_left, ll_bottom_right)
    level = kAvgArea.get_min_lv(llrect.Area())
    parent = level - 1 if level != 0 else 0
    # center = llrect.GetCenter()

    # Giving up on coverer. The covering algorith is a top-down algorithm, which oftentimes
    # returns undesired result if the given max cell is too low. The tables shows the result
    # At each level. For the detailed information, see the comment in the official source code:
    # https://github.com/micolous/s2-geometry-library/blob/master/geometry/s2/s2regioncoverer.h
    # max_cells:        3      4     5     6     8    12    20   100   1000
    # median ratio:  5.33   3.32  2.73  2.34  1.98  1.66  1.42  1.11   1.01
    # worst case:  215518  14.41  9.72  5.26  3.91  2.75  1.92  1.20   1.02

    coverer = S2RegionCoverer()
    coverer.set_max_cells(4)
    coverer.set_max_level(level)
    coverer.set_min_level(parent)
    covering = coverer.GetCovering(llrect)
    if len(covering) > 50:
        print 'level =', level, '\tparent =', parent
        print '#covering =', len(covering)

    try:
        assert level > 8
    except AssertionError:
        print 'Encountered a feature with level < 8'


def get_diag_distance(bbox):
    '''Given the boundary box of the feature, find the diagnoal distance of the bbox'''
    top_left = bbox[0]
    bottom_right = bbox[1]
    ll_top_left = S2LatLng.FromDegrees(top_left[0], top_left[1])
    ll_bottom_right = S2LatLng.FromDegrees(bottom_right[0], bottom_right[1])
    return ll_top_left.GetDistance(ll_bottom_right).abs().radians()


def get_level(bbox):
    '''Given the boundary box of the feature, find the most approprate level of that region.
    The function behavior is controlled by base level and top level defined in config.yml
    It only returns value between top level and base level inclusive.'''
    top_left = bbox[0]
    bottom_right = bbox[1]

    ll_top_left = S2LatLng.FromDegrees(top_left[0], top_left[1])
    ll_bottom_right = S2LatLng.FromDegrees(bottom_right[0], bottom_right[1])

    digonal_distance = ll_top_left.GetDistance(ll_bottom_right).abs().radians()

    level = kAvgDiag.get_min_lv(digonal_distance)
    # restrict the max level to base level, if the generated level is larger
    # than that
    level = BASE_LEVEL if level > BASE_LEVEL else level
    # restrict the min level to top level, just in case that feature is too
    # large
    level = MIN_LEVEL if level < MIN_LEVEL else level
    return level


def get_covering_level_from_bboxes(bboxes):
    '''Given bboxes, obtain the minimum covering level of the bboxes. Equivalent to call
    get_covering_level multiple times and take the minimum'''
    minimum = 30
    for bbox in bboxes:
        covering_level = get_covering_level(bbox)
        if minimum > covering_level:
            minimum = covering_level
    return minimum


def get_covering_level(bbox):
    '''Given the bbox, obtain the covering level of the feature. The data is stored as a side
    effect of function: get_covering. If you call this function on a feature before get_covering,
    Then the program simply runs get_level'''
    hash_value = hash(bbox)
    if not get_covering_level.dictionary.has_key(hash_value):
        # print "get_covering_level: Encountered an uncached feature"
        get_covering_level.dictionary[hash(bbox)] = get_level(bbox)
    return get_covering_level.dictionary[hash(bbox)]


def get_covering(bbox):
    '''Given the boundary box, find the most reasonable region covering
    of the area. A side effect of this function: each time a feature is processed, it keeps
    the final covering level of the feature. To obtain the covering level, call function
    get_covering_level'''
    # This part of the code dulplicates with get_level(). Remain here for slight efficiency
    # As if call get_level, multiple copies of ll_top_left, etc. need to be created,
    # because llrect will still use those variables.
    top_left = bbox[0]
    bottom_right = bbox[1]

    ll_top_left = S2LatLng.FromDegrees(top_left[0], top_left[1])
    ll_bottom_right = S2LatLng.FromDegrees(bottom_right[0], bottom_right[1])

    llrect = S2LatLngRect.FromPointPair(ll_top_left, ll_bottom_right)
    digonal_distance = ll_top_left.GetDistance(ll_bottom_right).abs().radians()

    level = kAvgDiag.get_min_lv(digonal_distance)

    # restrict the max level to base level, if the generated level is larger
    # than that
    level = BASE_LEVEL if level > BASE_LEVEL else level
    # restrict the min level to top level, just in case that feature is too
    # large
    level = MIN_LEVEL if level < MIN_LEVEL else level

    covering = []
    size = NUM_COVERING_LIMIT + 1
    while size > NUM_COVERING_LIMIT:
        coverer = S2RegionCoverer()
        coverer.set_max_level(level)
        coverer.set_min_level(level)
        covering = coverer.GetCovering(llrect)
        level = level - 1
        size = len(covering)
        # restrict the min level the seeking loop can go
        if level < MIN_LEVEL:
            print 'A FEATURE HITS MIN_LEVEL LIMIT:', MIN_LEVEL
            break

    # level + 1 to offset the -1 in the while loop
    level = level + 1
    # store the final level to get_covering_level
    get_covering_level.dictionary[hash(bbox)] = level

    ################DEBUG FUNCTION CALLS###########
    __print_new_low_lv(level)
    #__test_point(level)        # make sure when only points, they are on lv 30
    #__print_new_many_covering(len(covering))
    # check all generated covering are the same level
    __check_covering_same_level(covering)
    ################END DEBUG FUNCTIONS############
    return covering


def __test_point(level):
    assert level == 30, ("Why point is ", level)


def __print_new_low_lv(level):
    if level < __print_new_low_lv.min_lv:
        __print_new_low_lv.min_lv = level
        print 'new min level:', level


def __print_new_many_covering(length):
    if length > __print_new_many_covering.max_covering:
        __print_new_many_covering.max_covering = length
        print 'new max covering:', length


def __find_covering(llrect, init_lv, num_covering_limit=3):
    covering = []
    size = num_covering_limit + 1
    lv = init_lv
    while size > num_covering_limit:
        coverer = S2RegionCoverer()
        coverer.set_max_level(lv)
        coverer.set_min_level(lv)
        covering = coverer.GetCovering(llrect)
        size = len(covering)
        lv = lv - 1
    __print_new_low_lv(lv)
    return covering


def __check_covering_same_level(coverings):
    level = -1
    for cellid in coverings:
        cur_lv = cellid.level()
        if level == -1:
            level = cur_lv
        else:
            assert cur_lv == level, 'Level not the same???'


def __load_config():
    cfg = cfgparser.load_module('geohelper')
    global BASE_LEVEL
    global MIN_LEVEL
    global NUM_COVERING_LIMIT
    BASE_LEVEL = cfg['base level']
    MIN_LEVEL = cfg['top level']
    NUM_COVERING_LIMIT = cfg['num covering limit']


############# INITIALIZE ############
get_covering_level.dictionary = dict()
__print_new_low_lv.min_lv = 30
__print_new_many_covering.max_covering = 1
__load_config()
