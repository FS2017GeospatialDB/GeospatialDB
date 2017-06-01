'''Geo helper class'''

def is_correct_lat_range(lat_ranges, pt_list, percent=.3):
    '''Check if the coordinates fall in the lat_range. Percent to check can be defined as
    the last argument

    lat_range: list[tuple(min_lat, max_lat)]'''

    def in_range(lat, lat_range):
        '''Test if the lat is in the given range'''
        return True if lat >= lat_range[0] and lat <= lat_range[1] else False

    num_to_check = int(math.ceil(percent * len(pt_list)))
    i = 0
    for pt in pt_list:
        lat = pt[1]
        for lat_range in lat_ranges:
            if not in_range(lat, lat_range):
                return False
        i = i + 1
        if i == num_to_check:
            break
    return True


def get_bboxes(pt_list, return_lat_first):
    '''There are some cases that across the prime merdian (and +-180 deg). To solve that problem,
    one workaround solution is to return a list of bboxes'''

    min_lat = min_lng = 181
    max_lat = max_lng = -181

    def find_xtrem_coord(min_lat, max_lat, min_lng, max_lng):
        '''Find the min&max lat&lng for the feature'''
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

    def format_output(lat_ranges):
        '''Based on desired lat, lng order, return the list of bbox'''
        bboxes = []
        for lat_range in lat_ranges:
            if return_lat_first:
                top_left = tuple(lat_range[0], max_lng)
                bottom_right = tuple(lat_range[1], min_lng)
                bboxes.insert(tuple(top_left, bottom_right))
            else:
                top_left = tuple(max_lng, lat_range[0])
                bottom_right = tuple(min_lng, lat_range[1])
                bboxes.insert(tuple(top_left, bottom_right))
        return bboxes

    find_xtrem_coord(min_lat, max_lat, min_lng, max_lng)

    # Because there is no warp-around issue for lng, we only need to
    # differentiate lat
    if min_lat < 0 and max_lat > 0:
        # find the smallest distance of two different cases first.
        abs_dist_pass_0 = max_lat - min_lat
        abs_dist_pass_180 = 2 * 180 - max_lat + min_lat
        if abs_dist_pass_0 < abs_dist_pass_180:
            # try if the smallest distance is correct. It should be the case for most
            # of the time, because normally no feature would occupy more than 180 deg
            # But if a feature is over 180 deg, this assmuption breaks. So additional
            # check required.
            # Check to see if the coordinates of the feature fall into the range. If
            # not, then we simply choose the wrong one, need to get an opposite
            # range
            lat_range = [tuple(min_lat, max_lat)]
            if is_correct_lat_range(lat_range, pt_list):
                return format_output(lat_range)
            else:
                lat_range = [tuple(max_lat, 180 - 1e13), tuple(-180 + 1e13, min_lng)]
                return format_output(lat_range)
        else:
            lat_range = [tuple(max_lat, 180 - 1e13),
                         tuple(-180 + 1e13, min_lng)]
            if is_correct_lat_range(lat_range, pt_list):
                return format_output(lat_range)
            else:
                lat_range = [tuple(min_lat, max_lat)]
                return format_output(lat_range)
    else:
        lat_range = [tuple(min_lat, max_lat)]
        return format_output(lat_range)

