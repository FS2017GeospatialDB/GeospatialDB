# .py for syntax highlighting... 

NUM_COVERING_LIMIT = 3
TOP_LEVEL = 4
BASE_LEVEL = 13

def helper_func(llrect, level):
    result = asc_ordered_dict()
    # the term asc_ordered_dict is essentially a TreeMap with keys orderd in ascending order

    while size > NUM_COVERING_LIMIT:
        coverer = S2RegionCoverer()
        coverer.set_max_level(level)
        coverer.set_min_level(level)
        covering = coverer.GetCovering(llrect)
        result[level] = covering
        level = level - 1
        size = len(covering)
        # restrict the min level the seeking loop can go
        if level < TOP_LEVEL:
            print 'A FEATURE HITS MIN_LEVEL LIMIT:', TOP_LEVEL
            break
    return result
    

def query_response(l, b, r, t):      # 4 boundary coordinates
    bottom_left = S2ll(l,b)
    top_right = S2ll(r,t)
    llrect = S2llrect(bottom_left, top_right)
    level = KAvgDiag.minLevel(digonal_distance(digonal_distance))
    # restrict the max level to base level, if the generated level is larger than that
    level = BASE_LEVEL if level > BASE_LEVEL else level
    # restrict the min level to top level, just in case that feature is too large
    level = MIN_LEVEL if level < MIN_LEVEL else level
    covering_s2cells = helper_func(llrect, level)

    queried_feature_set = asc_ordered_dict()
    for level, s2cells in covering_s2cells.iteritems():
        queried_feature_set[level] = query_all_from_c*(covering_s2cells)

    result = hash_map()
    for level in queried_feature_set.iterkeys():
        features_of_level = queried_feature_set[level]
        for feature in features_of_level:
            osm_id = feature['id']
            if not result.has_key(osm_id):
                result[osm_id] = feature
            #else:
            # we actually dont need this else statement. Because we are accessing the level
            # of features in ascending order (6,7,8...), the first feature that is stored into
            # the result hash map guaranteed to be the largest feature. Then for any other
            # features with the same osm_id, we just discard them.

    # lets forget about simplification for now, test to see if the above code works as desired
    result_list = to_list(result)
    return result_list
    