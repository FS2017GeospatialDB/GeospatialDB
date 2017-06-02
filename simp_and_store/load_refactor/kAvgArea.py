'''This method works great for a good shaped rectangle i.e. the shape is similar to the tile
But it does not evaluate the overall shape well if the shape is irregular'''

import math

dim = 2
deriv = 4 * math.pi / 6


def get_min_lv(value):
    if value <= 0:
        return 30
    mantissa, exp = math.frexp(value / deriv)
    level = int(max(0, min(30, -((exp - 1) >> 1))))
    return level


def get_closest_lv(value):
    return get_min_lv(2 * value)



