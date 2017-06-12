import math

dim = 1
deriv = 2.060422738998471683  # S2_QUADRATIC_PROJECTION

M_SQRT2 = 1.41421356237309504880168


def get_min_lv(value):
    if value <= 0:
        return 30
    mantissa, exp = math.frexp(value / deriv)
    level = int(max(0, min(30, -(exp - 1))))
    return level


def get_closest_lv(value):
    return get_min_lv(M_SQRT2 * value)
