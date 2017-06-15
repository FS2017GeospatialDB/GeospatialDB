'''Moulde to parse and load the configuration file of this dbloader.
Main interface are load(), load_module(module)'''

import sys
import yaml

#HARD_CODE FILENAME
FILENAME = 'config.yml'

# Default values for geohelper
DEFAULT_BASE_LEVEL = 16
DEFAULT_MIN_LEVEL = 4
DEFAULT_NUM_COVERING_LIMIT = 3

# Default values for main
DEFAULT_RUN_DUPLICATION = True
DEFAULT_RUN_CUTTING = True


def __load_or_default(module, key, default_value, cfg):
    if not cfg.has_key(module):
        print 'Error: module', module, 'not defined'
        sys.exit(1)
    if not cfg[module].has_key(key):
        print key, 'does not appear in module cfg:', module, '! Using default value:', default_value
        cfg[module][key] = default_value
    return cfg


def __sanitize(cfg):
    # TODO: expend and more regid sanitize
    # sanitize geohelper
    cfg = __load_or_default(
        'geohelper', 'base level', DEFAULT_BASE_LEVEL, cfg)
    cfg = __load_or_default(
        'geohelper', 'top level', DEFAULT_MIN_LEVEL, cfg)
    cfg = __load_or_default(
        'geohelper', 'num covering limit', DEFAULT_NUM_COVERING_LIMIT, cfg)
    return cfg


def __try_load(FILENAME):
    if not __try_load.cache.has_key(FILENAME):
        cfg_file = open(FILENAME, 'rb').read()
        cfg = yaml.load(cfg_file)
        __try_load.cache[FILENAME] = __sanitize(cfg)
    return __try_load.cache[FILENAME]


def load():
    '''return yml obj'''
    return __try_load(FILENAME)

def load_module(module):
    '''Given the module name, return yml obj for that specific module'''
    return __try_load(FILENAME)[module]


# INIT
__try_load.cache = dict()

if __name__ == '__main__':  # testing
    print load()
