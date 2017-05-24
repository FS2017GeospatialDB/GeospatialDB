import json
from collections import namedtuple

data = open('map1.json','r').read()

# Parse JSON into an object with attributes corresponding to dict keys.
x = json.loads(data, object_hook=lambda d: namedtuple('GeoData', d.keys())(*d.values()))
print x.type
