import geojson
import re

fin = open('map.json','r').read()

data = geojson.loads(fin)

str1 = 'way/4256958'

patt = 'way'

reg = re.compile(patt)

if reg.match(str1, re.I):
    print 'match'
