import sys
import geojson
import s2sphere
import ctypes

ll = s2sphere.LatLng.from_degrees(float(sys.argv[1]),float(sys.argv[2]))
cell = s2sphere.CellId.from_lat_lng(ll).parent(16)
print(ctypes.c_long(cell.id()).value)
print(s2sphere.CellId.to_lat_lng(cell))
