import sys
import ctypes
import geojson
from s2 import *
import uuid
import time
from sets import Set
from cassandra.cluster import Cluster

minlevel = 16

def load():
	data = None
	with open(sys.argv[1], 'r') as in_file:
		data = geojson.loads(in_file.read())

	cluster = Cluster()
	session = cluster.connect('global')

	ins_statement = session.prepare('''INSERT INTO slave (level, s2_id, time, osm_id, json)
					VALUES (?, ?, ?, ?, ?)''')
	
	for feature in data['features']:
		osm_id = feature['id']
		json = geojson.dumps(feature)
		
		featuretype = feature['geometry']['type']
		featurecoord = feature['geometry']['coordinates']
	
		# A list of every lon,lat pairs a feature has
		cover = []
		if (featuretype == "Point"): case1(featurecoord, session, osm_id, json, ins_statement)
		if (featuretype == "LineString"): cover = case2(featurecoord)
		if (featuretype == "Polygon" or featuretype == "MultiLineString"): cover = case3(featurecoord)
		if (featuretype == "MultiPolygon"): cover = case4(featurecoord)

		if (featuretype == "MultiPoint"): 
			for point in featurecoord:
				case1(point, session, osm_id, json, ins_statement)

		for cellid in cover:
			new_cell = S2Cell(cellid)
			cell_value = ctypes.c_long(cellid.id()).value
			session.execute(ins_statement, (new_cell.level(), cell_value, uuid.uuid1(), osm_id, json))

	cluster.shutdown()

def bbox(ptlist):
	minlat=1000;
	minlon=1000;
	maxlat=-1000;
	maxlon=-1000;
	for pairs in ptlist:
		if (pairs[0] < minlon): minlon = pairs[0]
		if (pairs[0] > maxlon): maxlon = pairs[0]
		if (pairs[1] < minlat): minlat = pairs[1]
		if (pairs[1] > maxlat): maxlat = pairs[1]

	region_rect = S2LatLngRect(S2LatLng.FromDegrees(minlat,minlon),S2LatLng.FromDegrees(maxlat,maxlon))	

	# Region Coverer max 1 cell to get parent
	get_parent = S2RegionCoverer()
	get_parent.set_max_cells(1)
	parent_array = get_parent.GetCovering(region_rect) # Has only 1 element, the parent
	parent_cell = S2Cell(parent_array[1])
	
	# To ensure all features are not store on levels >16
	parent_level = (min(minlevel-1, parent_cell.level()))

	# Region Coverer using parents level and parent-1, to split if needed
	# If feature covers all 4 squares in parent-1, it makes sense to just store in parent
	coverer = S2RegionCoverer()
	coverer.set_max_cells(3)
	coverer.set_max_level(parent_level+1)
	coverer.set_min_level(parent_level)
	cover_array = coverer.GetCovering(region_rect)

	return cover_array

def case1(featurecoord, session, osm_id, json, ins_statement):
	global minlevel
	latlng = S2LatLng.FromDegrees(featurecoord[1], featurecoord[0])
	cell = S2CellId.FromLatLng(latlng).parent(minlevel)
	val = ctypes.c_long(cell.id()).value
	session.execute(ins_statement, (minlevel, val, uuid.uuid1(), osm_id, json))

def case2(featurecoord):
	ptlist = []
	for coords in featurecoord:
		ptlist.append(coords)
	return bbox(ptlist)

# NOT TESTED
def case3(featurecoord):
	ptlist = []
	cover = []
	for coords in featurecoord:
		for cs in coords:
			ptlist.append(cs)
		cover = cover + bbox(ptlist)
		ptlist = []
	return cover

# NOT TESTED
def case4(featurecoord):
	ptlist = []
	cover = []
	for coords in featurecoord:
		for cs in coords:
			for c in cs:
				ptlist.append(c)
			cover = cover + bbox(ptlist)
			ptlist = []
	return cover

t = time.time()
load()
print(time.time()-t)