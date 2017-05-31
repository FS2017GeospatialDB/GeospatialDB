import sys
import ctypes
import geojson
import s2sphere
import uuid
import time
from sets import Set
from cassandra.cluster import Cluster

def load():
	data = None
	with open(sys.argv[1], 'r') as in_file:
		data = geojson.loads(in_file.read())

	cluster = Cluster()
	session = cluster.connect('global')
	#level = 16
	#s2threshold = 100

	ins_statement = session.prepare('''INSERT INTO slave (level, s2_id, time, osm_id, json)
					VALUES (?, ?, ?, ?, ?)''')
	#ins_master = session.prepare('''INSERT INTO master (osm_id, json) VALUES (?,?)''')
	
	s = Set([])
	for feature in data['features']:
		osm_id = feature['id']
		json = geojson.dumps(feature)
		
		featuretype = feature['geometry']['type']
		featurecoord = feature['geometry']['coordinates']
		
		# A list of every lon,lat pairs a feature has
		#ptlist = []

		if (featuretype == "Point"): case1(featurecoord, session, osm_id, json, ins_statement)
		#if (featuretype == "LineString" or featuretype == "MultiPoint"): case2(featurecoord)
		#if (featuretype == "Polygon" or featuretype == "MultiLineString"): case3(featurecoord)
		#if (featuretype == "MultiPolygon"): case4(featurecoord)

		# Set to track distinct S2 values
		'''s2set = Set([])

		for pt in ptlist:
			latlng = s2sphere.LatLng.from_degrees(pt[1], pt[0])
			cell = s2sphere.CellId.from_lat_lng(latlng).parent(level)
			s2set.add(ctypes.c_long(cell.id()).value)
		
		if (len(s2set) < s2threshold):
			for s2 in s2set:
				session.execute(ins_statement, (level, s2, uuid.uuid1(), osm_id, json))
		else:
			for s2 in s2set:
				session.execute(ins_statement, (level, s2, uuid.uuid1(), osm_id))
				session.execute(ins_master, (osm_id, json))'''
	
	cluster.shutdown()

def bbox(ptlist, session, osm_id, json, ins_statement):
	minlat=1000;
	minlon=1000;
	maxlat=-1000;
	maxlon=-1000;
	for pairs in ptlist:
		if (pairs[0] < minlon): minlon = pairs[0]
		if (pairs[0] > maxlon): maxlon = pairs[0]
		if (pairs[1] < minlat): minlat = pairs[1]
		if (pairs[1] > maxlat): maxlat = pairs[1]

	region_rect = s2sphere.LatLngRect(s2sphere.LatLng.from_degrees(minlat,minlon),s2sphere.LatLng.from_degrees(maxlat,maxlon))

	# Region Coverer max 1 cell to get parent
	get_parent = s2sphere.RegionCoverer()
	get_parent.max_cells = 1
	parent_array = get_parent.get_covering(region_rect) # Has only 1 element, the parent
	parent = s2sphere.Cell(parent_array[0])

	# Region Coverer using parents level and parent-1, to split if needed
	# If feature covers all 4 squares in parent-1, it makes sense to just store in parent
	coverer = s2sphere.RegionCoverer()
	coverer.max_cells = 3
	coverer.max_level = parent.level()+1
	coverer.min_level = parent.level()
	cover_array = coverer.get_covering(region_rect)

	for cellid in cover_array:
		new_cell = s2sphere.Cell(cellid)
		cell_value = ctypes.c_long(cellid.id()).value
		#print(new_cell.id())
		#print(cell_value)
		session.execute(ins_statement, (new_cell.level(), cell_value, uuid.uuid1(), osm_id, json))


def case1(featurecoord, session, osm_id, json, ins_statement):
	ptlist = []
	ptlist.append(featurecoord)
	bbox(ptlist, session, osm_id, json, ins_statement)
	

def case2(featurecoord):
	ptlist = []
	for coords in featurecoord:
		ptlist.append(coords)
	return ptlist

def case3(featurecoord):
	ptlist = []
	for coords in featurecoord:
		for cs in coords:
			ptlist.append(cs)
	return ptlist
		
def case4(featurecoord):
	ptlist = []
	for coords in featurecoord:
		for cs in coords:
			for c in cs:
				ptlist.append(c)
	return ptlist

t = time.time()
load()
print(time.time()-t)