import numpy
import geojson
import s2sphere

from copy import deepcopy

def slicePoint(pointJson, level):
	coord = pointJson['geometry']['coordinates']
	latlng = s2sphere.LatLng.from_degrees(coord[1], coord[0])
	cellID = s2sphere.CellId.from_lat_lng(latlng).parent(level).id()

	return {cellID: pointJson}

def sliceMultiPoint(multiPointJson, level):
	coords = multiPointJson['geometry']['coordinates']
	multiPointJson['geometry']['coordinates'] = []
	pointJson = dict(multiPointJson)
	pointJson['geometry']['type'] = 'Point'

	result = {}
	for point in coords:
		pointJson['geometry']['coordinates'] = point
		pointLocation = slicePoint(pointJson, level)

		for location in pointLocation.keys():
			if not location in result:
				result[location] = dict(multiPointJson)
			result[location]['geometry']['coordinates'].append(list(point))

	for cellID, multiPoint in result.items():
		if len(multiPoint['geometry']['coordinates']) == 1:
			multiPoint['geometry']['coordinates'] = list(multiPoint['geometry']['coordinates'][0])
			multiPoint['geometry']['type'] = 'Point'

	return result

def sliceLineString(lineStringJson, level):
	line = lineStringJson['geometry']['coordinates']
	lineStringJson['geometry']['coordinates'] = []
	pointJson = deepcopy(lineStringJson)
	pointJson['geometry']['type'] = 'Point'

	result = {}
	lastPoint = lastCellID = None
	for point in line:
		pointJson['geometry']['coordinates'] = point
		pointLocation = slicePoint(pointJson, level)

		for location in pointLocation.keys():

			success = False
			while not success:
				if lastCellID == None:
					lastCellID = location
					lastPoint = point
					result[location] = deepcopy(lineStringJson)

				if location != lastCellID:
					deltaLat = point[1] - lastPoint[1]
					deltaLong = point[0] - lastPoint[0]
					lastCell = s2sphere.CellId(lastCellID)
					down, right, up, left = lastCell.get_edge_neighbors()

					rightT = (numpy.mean([right.to_lat_lng().lng().degrees, lastCell.to_lat_lng().lng().degrees]) - lastPoint[0]) / deltaLong
					leftT = (numpy.mean([left.to_lat_lng().lng().degrees, lastCell.to_lat_lng().lng().degrees]) - lastPoint[0]) / deltaLong
					upT = (numpy.mean([up.to_lat_lng().lat().degrees, lastCell.to_lat_lng().lat().degrees]) - lastPoint[1]) / deltaLat
					downT = (numpy.mean([down.to_lat_lng().lat().degrees, lastCell.to_lat_lng().lat().degrees]) - lastPoint[1]) / deltaLat
					
					decision = min(max(leftT, rightT), max(upT, downT))
					fakePoint = [(decision * deltaLong) + lastPoint[0], (decision * deltaLat) + lastPoint[1]]
					nextLocation = right.id() if (decision == rightT) else (left.id() if (decision == leftT) else (up.id() if (decision == upT) else down.id()))

					
					if decision == rightT:
						print "Right!\t",
					elif decision == leftT:
						print "Left!\t",
					elif decision == upT:
						print "Up!\t",
					elif decision == downT:
						print "Down!\t",
					print lastCell.id(), nextLocation, location

					cell = s2sphere.Cell(lastCell)
					print cell.get_vertex_raw(0), cell.get_vertex_raw(1), cell.get_vertex_raw(2), cell.get_vertex_raw(3),
					print cell.get_edge_raw(0), cell.get_edge_raw(1), cell.get_edge_raw(2), cell.get_edge_raw(3)
					print cell.get_rect_bound()
					"""
					print leftT, upT, rightT, downT
					print deltaLat, deltaLong, lastCell.to_lat_lng().lat().degrees,
					print lastCell.to_lat_lng().lng().degrees, lastCell.id(), nextLocation, location
					print
					"""
					
					result[lastCellID]['geometry']['coordinates'].append(fakePoint)
					if not nextLocation in result:
						result[nextLocation] = deepcopy(lineStringJson)
					result[nextLocation]['geometry']['coordinates'].append(fakePoint)

					lastCellID = nextLocation
					lastPoint = fakePoint
				else:
					result[location]['geometry']['coordinates'].append(list(point))
					success = True
	return result

def sliceMultiLineString(multiLineStringJson, level):
	lines = multiLineStringJson['geometry']['coordinates']
	multiLineStringJson['geometry']['coordinates'] = []
	lineStringJson = dict(multiLineStringJson)
	lineStringJson['geometry']['type'] = 'Point'

	result = {}
	for line in lines:
		lineStringJson['geometry']['coordinates'] = line
		lineLocations = sliceLineString(lineStringJson, level)

		for location in lineLocations.keys():
			if not location in result:
				result[location] = dict(multiLineStringJson)
			result[location]['geometry']['coordinates'].append(lineLocations[location]['geometry']['coordinates'])

	for location in result:
		if len(result[location]) == 1:
			result[location]['geometry']['type'] = 'LineString'
			result[location]['geometry']['coordinates'] = result[location]['geometry']['coordinates'][0]
	return result