import geojson
import s2sphere

def slicePoint(pointJson, level):
	coord = pointJson['geometry']['coordinates']
	latlng = s2sphere.LatLng.from_degrees(coord[1], coord[0])
	cellID = s2sphere.CellId.from_lat_lng(latlng).parent(level).id()

	return {cellID: pointJson}

def sliceMultiPoint(multiPointJson, level):
	coords = multiPointJson['geometry']['coordinates']
	multiPointJson['geometry']['coordinates'] = []
	pointJson = multiPointJson.copy()
	pointJson['geometry']['type'] = 'Point'

	result = {}
	for point in coords:
		pointJson['geometry']['coordinates'] = point
		pointLocation = slicePoint(pointJson, level)

		for location in pointLocation.keys():
			if not location in result:
				result[location] = multiPoint.copy()
			result[location]['geometry']['coordinates'].append(point)

	for cellID, multiPoint in result.items():
		if len(multiPoint['geometry']['coordinates']) == 1:
			multiPoint['geometry']['coordinates'] = multiPoint['geometry']['coordinates'][0]
			multiPoint['geometry']['type'] = 'Point'
	return result

def sliceLineString(lineStringJson, level):
	line = lineStringJson['geometry']['coordinates']
	lineStringJson['geometry']['coordinates'] = []
	pointJson = lineStringJson.copy()
	pointJson['geometry']['type'] = 'Point'

	result = {}, lastPoint = None, lastCellID = None
	for point in line:
		pointJson['geometry']['coordinates'] = point
		pointLocation = slicePoint(pointJson, level)

		for location in pointLocation.keys():

			success = False
			while not success:
				if lastCellID == None:
					lastCellID = location
					lastPoint = point
					result[location] = lineStringJson.copy()
					result[location]['geometry']['coordinates'].append(point)

				if location != lastCellID:
					deltaLat = point[0] - lastPoint[0]
					deltaLong = point[1] - lastPoint[1]
					latlng = s2sphere.LatLng.from_degrees(lastPoint[1], lastPoint[0])
					lastCell = s2sphere.CellId.from_lat_lng(latlng)
					bottom, right, up, left = lastCell.get_edge_neighbors()

					rightT = (mean(right.to_lat_lng().lng(), lastCell.to_lat_lng().lng()) - lastPoint[1]) / deltaLong
					leftT = (lastPoint[1] - mean(left.to_lat_lng().lng(), lastCell.to_lat_lng().lng())) / deltaLong
					upT = (mean(up.to_lat_lng.lat(), lastCell.to_lat_lng.lat()) - lastPoint[0]) / deltaLat
					downT = (lastPoint[0] - mean(down.to_lat_lng.lat(), lastCell.to_lat_lng.lat())) / deltaLat
					
					decision = min(max(leftT, rightT), max(upT, downT))
					fakePoint = [decision * deltaLat + lastPoint[0], decision * deltaLong + lastPoint[0]]
					nextLocation = (decision == rightT ? right.id() : (decision == leftT ? left.id() : (decision == upT ? up.id() : down.id())))
					
					result[location]['geometry']['coordinates'].append(fakePoint)
					if not nextLocation in result
						result[nextLocation] = lineStringJson.copy()
					result[nextLocation]['geometry']['coordinates'].append(fakePoint)

					lastCellID = nextLocation
					lastPoint = fakePoint

				else:
					result[location]['geometry']['coordinates'].append(point)
					success = True
	return result

def sliceMultiLineString(multiLineStringJson, level):
	lines = multiLineStringJson['geometry']['coordinates']
	multiLineStringJson['geometry']['coordinates'] = []
	lineStringJson = multiLineStringJson.copy()
	lineStringJson['geometry']['type'] = 'Point'

	result = {}
	for line in lines:
		lineStringJson['geometry']['coordinates'] = line
		lineLocations = sliceLineString(lineStringJson, level)

		for location in lineLocations.keys():
			if not location in result
				result[location] = multiLineStringJson.copy()
			result[location]['geometry']['coordinates'].append(lineLocations[location]['geometry']['coordinates'])

	for location in result:
		if len(result[location]) == 1:
			result[location]['geometry']['type'] = 'LineString'
			result[location]['geometry']['coordinates'] = result[location]['geometry']['coordinates'][0]
	return result