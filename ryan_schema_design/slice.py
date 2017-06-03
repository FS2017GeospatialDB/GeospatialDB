import math
import numpy
import geojson
import s2sphere

from copy import deepcopy

toLeft = {0:4, 1:0, 2:4, 3:1, 4:3, 5:4}
toRight = {0:1, 1:3, 2:1, 3:4, 4:0, 5:1}
toTop = {0:2, 1:2, 2:3, 3:2, 4:2, 5:0}
toBottom = {0:5, 1:5, 2:0, 3:5, 4:5, 5:3}

def slicePoint(pointJson, level):
	pointJson = deepcopy(pointJson)
	coord = pointJson['geometry']['coordinates']
	latlng = s2sphere.LatLng.from_degrees(coord[1], coord[0])
	cellID = s2sphere.CellId.from_lat_lng(latlng).parent(level).id()

	return {cellID: pointJson}

def sliceMultiPoint(multiPointJson, level):
	multiPointJson = deepcopy(multiPointJson)
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

def sliceLineString(lineStringJson, level, clockwise = True):
	lineStringJson = deepcopy(lineStringJson)
	line = lineStringJson['geometry']['coordinates']
	lineStringJson['geometry']['coordinates'] = []
	pointJson = deepcopy(lineStringJson)
	pointJson['geometry']['type'] = 'Point'

	result = {}
	lastCell = lastPoint = None
	for point in line:
		cell = s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(point[1], point[0])).parent(level)
		point = s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(point[1], point[0]))

		success = False
		while not success:
			if lastCell == None:
				lastCell = cell
				lastPoint = point
				result[cell.id()] = deepcopy(lineStringJson)

			if cell.id() != lastCell.id():
				s1 = t1 = s2 = t2 = s3 = t3 = s4 = t4 = 0

				# Handle Different Sides
				if lastCell.face() != cell.face():
					s3,t3 = get_st(lastPoint)
					s4,t4 = get_st_for_face(lastPoint.face(), get_closest_xyz(lastPoint, point))
					print "(Different Face)", get_closest_xyz(lastPoint, point)
					raise NotImplementedError("LineStrings on different faces not implemented.")

				# Handle Same Side
				else:
					s3,t3 = get_st(lastPoint)
					s4,t4 = get_st(point)

				cs1, ct1 = get_st_for_face(lastCell.face(), get_xyz(s2sphere.CellId.from_point(s2sphere.Cell(lastCell).get_vertex(0))))
				cs2, ct2 = get_st_for_face(lastCell.face(), get_xyz(s2sphere.CellId.from_point(s2sphere.Cell(lastCell).get_vertex(2))))
				rightT, leftT, upT, downT = max(cs1, cs2) - s3, min(cs1, cs2) - s3, max(ct1, ct2) - t3, min(ct1, ct2) - t3
				
				if rightT == 0:
					rightT += 1e-6
				if leftT == 0:
					leftT -= 1e-6
				if upT == 0:
					upT += 1e-6
				if downT == 0:
					downT -= 1e-6
				
				try:
					rightT /= (s4 - s3)
					leftT /= (s4 - s3)
				except ZeroDivisionError:
					rightT = leftT = float('inf')

				try:
					upT /= (t4 - t3)
					downT /= (t4 - t3)
				except ZeroDivisionError:
					upT = downT = float('inf')

				decision = min(max(leftT, rightT), max(upT, downT))
				down, right, up, left = lastCell.get_edge_neighbors()
				nextCell = s2sphere.CellId(right.id() if (decision == rightT) \
					else (left.id() if (decision == leftT) \
						else (up.id() if (decision == upT) \
							else down.id())))
				nextCell = s2sphere.CellId(right.id() if (cell.id() == right.id()) \
					else (left.id() if (cell.id() == left.id()) \
						else (up.id() if (cell.id() == up.id()) \
							else (down.id() if (cell.id() == down.id()) \
								else nextCell.id()))))
				decisionCode = (['L','R'] if nextCell.id() == left.id() \
					else (['R','L'] if nextCell.id() == right.id() \
						else (['U','D'] if nextCell.id() == up.id() \
							else ['D','U'])))


				face,i,j = lastCell.face(), s2sphere.CellId.st_to_ij((decision * (s4 - s3)) + s3), s2sphere.CellId.st_to_ij((decision * (t4 - t3)) + t3)
				fakeS,fakeT = get_st_for_face(nextCell.face(), get_xyz(s2sphere.CellId.from_face_ij(face, i, j)))
				fakePoint = s2sphere.CellId.from_face_ij(nextCell.face(), s2sphere.CellId.st_to_ij(fakeS), s2sphere.CellId.st_to_ij(fakeT))

				if decision == rightT:
					print "Right!\t",
				elif decision == leftT:
					print "Left!\t",
				elif decision == upT:
					print "Up!\t",
				elif decision == downT:
					print "Down!\t",
				print lastCell.id(), nextCell.id()			
				
				"""
				print leftT, rightT, upT, downT
				print "Box: ", min(cs1, cs2), min(ct1, ct2), max(cs1, cs2), max(ct1, ct2)
				print "Current: ", s3, t3
				print "Target: ", s4, t4
				print "Fake Point: ", fakePoint.to_lat_lng()
				print
				"""

				result[lastCell.id()]['geometry']['coordinates'].append([fakePoint.to_lat_lng().lng().degrees, fakePoint.to_lat_lng().lat().degrees, 'EL' + decisionCode[0]])
				if not nextCell.id() in result:
					result[nextCell.id()] = deepcopy(lineStringJson)
				result[nextCell.id()]['geometry']['coordinates'].append([fakePoint.to_lat_lng().lng().degrees, fakePoint.to_lat_lng().lat().degrees, 'EE' + decisionCode[1]])

				# Wrap the Corner (if necessary)
				if len(result[nextCell.id()]['geometry']['coordinates']) > 1:
					newPoints = wrapCorner(nextCell.id(), clockwise, result[nextCell.id()]['geometry']['coordinates'][-2][2], result[nextCell.id()]['geometry']['coordinates'][-1][2])
					if newPoints != None:
						result[nextCell.id()]['geometry']['coordinates'][-1:-1] = newPoints

				lastCell, lastPoint = nextCell, fakePoint

			else:
				result[cell.id()]['geometry']['coordinates'].append([point.to_lat_lng().lng().degrees, point.to_lat_lng().lat().degrees])
				lastPoint = point
				success = True

	return result

# Untested
def sliceMultiLineString(multiLineStringJson, level):
	multiLineStringJson = deepcopy(multiLineStringJson)
	lines = multiLineStringJson['geometry']['coordinates']
	multiLineStringJson['geometry']['coordinates'] = []
	lineStringJson = deepcopy(multiLineStringJson)
	lineStringJson['geometry']['type'] = 'LineString'

	result = {}
	for line in lines:
		lineStringJson['geometry']['coordinates'] = line
		lineLocations = sliceLineString(lineStringJson, level)

		for location in lineLocations.keys():
			if not location in result:
				result[location] = deepcopy(multiLineStringJson)
			result[location]['geometry']['coordinates'].append(lineLocations[location]['geometry']['coordinates'])

	for location in result:
		if len(result[location]) == 1:
			result[location]['geometry']['type'] = 'LineString'
			result[location]['geometry']['coordinates'] = result[location]['geometry']['coordinates'][0]
	return result

# Untested
def slicePolygon(polygonJson, level):
	polygonJson = deepcopy(polygonJson)
	lines = polygonJson['geometry']['coordinates']
	polygonJson['geometry']['coordinates'] = [[]]
	lineStringJson = deepcopy(polygonJson)
	lineStringJson['geometry']['type'] = 'LineString'

	if len(lines) > 1:
		raise NotImplementedError("Polygons with holes are not yet implemented.")

	result = {}
	for line in lines:
		clockwise = isClockwise(line)
		lineStringJson['geometry']['coordinates'] = line
		lineLocations = sliceLineString(lineStringJson, level, clockwise)

		for location in lineLocations.keys():
			if not location in result.keys():
				result[location] = deepcopy(polygonJson)
			result[location]['geometry']['coordinates'][0].extend(lineLocations[location]['geometry']['coordinates'])

	for location in result:
		"""
		for pointNum,point in enumerate(result[location]['geometry']['coordinates'][0]):
			if len(point) == 3:

				# Entering Polygon, Round the Corner
				if point[2][1] == 'E' and pointNum != 0:
					print result[location]['geometry']['coordinates'][0][pointNum-1][2], result[location]['geometry']['coordinates'][0][pointNum][2]
					newPoints = wrapCorner(location, clockwise, result[location]['geometry']['coordinates'][0][pointNum-1][2], result[location]['geometry']['coordinates'][0][pointNum][2])
					if newPoints != None:
						result[location]['geometry']['coordinates'][0][pointNum:pointNum] = newPoints

				# Leaving the Polygon, Nothing to See Here
				elif point[2][1] == 'L':
					pass
		"""

		# Check if result[location] is not closed
		if result[location]['geometry']['coordinates'][0][0] != result[location]['geometry']['coordinates'][0][-1]:
			newPoints = wrapCorner(location, clockwise, result[location]['geometry']['coordinates'][0][-1][2], result[location]['geometry']['coordinates'][0][0][2])
			if newPoints != None:
				result[location]['geometry']['coordinates'][0].extend(newPoints)
			result[location]['geometry']['coordinates'][0].append(result[location]['geometry']['coordinates'][0][0])		

	# Search for enclosed cells not listed here that need to be added
	"""
	searched = []
	frontier = []
	edgeCells = []
	for location in result:
		edgeCells.append(location)

	for cell in edgeCells:
		down, right, up, left = s2sphere.CellId(cell).get_edge_neighbors()
		downB, rightB, upB, leftB = down.id() in edgeCells, rightB.id() in edgeCells, upB.id() in edgeCells, leftB.id() in edgeCells

	
	while len(frontier) != 0:
		cell = frontier.pop(0)
		down, right, up, left = s2sphere.CellId(cell).get_edge_neighbors()

		if down.id() not in frontier and down.id() not in searched and down.id() not in edgeCells:
			frontier.append(down.id())
		if left.id() not in frontier and left.id() not in searched and left.id() not in edgeCells:
			frontier.append(left.id())
		if up.id() not in frontier and up.id() not in searched and up.id() not in edgeCells:
			frontier.append(up.id())
		if right.id() not in frontier and right.id() not in searched and right.id() not in edgeCells:
			frontier.append(right.id())
	"""
	
	return result

# Untested
def sliceMultiPolygon(multiPolygonJson, level):
	multiPolygonJson = deepcopy(multiPolygonJson)
	polygons = multiPolygonJson['geometry']['coordinates']
	multiPolygonJson['geometry']['coordinates'] = []
	polygonJson = deepcopy(multiPolygonJson)
	polygonJson['geometry']['type'] = 'Polygon'

	result = {}
	for polygon in polygons:
		polygonJson['geometry']['coordinates'] = polygon
		polygonLocations = slicePolygon(polygonJson, level)

		for location in polygonLocations.keys():
			if not location in result:
				result[location] = deepcopy(multiPolygonJson)
			result[location]['geometry']['coordinates'].append(polygonLocations[location]['geometry']['coordinates'])

	for location in result:
		if len(result[location]) == 1:
			result[location]['geometry']['type'] = 'Polygon'
			result[location]['geometry']['coordinates'] = result[location]['geometry']['coordinates'][0]
	return result

# Untested
def wrapCorner(s2_id, clockwise, start_code, end_code):
	# Check if Work has to be Done
	startSide = start_code[2].upper()
	endSide = end_code[2].upper()
	if startSide == endSide:
		return None

	# Define Variables
	sides = ['U', 'R', 'D', 'L']
	delta = 1 if clockwise else -1

	# Loop Until Connected
	start = sides.index(startSide)
	end = sides.index(endSide)
	result = []
	while start != end:

		curEdge = sides[start]
		nextEdge = sides[(start + delta) % len(sides)]

		latlng = getCornerLatLong(s2sphere.CellId(s2_id), curEdge, nextEdge)
		result.append([latlng.lng().degrees, latlng.lat().degrees, 'C'+curEdge+nextEdge])

		start = (start + delta) % len(sides)
	return result

# Untested
def getCornerLatLong(cell, startSide, endSide):
	if startSide in ['U', 'R'] and endSide in ['U', 'R']:
		return s2sphere.CellId.from_point(s2sphere.Cell(cell).get_vertex(2)).to_lat_lng()
	elif startSide in ['R', 'D'] and endSide in ['R', 'D']:
		return s2sphere.CellId.from_point(s2sphere.Cell(cell).get_vertex(1)).to_lat_lng()
	elif startSide in ['D', 'L'] and endSide in ['D', 'L']:
		return s2sphere.CellId.from_point(s2sphere.Cell(cell).get_vertex(0)).to_lat_lng()
	elif startSide in ['L', 'U'] and endSide in ['L', 'U']:
		return s2sphere.CellId.from_point(s2sphere.Cell(cell).get_vertex(3)).to_lat_lng()

def get_st(cellId):
	# Compute s,t
	u,v = cellId.get_center_uv()
	s,t = s2sphere.CellId.uv_to_st(u), s2sphere.CellId.uv_to_st(v)

	return s,t

def get_xyz(cellId):
	# Compute face,s,t
	face = cellId.face()
	s,t = get_st(cellId)

	# Corner Point (0,0,0) @ (-45\deg, -45\deg)
	x,y,z = 0,0,0
	if face == 0:				# Front Face
		x,y,z = s,t,0
	elif face == 1:				# Right Face
		x,y,z = 1,t,-s
	elif face == 2:				# Top Face
		x,y,z = s,1,t-1
	elif face == 3:				# Back Face
		x,y,z = 1-t,s,-1
	elif face == 4:				# Left Face
		x,y,z = 0,1-s,t-1
	elif face == 5:				# Bottom Face
		x,y,z = s,0,t-1

	return x,y,z

def get_st_for_face(face, xyz):
	x,y,z = xyz[0],xyz[1],xyz[2]
	if face == 0:
		return x,y
	elif face == 1:
		return -z,y
	elif face == 2:
		return x,1+z
	elif face == 3:
		return y,1-x
	elif face == 4:
		return 1-y,1+z
	elif face == 5:
		return x,1+z

# Untested and Broken
def get_closest_xyz(srcCell, destCell):
	srcFace = srcCell.face()
	destFace = destCell.face()
	srcX, srcY, srcZ = get_xyz(srcCell)
	x,y,z = get_xyz(destCell)

	# Opposite Faces
	if (srcFace + 3) % 6 == destFace:
		# Magic Here
		
		# apply rotation formulas for each possible configuration
		# find shortest route and return x,y,z
		pass

	# Adjacent Faces
	else:
		rot = getFaceAxes(srcFace)
		offset = getFaceOffset(srcFace)
		score1 = score2 = score3 = float('inf')

		if toLeft[srcFace] == destFace:
			
			x,y,z = translate(-offset[0], -offset[1], -offset[2], x, y, z)
			x,y,z = rotate(rot[1][0], rot[1][1], rot[1][2], x, y, z)
			x,y,z = translate(offset[0], offset[1], offset[2], x, y, z)
			score1 = distance(srcX, srcY, srcZ, x, y, z)

			xa,ya,za = translate(-offset[0], -offset[1], -offset[2], x, y, z)
			xa,ya,za = rotate(rot[2][0], rot[2][1], rot[2][2], xa, ya, za)
			xa,ya,za = translate(offset[0], offset[1], offset[2], xa, ya, za)
			score2 = distance(srcX, srcY, srcZ, xa, ya, za)
			
			xb,yb,zb = translate(-offset[0], -offset[1], -offset[2], x, y, z)
			xb,yb,zb = rotate(-rot[2][0], -rot[2][1], -rot[2][2], xb, yb, zb)
			xb,yb,zb = translate(offset[0] + rot[1][0] - rot[0][0], offset[1] + rot[1][1] - rot[0][1], offset[2] + rot[1][2] - rot[0][2], xb, yb, zb)
			score3 = distance(srcX, srcY, srcZ, xb, yb, zb)

			#print x,y,z,xa,ya,za,xb,yb,zb
			if score2 < score1:
				x,y,z = xa,ya,za
				score1 = score2
			if score3 < score1:
				x,y,z = xb,yb,zb
				score1 = score3

		elif toRight[srcFace] == destFace:
			x,y,z = translate(-offset[0] - rot[0][0], -offset[1] - rot[0][1], -offset[2] - rot[0][2], x, y, z)
			x,y,z = rotate(-rot[1][0], -rot[1][1], -rot[1][2], x, y, z)
			x,y,z = translate(offset[0] + rot[0][0], offset[1] + rot[0][1], offset[2] + rot[0][2], x, y, z)
			score1 = distance(srcX, srcY, srcZ, x, y, z)

			xa,ya,za = translate(-offset[0] - rot[0][0], -offset[1] - rot[0][1], -offset[2] - rot[0][2], x, y, z)
			xa,ya,za = rotate(-rot[2][0], -rot[2][1], -rot[2][2], xa, ya, za)
			xa,ya,za = translate(offset[0] + rot[0][0], offset[1] + rot[0][1], offset[2] + rot[0][2], xa, ya, za)
			score2 = distance(srcX, srcY, srcZ, xa, ya, za)
			
			xb,yb,zb = translate(-offset[0] - rot[0][0], -offset[1] - rot[0][1], -offset[2] - rot[0][2], x, y, z)
			xb,yb,zb = rotate(rot[2][0], rot[2][1], rot[2][2], xb, yb, zb)
			xb,yb,zb = translate(offset[0] + rot[0][0] + rot[1][0], offset[1] + rot[0][1] + rot[1][1], offset[2] + rot[0][2] + rot[1][2], xb, yb, zb)
			score3 = distance(srcX, srcY, srcZ, xb, yb, zb)

			print x,y,z,xa,ya,za,xb,yb,zb
			if score2 < score1:
				x,y,z = xa,ya,za
				score1 = score2
			if score3 < score1:
				x,y,z = xb,yb,zb
				score1 = score3

		elif toTop[srcFace] == destFace:
			x,y,z = translate(-offset[0] - rot[1][0], -offset[1] - rot[1][1], -offset[2] - rot[1][2], x, y, z)
			x,y,z = rotate(rot[0][0], rot[0][1], rot[0][2], x, y, z)
			x,y,z = translate(offset[0] + rot[1][0], offset[1] + rot[1][1], offset[2] + rot[1][2], x, y, z)

		elif toBottom[srcFace] == destFace:
			x,y,z = translate(-offset[0], -offset[1], -offset[2], x, y, z)
			x,y,z = rotate(rot[0][0], rot[0][1], rot[0][2], x, y, z)
			x,y,z = translate(offset[0], offset[1], offset[2], x, y, z)

		# determine if left, up, right, down
		# apply translation formula (etc. for rotation about y axis, switch x & z)
		# apply rotation formulas for each possible configuration
		# find shortest route and return x,y,z
	return x,y,z

# Broken
def getFaceAxes(face):
	if face == 0:
		return [[1,0,0], [0,1,0], [0,0,1]]
	elif face == 1:
		return [[0,0,-1], [0,1,0], [1,0,0]]
	elif face == 2:
		return [[1,0,0], [0,0,-1], [0,1,0]]
	elif face == 3:
		return [[-1,0,0], [0,1,0], [0,0,-1]]
	elif face == 4:
		return [[0,0,1], [0,1,0], [-1,0,0]]
	elif face == 5:
		return [[1,0,0], [0,0,1], [0,-1,0]]

# Broken
def getFaceOffset(face):
	if face == 0:
		return [0,0,0]
	elif face == 1:
		return [1,0,0]
	elif face == 2:
		return [0,1,0]
	elif face == 3:
		return [0,0,-1]
	elif face == 4:
		return [0,0,0]
	elif face == 5:
		return [0,0,-1]

# Broken
def rotate(axisX, axisY, axisZ, x, y, z):
	if abs(axisX) == 1:
		return rotateX(axisX > 0, x, y, z)
	elif abs(axisY) == 1:
		return rotateY(axisY > 0, x, y, z)
	elif abs(axisZ):
		return rotateZ(axisZ > 0, x, y, z)

# Broken
def rotateX(positive, x, y, z):
	if positive:
		return x, -z, y
	else:
		return x, z, -y

# Broken
def rotateY(positive, x, y, z):
	if positive:
		return z, y, -x
	else:
		return -z, y, x

# Broken
def rotateZ(positive, x, y, z):
	if positive:
		return y, -x, z
	else:
		return -y, x, z

# Broken
def translate(tx, ty, tz, x, y, z):
	return x+tx, y+ty, z+tz

# Broken
def distance(x1, y1, z1, x2, y2, z2):
	return math.sqrt((x2-x1)**2 + (y2-y1)**2 + (z2-z1)**2)

def isClockwise(line):
	# Determine Wrapping Direction (Delta-Theta Method)
	theta, lastPoint = 0, None
	lastAngle = atan2((line[-1][1] - line[-2][1]),(line[-1][0] - line[-2][0]))
	for point in line:
		if lastPoint != None:
			angle = atan2((point[1] - lastPoint[1]), (point[0] - lastPoint[0]))

			delta = angle - lastAngle
			if angle < lastAngle - math.pi:
				delta += 2 * math.pi
			if angle > lastAngle + math.pi:
				delta -= 2 * math.pi
			
			#print "Last Angle: ",lastAngle,"\tNew Angle: ",angle,"\tDelta: ",delta
			theta += delta
			lastAngle = angle
		lastPoint = point
	return theta < 0

def atan2(y,x):
	return math.atan2(y,x) if x != 0 else (math.pi/2 if (y > 0) else -math.pi/2)
