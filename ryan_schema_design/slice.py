'''Serious module to cut the feature'''

import sys
import math
import numpy
import geojson
import s2sphere
import traceback
from s2 import *

from copy import deepcopy

def enclosed_range(feature):
    # Method Variables
    type = feature['geometry']['type']
    minLat = minLong = sys.maxsize
    maxLat = maxLong = -sys.maxsize

    # Switch on type
    if type == 'Point':
        point = feature['geometry']['coordinates']
        minLat, maxLat, minLong, maxLong = point[1], point[1], point[0], point[0]
    elif type == 'LineString':
        minLat, maxLat, minLong, maxLong = get_latlong_range(feature['geometry']['coordinates'])
    elif type == 'MultiLineString' or type == 'Polygon':
        for line in feature['geometry']['coordinates']:
            a, b, c, d = get_latlong_range(line)
            minLat = min(minLat, a)
            maxLat = max(maxLat, b)
            minLong = min(minLong, c)
            maxLong = max(maxLong, d)
    elif type == 'MultiPolygon':
        for polygon in feature['geometry']['coordinates']:
            for line in polygon:
                a, b, c, d = get_latlong_range(line)
                minLat = min(minLat, a)
                maxLat = max(maxLat, b)
                minLong = min(minLong, c)
                maxLong = max(maxLong, d)

    # Compute Maximum Coordinate Range
    return max(maxLat - minLat, maxLong - minLong)

def get_latlong_range(coordinates):
    minLat = minLong = sys.maxsize
    maxLat = maxLong = -sys.maxsize

    # Loop through coordinates
    for coord in coordinates:
        if coord[0] > maxLong:
            maxLong = coord[0]
        if coord[0] < minLong:
            minLong = coord[0]
        if coord[1] > maxLat:
            maxLat = coord[1]
        if coord[1] < minLat:
            minLat = coord[1]

    # Yes, this screws up features that cross,
    # the antimeridian; but for our purposes we'll
    # just assume that these features are important
    # and are large in size.
    return minLat, maxLat, minLong, maxLong

def slice_feature(json, level):
    '''Given the raw_feature and the desired level, cutting the feature.
    Note: This function is not fully implemented
    The result will be {CellId (long): json}, where cellid is the region
    that feature belongs to, json is the cutted geojson feature'''
    feature_set = dict()
    geo_type = json['geometry']['type']
    try:
        if geo_type == 'Point':
            feature_set = slicePoint(json, level)
        elif geo_type == 'MultiPoint':
            feature_set = sliceMultiPoint(json, level)
        elif geo_type == 'LineString':
            feature_set = sliceLineString(json, level)
        elif geo_type == 'MultiLineString':
            feature_set = sliceMultiLineString(json, level)
        elif geo_type == 'Polygon':
            feature_set = slicePolygon(json, level)
        elif geo_type == 'MultiPolygon':
            feature_set = sliceMultiPolygon(json, level)
    except IndexError, e:
        print ">>> IndexError %s" % e
        print traceback.format_exc()
    except NotImplementedError, e:
        print ">>> NotImplementedError: %s" % e
    return feature_set

def slicePoint(pointJson, level):
    pointJson = deepcopy(pointJson)
    coord = pointJson['geometry']['coordinates']
    latlng = S2LatLng.FromDegrees(coord[1], coord[0])
    cellID = S2CellId.FromLatLng(latlng).parent(level).id()
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
        latlng = S2LatLng.FromDegrees(point[1], point[0])
        cell = S2CellId.FromLatLng(latlng).parent(level)
        point = S2CellId.FromLatLng(latlng)

        success = False
        while not success:
            if lastCell is None:
                lastCell = cell
                lastPoint = point
                result[cell.id()] = deepcopy(lineStringJson)

            if cell.id() != lastCell.id():
                s1 = t1 = s2 = t2 = s3 = t3 = s4 = t4 = 0

                # Handle Different Sides
                if lastCell.face() != cell.face():
                    s3, t3 = get_st(lastPoint)
                    s4, t4 = get_st_for_face(lastPoint.face(), get_closest_xyz(lastPoint, point))

                # Handle Same Side
                else:
                    s3, t3 = get_st(lastPoint)
                    s4, t4 = get_st(point)

                cs1, ct1 = get_st_for_face(lastCell.face(), get_xyz(S2CellId.FromPoint(S2Cell(lastCell).GetVertex(0))))
                cs2, ct2 = get_st_for_face(lastCell.face(), get_xyz(S2CellId.FromPoint(S2Cell(lastCell).GetVertex(2))))
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
                down, right, up, left = lastCell.GetEdgeNeighbors()
                nextCell = S2CellId(right.id() if (decision == rightT) \
                    else (left.id() if (decision == leftT) \
                        else (up.id() if (decision == upT) \
                            else down.id())))
                nextCell = S2CellId(right.id() if (cell.id() == right.id()) \
                    else (left.id() if (cell.id() == left.id()) \
                        else (up.id() if (cell.id() == up.id()) \
                            else (down.id() if (cell.id() == down.id()) \
                                else nextCell.id()))))
                decisionCode = (['L', 'R'] if nextCell.id() == left.id() \
                    else (['R', 'L'] if nextCell.id() == right.id() \
                        else (['U', 'D'] if nextCell.id() == up.id() \
                            else ['D', 'U'])))

                face, i, j = lastCell.face(), S2CellId.STtoIJ((decision * (s4 - s3)) + s3), S2CellId.STtoIJ((decision * (t4 - t3)) + t3)
                fakeS, fakeT = get_st_for_face(nextCell.face(), get_xyz(S2CellId.FromFaceIJ(face, i, j)))
                fakePoint = S2CellId.FromFaceIJ(nextCell.face(), S2CellId.STtoIJ(fakeS), S2CellId.STtoIJ(fakeT))

                result[lastCell.id()]['geometry']['coordinates'].append([fakePoint.ToLatLng().lng().degrees(), fakePoint.ToLatLng().lat().degrees(), ('EL' if clockwise else 'EE') + decisionCode[0]])
                if not nextCell.id() in result:
                    result[nextCell.id()] = deepcopy(lineStringJson)
                result[nextCell.id()]['geometry']['coordinates'].append([fakePoint.ToLatLng().lng().degrees(), fakePoint.ToLatLng().lat().degrees(), ('EE' if clockwise else 'EL') + decisionCode[1]])

                # Wrap the Corner (if necessary)
                if len(result[nextCell.id()]['geometry']['coordinates']) > 1:
                    newPoints = wrapCorner(nextCell.id(), clockwise, result[nextCell.id()]['geometry']['coordinates'][-2][2], result[nextCell.id()]['geometry']['coordinates'][-1][2])
                    if newPoints != None:
                        result[nextCell.id()]['geometry']['coordinates'][-1:-1] = newPoints

                lastCell, lastPoint = nextCell, fakePoint

            else:
                result[cell.id()]['geometry']['coordinates'].append([point.ToLatLng().lng().degrees(), point.ToLatLng().lat().degrees()])
                lastPoint = point
                success = True

    # Handle Flipped Lines
    for location in result:
        if not clockwise:
            result[location]['geometry']['coordinates'] = result[location]['geometry']['coordinates'][::-1]

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
def slicePolygon(polygonJsonParam, level):
    polygonJson = deepcopy(polygonJsonParam)
    lines = polygonJson['geometry']['coordinates']
    polygonJson['geometry']['coordinates'] = []
    lineStringJson = deepcopy(polygonJson)
    lineStringJson['geometry']['type'] = 'LineString'

    result = {}
    for line in lines:
        clockwise = isClockwise(line)
        lineStringJson['geometry']['coordinates'] = line
        lineLocations = sliceLineString(lineStringJson, level, clockwise)

        for location in lineLocations.keys():
            if not location in result.keys():
                result[location] = deepcopy(polygonJson)
            result[location]['geometry']['coordinates'].append(lineLocations[location]['geometry']['coordinates'])

    for location in result:
        for lineNum, line in enumerate(result[location]['geometry']['coordinates']):
            for pointNum, point in enumerate(line):
                if len(point) == 3:
                    # Entering Polygon, Round the Corner
                    if point[2][1] == 'E' and pointNum != 0:
                        newPoints = wrapCorner(location, True, line[pointNum-1][2], line[pointNum][2])
                        if newPoints != None:
                            line[pointNum:pointNum] = newPoints

                    # Leaving the Polygon, Nothing to See Here
                    elif point[2][1] == 'L':
                        pass

            # Check if result[location] is not closed
            if line[0] != line[-1]:
                newPoints = wrapCorner(location, True, line[-1][2], line[0][2])
                if newPoints != None:
                    line.extend(newPoints)
                line.append(line[0])

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

        latlng = getCornerLatLong(S2CellId(s2_id), curEdge, nextEdge)
        result.append([latlng.lng().degrees(), latlng.lat().degrees(), 'C'+curEdge+nextEdge])

        start = (start + delta) % len(sides)
    return result

# Untested
def getCornerLatLong(cell, startSide, endSide):
    if startSide in ['U', 'R'] and endSide in ['U', 'R']:
        return S2CellId.FromPoint(S2Cell(cell).GetVertex(2)).ToLatLng()
    elif startSide in ['R', 'D'] and endSide in ['R', 'D']:
        return S2CellId.FromPoint(S2Cell(cell).GetVertex(1)).ToLatLng()
    elif startSide in ['D', 'L'] and endSide in ['D', 'L']:
        return S2CellId.FromPoint(S2Cell(cell).GetVertex(0)).ToLatLng()
    elif startSide in ['L', 'U'] and endSide in ['L', 'U']:
        return S2CellId.FromPoint(S2Cell(cell).GetVertex(3)).ToLatLng()

def get_st(cellId):
    # Compute s,t
    uv = cellId.GetCenterUV()
    u, v = uv.x(), uv.y()
    s, t = s2sphere.CellId.uv_to_st(u), s2sphere.CellId.uv_to_st(v)
    return s, t

def get_xyz(cellId):
    # Compute face,s,t
    face = cellId.face()
    s, t = get_st(cellId)

    # Corner Point (0,0,0) @ (-45\deg, -45\deg)
    x,y,z = 0,0,0
    if face == 0:				# Front Face
        x,y,z = s,t,0
    elif face == 1:				# Right Face
        x,y,z = 1,t,-s
    elif face == 2:				# Top Face
        x,y,z = 1-t,1,-s
    elif face == 3:				# Back Face
        x,y,z = 1-t,1-s,-1
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
        return -z,1-x
    elif face == 3:
        return 1-y,1-x
    elif face == 4:
        return 1-y,1+z
    elif face == 5:
        return x,1+z

toLeft = {0:4, 1:0, 2:4, 3:1, 4:3, 5:4}
toRight = {0:1, 1:3, 2:1, 3:4, 4:0, 5:1}
toTop = {0:2, 1:2, 2:3, 3:2, 4:2, 5:0}
toBottom = {0:5, 1:5, 2:0, 3:5, 4:5, 5:3}

# Untested
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
        raise NotImplementedError("Route finding for consecutive points on opposing faces is not implemented.")

    # Adjacent Faces
    else:
        rot = getFaceAxes(srcFace)
        offset = getFaceOffset(srcFace)
        score1 = score2 = score3 = float('inf')

        if toLeft[srcFace] == destFace:
            x,y,z = translate(-offset[0], -offset[1], -offset[2], x, y, z)
            x,y,z = rotate(rot[1][0], rot[1][1], rot[1][2], x, y, z)
            x,y,z = translate(offset[0], offset[1], offset[2], x, y, z)

        elif toRight[srcFace] == destFace:
            x,y,z = translate(-offset[0] - rot[0][0], -offset[1] - rot[0][1], -offset[2] - rot[0][2], x, y, z)
            x,y,z = rotate(-rot[1][0], -rot[1][1], -rot[1][2], x, y, z)
            x,y,z = translate(offset[0] + rot[0][0], offset[1] + rot[0][1], offset[2] + rot[0][2], x, y, z)

        elif toTop[srcFace] == destFace:
            x,y,z = translate(-offset[0] - rot[1][0], -offset[1] - rot[1][1], -offset[2] - rot[1][2], x, y, z)
            x,y,z = rotate(rot[0][0], rot[0][1], rot[0][2], x, y, z)
            x,y,z = translate(offset[0] + rot[1][0], offset[1] + rot[1][1], offset[2] + rot[1][2], x, y, z)

        elif toBottom[srcFace] == destFace:
            x,y,z = translate(-offset[0], -offset[1], -offset[2], x, y, z)
            x,y,z = rotate(-rot[0][0], -rot[0][1], -rot[0][2], x, y, z)
            x,y,z = translate(offset[0], offset[1], offset[2], x, y, z)

        # determine if left, up, right, down
        # apply translation formula (etc. for rotation about y axis, switch x & z)
        # apply rotation formulas for each possible configuration
        # find shortest route and return x,y,z
    return x,y,z

# Untested
def getFaceAxes(face):
    if face == 0:
        return [[1,0,0], [0,1,0], [0,0,1]]
    elif face == 1:
        return [[0,0,-1], [0,1,0], [-1,0,0]]
    elif face == 2:
        return [[1,0,0], [0,0,-1], [0,1,0]]
    elif face == 3:
        return [[-1,0,0], [0,1,0], [0,0,-1]]
    elif face == 4:
        return [[0,0,1], [0,1,0], [1,0,0]]
    elif face == 5:
        return [[1,0,0], [0,0,1], [0,-1,0]]

# Untested
def getFaceOffset(face):
    if face == 0:
        return [0,0,0]
    elif face == 1:
        return [1,0,0]
    elif face == 2:
        return [0,1,0]
    elif face == 3:
        return [1,0,-1]
    elif face == 4:
        return [0,0,-1]
    elif face == 5:
        return [0,0,-1]

# Untested
def rotate(axisX, axisY, axisZ, x, y, z):
    if abs(axisX) == 1:
        return rotateX(axisX > 0, x, y, z)
    elif abs(axisY) == 1:
        return rotateY(axisY > 0, x, y, z)
    elif abs(axisZ):
        return rotateZ(axisZ > 0, x, y, z)

# Untested
def rotateX(positive, x, y, z):
    if positive:
        return x, -z, y
    else:
        return x, z, -y

# Untested
def rotateY(positive, x, y, z):
    if positive:
        return z, y, -x
    else:
        return -z, y, x

# Untested
def rotateZ(positive, x, y, z):
    if positive:
        return -y, x, z
    else:
        return y, -x, z

# Untested
def translate(tx, ty, tz, x, y, z):
    return x+tx, y+ty, z+tz

# Untested
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
            
            theta += delta
            lastAngle = angle
        lastPoint = point
    return theta < 0

def atan2(y,x):
    return math.atan2(y,x) if x != 0 else (math.pi/2 if (y > 0) else -math.pi/2)
