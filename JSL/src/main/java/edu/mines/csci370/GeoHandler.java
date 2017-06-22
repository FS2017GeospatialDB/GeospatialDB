package edu.mines.csci370;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.geometry.*;
import edu.mines.csci370.api.Feature;
import edu.mines.csci370.api.GeolocationService;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.*;

public class GeoHandler implements GeolocationService.Iface {

    @Override
    public List<Feature> getFeatures(double lBox, double rBox, double bBox, double tBox, long timestampMillis) {

        // Build Rectangles
        long start = System.currentTimeMillis();
        S2LatLng bottomLeft = S2LatLng.fromDegrees(bBox, lBox);
        S2LatLng topRight = S2LatLng.fromDegrees(tBox, rBox);
        S2LatLngRect rect = new S2LatLngRect(bottomLeft, topRight);

        // Determine Necessary Level
        double area = rect.area();
        int level = 12;
        if (area > 1e-7) level = 8;
        if (area > 1e-6) level = 4;

        // Get Cells Covering Area
        ArrayList<S2CellId> cells = new ArrayList<>();
        S2RegionCoverer coverer = new S2RegionCoverer();
        coverer.setMinLevel(level);
        coverer.setMaxLevel(level);
        coverer.setMaxCells(Integer.MAX_VALUE);
        coverer.getCovering(rect, cells);

        // Lookup the Cells in the Database
        List<Feature> results = new ArrayList<>();
        Map<String /* OSM_ID */, Map<Long /* S2 Cell ID */, JSONObject /* GeoJSON */>> pieces = new HashMap<>();
        Map<String /* OSM_ID */, Map<Long /* S2 Cell ID */, Long /* TIMESTAMP */>> changeDate = new HashMap<>();
        Map<String /* OSM_ID */, Long /* TIMESTAMP */> timestamps = new HashMap<>();

        Session session = Database.getSession();
        PreparedStatement statement = Database.prepareFromCache(
                "SELECT unixTimestampOf(time) AS time_unix, json, osm_id FROM global.slave WHERE level=? AND s2_id=? AND time >= ?");

        // Execute the Query
        for (S2CellId cell : cells) {
            ResultSet rs = session.execute(statement.bind(level, cell.id(), UUIDs.startOf(timestampMillis)));

            while (!rs.isExhausted()) {
                // Parse the row from the Database
                Row row = rs.one();
                String json = row.getString("json");
                String osmId = row.getString("osm_id");
                Long timestamp = row.getLong("time_unix");

                try {
                    // Handle Initial Conditions
                    if (!pieces.containsKey(osmId))
                        pieces.put(osmId, new HashMap<Long, JSONObject>());
                    if (!changeDate.containsKey(osmId))
                        changeDate.put(osmId, new HashMap<Long, Long>());

                    // If this is the Oldest Revision, Take It
                    if (!changeDate.get(osmId).containsKey(cell.id())
                            || changeDate.get(osmId).get(cell.id()) > timestamp) {
                        pieces.get(osmId).put(cell.id(), (JSONObject) (new JSONParser()).parse(json));
                        timestamps.put(osmId, timestamp);
                        //results.add(new Feature(timestamp, json));
                    }

                } catch (ParseException e) {
                    throw new RuntimeException("JSON Parsing Exception Caught: " + e.getMessage());
                }
            }
        }

        // Recombine Features
        for (String osmId : pieces.keySet()) {
            Map<Long, JSONObject> list = pieces.get(osmId);

            JSONObject unified = reconstruct(list, lBox, rBox, bBox, tBox);
            if (unified != null) {
                results.add(new Feature(
                        timestamps.get(osmId),
                        unified.toString()
                ));
            }
        }

        long finish = System.currentTimeMillis();
        System.out.println(cells.size() + " queries @ scale=" + level + " in " + (finish - start) + "ms");
        return results;
    }

    @Override
    public List<Feature> getCell(double lat, double lng, long timestampMillis) {

        // Find cell
        long start = System.currentTimeMillis();
        S2LatLng loc = S2LatLng.fromDegrees(lat, lng);
        S2CellId cell = new S2Cell(loc).id().parent(16);
        System.out.println(cell.id());

        // Lookup the Cells in the Database
        List<Feature> results = new ArrayList<>();
        Session session = Database.getSession();
        PreparedStatement statement = Database.prepareFromCache(
                "SELECT unixTimestampOf(time) AS time_unix, json FROM global.slave WHERE level=? AND s2_id=? AND time >= ?");

        // Execute the Query
        ResultSet rs = session.execute(statement.bind(cell.level(), cell.id(), UUIDs.startOf(timestampMillis)));

        while (!rs.isExhausted()) {
            Row row = rs.one();
            Feature feature = new Feature(
                    row.getLong("time_unix"),
                    row.getString("json"));
            results.add(feature);
        }

        long finish = System.currentTimeMillis();
        System.out.println("1 query @ scale=" + cell.level() + " in " + (finish - start) + "ms");
        return results;
    }

    private JSONObject reconstruct(Map<Long, JSONObject> s2CellToJson, double lBox, double rBox, double bBox, double tBox) {
        //System.out.println("Reconstructing " + s2CellToJson);

        // Simple Case == Already Reconstructed
        if (s2CellToJson.size() == 1)
            for (Long key : s2CellToJson.keySet())
                return s2CellToJson.get(key);

        // Switch on Feature Type
        Long key = s2CellToJson.keySet().iterator().next();
        String type = (String) ((JSONObject) (s2CellToJson.get(key)).get("geometry")).get("type");

        if (type.equals("Point") || type.equals("MultiPoint"))
            return reconstructMultiPoint(s2CellToJson);
        else if (type.equals("LineString") || type.equals("MultiLineString"))
            return reconstructMultiLineString(s2CellToJson);
        else if (type.equals("Polygon") || type.equals("MultiPolygon"))
            return reconstructMultiPolygon(s2CellToJson, lBox, rBox, bBox, tBox);
        else return null;
    }

    private JSONObject reconstructMultiPoint(Map<Long, JSONObject> s2CellToJson) {
        // Iterate Cells and Combine Coordinates
        JSONArray coordinates = new JSONArray();
        for (Long cellID : s2CellToJson.keySet()) {
            String type = (String) ((JSONObject) (s2CellToJson.get(cellID)).get("geometry")).get("type");

            if (type.equals("Point"))
                coordinates.add(((JSONObject) (s2CellToJson.get(cellID)).get("geometry")).get("coordinates"));
            else
                coordinates.addAll((JSONArray) ((JSONObject) (s2CellToJson.get(cellID)).get("geometry")).get("coordinates"));
        }

        // Build a new Points JSON
        Long key = s2CellToJson.keySet().iterator().next();
        ((JSONObject) (s2CellToJson.get(key)).get("geometry")).put("coordinates", coordinates.size() > 1 ? coordinates : coordinates.get(0));
        ((JSONObject) (s2CellToJson.get(key)).get("geometry")).put("type", coordinates.size() > 1 ? "MultiPoint" : "Point");
        return s2CellToJson.get(key);
    }

    private JSONObject reconstructMultiLineString(Map<Long, JSONObject> s2CellToJson) {
        // Naive Approach -- Throw Away "Fake" Points
        List<JSONArray> results = new ArrayList<>();

        // Put all Line Segments into Map
        Map<Long, List<JSONArray>> lines = new HashMap<>();
        Map<Long, List<JSONArray>> processed = new HashMap<>();
        for (Long cellID : s2CellToJson.keySet()) {
            String type = (String) ((JSONObject) (s2CellToJson.get(cellID)).get("geometry")).get("type");

            // Add list of partial "lines" to the Map
            JSONArray array = new JSONArray(), tmp = new JSONArray();
            if (type.equals("LineString")) {
                array.add(((JSONObject) (s2CellToJson.get(cellID)).get("geometry")).get("coordinates"));
            } else {
                array.addAll((JSONArray) ((JSONObject) (s2CellToJson.get(cellID)).get("geometry")).get("coordinates"));
            }
            lines.put(cellID, array);
        }

        Long seed = null;
        JSONArray lineSegment = null, lineComplete = null;

        // Loop Until Done
        while (lines.size() > 0 || lineSegment != null) {

            // Choose a Seed to Start With
            if (lineSegment == null) {
                seed = lines.keySet().iterator().next();
                lineSegment = lines.get(seed).get(0);
                if (!processed.containsKey(seed)) processed.put(seed, new JSONArray());
                processed.get(seed).add(lineSegment);
                lines.get(seed).remove(0);
                if (lines.get(seed).size() == 0) lines.remove(seed);

                // Search for Existing lineComplete
                lineComplete = null;
                for (JSONArray tmpLineComplete : results) {
                    if ((double) ((JSONArray) tmpLineComplete.get(tmpLineComplete.size() - 1)).get(0) == (double) ((JSONArray) lineSegment.get(0)).get(0)
                            && (double) ((JSONArray) tmpLineComplete.get(tmpLineComplete.size() - 1)).get(1) == (double) ((JSONArray) lineSegment.get(0)).get(1)) {
                        //System.out.println("Appending to existing line segment.");
                        lineComplete = tmpLineComplete;
                    }
                }
                if (lineComplete == null) {
                    lineComplete = new JSONArray();
                    results.add(lineComplete);
                }
            }

            // Traverse Line Segment
            for (int i = 0; i < lineSegment.size(); i++) {
                lineComplete.add(lineSegment.get(i));

                // End of the Line
                if (i == lineSegment.size() - 1 && (((JSONArray) lineSegment.get(i)).size() != 3 || i == 0)) {
                    seed = null;
                    lineSegment = null;
                    break;
                }

                // Replace Scalia (leaving s2 cell...)
                if (i > 0 && ((JSONArray) lineSegment.get(i)).size() == 3) {
                    String code = (String) ((JSONArray) lineSegment.get(i)).get(2);
                    char direction = code.charAt(2);
                    assert (code.startsWith("EL"));

                    // Make Decision for Next Cell
                    S2CellId[] neighbors = new S2CellId[4];
                    (new S2CellId(seed)).getEdgeNeighbors(neighbors);
                    Long nextCell = (direction == 'D' ? neighbors[0].id() : (direction == 'R' ? neighbors[1].id() : (direction == 'U' ? neighbors[2].id() : neighbors[3].id())));

                    // Add Remainder of lineSegment as New Line Segment to be searched
                    if (i + 1 < lineSegment.size()) {
                        code = (String) ((JSONArray) lineSegment.get(i + 1)).get(2);
                        assert (code.startsWith("EE"));
                        if (!lines.containsKey(seed)) lines.put(seed, new JSONArray());
                        JSONArray split = new JSONArray();
                        while (i + 1 < lineSegment.size()) {
                            split.add(lineSegment.get(i + 1));
                            lineSegment.remove(i + 1);
                        }
                        lines.get(seed).add(split);
                    }

                    // Check if Continuation is Unsearched
                    if (lines.containsKey(nextCell)) {
                        for (JSONArray tmpSegment : lines.get(nextCell)) {
                            if ((double) ((JSONArray) tmpSegment.get(0)).get(0) == (double) ((JSONArray) lineSegment.get(i)).get(0)
                                    && (double) ((JSONArray) tmpSegment.get(0)).get(1) == (double) ((JSONArray) lineSegment.get(i)).get(1)) {
                                //System.out.println("Perfect Continuation: " + nextCell);
                                seed = nextCell;
                                lineSegment = tmpSegment;
                                lines.get(seed).remove(tmpSegment);
                                if (lines.get(seed).size() == 0) lines.remove(seed);
                                break;
                            }
                        }
                        if (seed == nextCell) break;
                    }

                    // Check if Continuation is Searched
                    if (processed.containsKey(nextCell)) {
                        for (JSONArray tmpSegment : processed.get(nextCell)) {
                            if ((double) ((JSONArray) tmpSegment.get(0)).get(0) == (double) ((JSONArray) lineSegment.get(i)).get(0)
                                    && (double) ((JSONArray) tmpSegment.get(0)).get(1) == (double) ((JSONArray) lineSegment.get(i)).get(1)) {
                                //System.out.println("Continuation Found, But Processed: " + nextCell);

                                // Search for Existing lineComplete
                                for (JSONArray tmpLineComplete : results) {
                                    if ((double) ((JSONArray) tmpLineComplete.get(0)).get(0) == (double) ((JSONArray) lineSegment.get(i)).get(0)
                                            && (double) ((JSONArray) tmpLineComplete.get(0)).get(1) == (double) ((JSONArray) lineSegment.get(i)).get(1)) {
                                        //System.out.println("Prepending to Existing Segment: " + nextCell);
                                        results.remove(lineComplete);
                                        tmpLineComplete.addAll(0, lineSegment);
                                        seed = null;
                                        lineSegment = null;
                                        break;
                                    }
                                }
                                if (seed == null) break;
                            }
                        }
                    }

                    // We Fell off the Map (or already merged)
                    seed = null;
                    lineSegment = null;
                    break;
                }
            }
        }

        // Trim "Fake" News
        for (JSONArray lineCompleted : results) {
            for (int i = 0; i < lineCompleted.size(); i++) {                            // Used to be [1, size() - 1), but it doesn't make sense to perform any more reconstruction
                JSONArray point = (JSONArray) lineCompleted.get(i);
                while (point.size() > 2) point.remove(point.size() - 1);
            }
        }

        // Return the Results
        Long finishedSeed = s2CellToJson.keySet().iterator().next();
        JSONArray coordinates = new JSONArray();

        for (ArrayList finishedLineSegment : results)
            coordinates.add(finishedLineSegment);
        ((JSONObject) (s2CellToJson.get(finishedSeed)).get("geometry")).put("type", "MultiLineString");
        ((JSONObject) (s2CellToJson.get(finishedSeed)).get("geometry")).put("coordinates", coordinates);
        //System.out.println(((HashMap.Entry)((HashMap)s2CellToJson).entrySet().toArray()[0]).getValue());
        return s2CellToJson.get(finishedSeed);
    }

    private JSONObject reconstructMultiPolygon(Map<Long, JSONObject> s2CellToJson, double lBox, double rBox, double bBox, double tBox) {
        // Downgrade MultiPolygon->MultiLineString and Polygon->LineString
        for (Long key : s2CellToJson.keySet()) {
            String type = (String) ((JSONObject) ((JSONObject) s2CellToJson.get(key)).get("geometry")).get("type");
            JSONArray coordinates = (JSONArray) ((JSONObject) ((JSONObject) s2CellToJson.get(key)).get("geometry")).get("coordinates");

            if (type.equals("Polygon")) {
                // Polygons w/ Holes are not Supported!!!
                assert (coordinates.size() == 1);

                // Unwind the Polygon
                ((JSONArray) coordinates.get(0)).remove(((JSONArray) coordinates.get(0)).size() - 1);

                // Remove Corners
                for (int i = 0; i < ((JSONArray) coordinates.get(0)).size(); i++) {
                    JSONArray point = (JSONArray) ((JSONArray) coordinates.get(0)).get(i);
                    if (point.size() == 3 && ((String) point.get(2)).startsWith("C")) {
                        ((JSONArray) coordinates.get(0)).remove(i);
                        i--;
                    }
                }
                if (((JSONArray) coordinates.get(0)).size() == 0) continue;

                // Convert to LineString
                ((JSONObject) ((JSONObject) s2CellToJson.get(key)).get("geometry")).put("coordinates", coordinates.get(0));
                ((JSONObject) ((JSONObject) s2CellToJson.get(key)).get("geometry")).put("type", "LineString");
            } else {
                for (int i = 0; i < coordinates.size(); i++) {
                    // Polygons w/ Holes are not Supported!!
                    assert (((JSONArray) coordinates.get(i)).size() == 1);

                    // Unwind the Polygon
                    ((JSONArray) ((JSONArray) coordinates.get(i)).get(0)).remove(((JSONArray) ((JSONArray) coordinates.get(i)).get(0)).size() - 1);

                    // Remove Corners
                    for (int j = 0; j < ((JSONArray) ((JSONArray) coordinates.get(i)).get(0)).size(); j++) {
                        JSONArray point = (JSONArray) ((JSONArray) ((JSONArray) coordinates.get(i)).get(0)).get(j);
                        if (point.size() == 3 && ((String) point.get(2)).startsWith("C")) {
                            ((JSONArray) ((JSONArray) coordinates.get(i)).get(0)).remove(j);
                            j--;
                        }
                    }

                    // Convert to MultiLineString
                    coordinates.set(i, ((JSONArray) coordinates.get(i)).get(0));
                }
                ((JSONObject) ((JSONObject) s2CellToJson.get(key)).get("geometry")).put("type", "MultiLineString");
            }
        }

        // Perform Reconstruction
        JSONObject multiLine = reconstructMultiLineString(s2CellToJson);

        // Close Polygon w/ Corner Wrapping
        JSONArray lines = (JSONArray) ((JSONObject) multiLine.get("geometry")).get("coordinates");
        for (int i = 0; i < lines.size(); i++) {
            JSONArray firstPoint = (JSONArray) ((JSONArray) lines.get(i)).get(0);
            JSONArray lastPoint = (JSONArray) ((JSONArray) lines.get(i)).get(((JSONArray) lines.get(i)).size() - 1);

            if (firstPoint.size() == 3 && lastPoint.size() == 3) {
                JSONArray newPoints = wrapCorner(true, ((String) firstPoint.get(2)).charAt(2), ((String) lastPoint.get(2)).charAt(2), lBox, rBox, bBox, tBox);
                if (newPoints != null) ((JSONArray) lines.get(i)).addAll(newPoints);
            }

            if (firstPoint.get(0) != lastPoint.get(0) || firstPoint.get(1) != lastPoint.get(1))
                ((JSONArray) lines.get(i)).add(firstPoint);
        }

        // Return the Results
        ((JSONObject) multiLine.get("geometry")).put("type", "Polygon");
        //System.out.println("=> " + multiLine.toString());
        return multiLine;
    }

    private JSONArray wrapCorner(boolean clockwise, char startCode, char endCode, double lBox, double rBox, double bBox, double tBox) {
        // Check if Work Has to be Done
        startCode = Character.toUpperCase(startCode);
        endCode = Character.toUpperCase(endCode);
        if (startCode == endCode) return null;

        // Define Variables
        List<Character> sides = new ArrayList<Character>(Arrays.asList('U', 'R', 'D', 'L'));
        int delta = clockwise ? 1 : -1;
        S2LatLng latlng = S2LatLng.fromDegrees((tBox + bBox) / 2.0, (lBox + rBox) / 2.0);
        int face = S2CellId.fromLatLng(latlng).face();

        // Loop Until Connected
        int start = sides.indexOf(startCode);
        int end = sides.indexOf(endCode);
        JSONArray result = new JSONArray();
        while (start != end) {
            char currentEdge = sides.get(start);
            char nextEdge = sides.get((start + delta) % 4);

            // TODO: Add Rotations According to S-T Axes
            if (face <= 2) latlng = getCornerLatLong(currentEdge, nextEdge, lBox, rBox, bBox, tBox);
            else
                latlng = getCornerLatLong(sides.get((start + 1) % 4), sides.get((start + delta + 1) % 4), lBox, rBox, bBox, tBox);

            JSONArray corner = new JSONArray();
            corner.add(latlng.lngDegrees());
            corner.add(latlng.latDegrees());
            corner.add('C' + currentEdge + nextEdge);
            result.add(corner);

            start = (start + delta) % 4;
        }

        // Return results
        return result;
    }

    private S2LatLng getCornerLatLong(char startSide, char endSide, double lBox, double rBox, double bBox, double tBox) {
        if ((startSide == 'U' || startSide == 'R') && (endSide == 'U' || endSide == 'R'))
            return S2LatLng.fromDegrees(tBox, rBox);
        else if ((startSide == 'R' || startSide == 'D') && (endSide == 'R' || endSide == 'D'))
            return S2LatLng.fromDegrees(bBox, rBox);
        else if ((startSide == 'D' || startSide == 'L') && (endSide == 'D' || endSide == 'L'))
            return S2LatLng.fromDegrees(bBox, lBox);
        else if ((startSide == 'L' || startSide == 'U') && (endSide == 'L' || endSide == 'U'))
            return S2LatLng.fromDegrees(tBox, lBox);
        return null;
    }
}

/**
 * Face:  0  1  2  3  4  5
 * UR: UR UR ?? BR BR ??
 * RD: RD RD ?? BL BL ??
 * DL: DL DL ?? TL TL ??
 * LU: LU LU ?? UR UR ??
 */