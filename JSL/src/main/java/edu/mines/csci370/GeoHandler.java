package edu.mines.csci370;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.utils.UUIDs;

import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2Region;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2RegionCoverer;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import edu.mines.csci370.api.Feature;
import edu.mines.csci370.api.GeolocationService;

public class GeoHandler implements GeolocationService.Iface {

  @Override
  public List<Feature> getFeatures(double lBox, double rBox, double bBox, double tBox, long timestampMillis) {

    // Build Rectangles
    long start = System.currentTimeMillis();
    S2LatLng bottomLeft = S2LatLng.fromDegrees(bBox, lBox);
    S2LatLng topRight = S2LatLng.fromDegrees(tBox, rBox);
    S2LatLngRect rect = new S2LatLngRect(bottomLeft, topRight);

    // Determine Necessary Level
    double area = rect.area() * S2LatLng.EARTH_RADIUS_METERS * S2LatLng.EARTH_RADIUS_METERS;
    int level = 16;

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

           // results.add(new Feature(timestamp, json));
          }
          
        } catch (ParseException e) {
          throw new RuntimeException("JSON Parsing Exception Caught: " + e.getMessage());
        }
      }
    }

    // Recombine Features
    for (String osmId : pieces.keySet()) {
      Map<Long, JSONObject> list = pieces.get(osmId);

      JSONObject unified = reconstruct(list);
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

  private JSONObject reconstruct(Map<Long, JSONObject> s2CellToJson) {
    // Simple Case == Already Reconstructed
    if (s2CellToJson.size() == 1)
      for (Long key : s2CellToJson.keySet())
        return s2CellToJson.get(key);

    // Switch on Feature Type
    Long key = s2CellToJson.keySet().iterator().next();
    String type = (String) ((JSONObject) ((JSONObject) s2CellToJson.get(key)).get("geometry")).get("type");

    if (type.equals("Point") || type.equals("MultiPoint"))
      return reconstructMultiPoint(s2CellToJson);
    else if (type.equals("LineString") || type.equals("MultiLineString"))
      return reconstructMultiLineString(s2CellToJson);
    else if (type.equals("Polygon") || type.equals("MultiPolygon"))
      return reconstructMultiPolygon(s2CellToJson);
    else return null;
  }

  private JSONObject reconstructMultiPoint(Map<Long, JSONObject> s2CellToJson) {
    // Iterate Cells and Combine Coordinates
    JSONArray coordinates = new JSONArray();
    for (Long cellID : s2CellToJson.keySet()) {
      String type = (String) ((JSONObject) ((JSONObject) s2CellToJson.get(cellID)).get("geometry")).get("type");

      if (type.equals("Point"))
        coordinates.add((JSONArray) ((JSONObject) ((JSONObject) s2CellToJson.get(cellID)).get("geometry")).get("coordinates"));
      else
        coordinates.addAll((JSONArray) ((JSONObject) ((JSONObject) s2CellToJson.get(cellID)).get("geometry")).get("coordinates"));
    }

    // Build a new Points JSON
    Long key = s2CellToJson.keySet().iterator().next();
    ((JSONObject) ((JSONObject) s2CellToJson.get(key)).get("geometry")).put("coordinates", coordinates.size() > 1 ? coordinates : coordinates.get(0));
    ((JSONObject) ((JSONObject) s2CellToJson.get(key)).get("geometry")).put("type", coordinates.size() > 1 ? "MultiPoint" : "Point");
    System.out.println(s2CellToJson.size() + " down to 1");
    return s2CellToJson.get(key);
  }

  private JSONObject reconstructMultiLineString(Map<Long, JSONObject> s2CellToJson) {
    // Naive Approach -- Throw Away "Fake" Points
    List<JSONArray> results = new ArrayList<>();

    // Put all Line Segments into Map
    Map<Long, List<JSONArray>> lines = new HashMap<>();
    Map<Long, List<JSONArray>> processed = new HashMap<>();
    for (Long cellID : s2CellToJson.keySet()) {
      String type = (String) ((JSONObject) ((JSONObject) s2CellToJson.get(cellID)).get("geometry")).get("type");

      // Add list of partial "lines" to the Map
      JSONArray array = new JSONArray();
      if (type.equals("LineString"))
        array.add((JSONArray) ((JSONObject) ((JSONObject) s2CellToJson.get(cellID)).get("geometry")).get("coordinates"));
      else
        array.addAll((JSONArray) ((JSONObject) ((JSONObject) s2CellToJson.get(cellID)).get("geometry")).get("coordinates"));
      lines.put(cellID, array);
    }

    Long seed = null;
    JSONArray lineSegment = null, lineComplete = null;

    // Loop Until Done
    while (lines.size() > 0) {

      // Choose a Seed to Start With
      if (lineSegment == null) {
        seed = lines.keySet().iterator().next();
        lineSegment = lines.get(seed).get(0);
        if (!processed.containsKey(seed)) processed.put(seed, new JSONArray());
        processed.get(seed).add(lineSegment); lines.get(seed).remove(0);
        if (lines.get(seed).size() == 0) lines.remove(seed);
        lineComplete = new JSONArray(); results.add(lineComplete);
      }

      // Traverse Line Segment
      for (int i = 0; i < lineSegment.size(); i++) {
        lineComplete.add(lineSegment.get(i));

        // Replace Scalia
        if (i == lineSegment.size() - 1 && ((JSONArray) lineSegment.get(i)).size() == 3) {
          String code = (String) ((JSONArray) lineSegment.get(i)).get(2);
          char direction = code.charAt(2);
          assert(code.startsWith("EL"));

          // Make Decision for Next Cell
          S2CellId[] neighbors = new S2CellId[4];
          (new S2CellId(seed)).getEdgeNeighbors(neighbors);
          Long nextCell = (direction == 'D' ? neighbors[0].id() : (direction == 'R' ? neighbors[1].id() : (direction == 'U' ? neighbors[2].id() : neighbors[3].id())));

          // Check if Continuation is Unsearched
          if (lines.containsKey(nextCell)) {
            for (JSONArray tmpSegment : lines.get(nextCell)) {
              if ((double) ((JSONArray) tmpSegment.get(0)).get(0) == (double) ((JSONArray) lineSegment.get(i)).get(0)
                    && (double) ((JSONArray) tmpSegment.get(0)).get(1) == (double) ((JSONArray) lineSegment.get(i)).get(1))
                seed = nextCell; lineSegment = tmpSegment;
            }
            if (seed == nextCell) break;
          }

          // Check if Continuation is Searched
          if (processed.containsKey(nextCell)) {
            for (JSONArray tmpSegment : processed.get(nextCell)) {
              if ((double) ((JSONArray) tmpSegment.get(0)).get(0) == (double) ((JSONArray) lineSegment.get(i)).get(0)
                    && (double) ((JSONArray) tmpSegment.get(0)).get(1) == (double) ((JSONArray) lineSegment.get(i)).get(1)) {

                // Search for Existing lineComplete
                for (JSONArray tmpLineComplete : results) {
                  if ((double) ((JSONArray) tmpLineComplete.get(0)).get(0) == (double) ((JSONArray) lineSegment.get(i)).get(0)
                      && (double) ((JSONArray) tmpLineComplete.get(0)).get(1) == (double) ((JSONArray) lineSegment.get(i)).get(1)) {
                    tmpLineComplete.addAll(0, lineSegment);
                    seed = null; lineSegment = null;
                    break;
                  }
                }
                if (seed == null) break;
              }
            }
          }

          // We Fell off the Map (or already merged)
          seed = null; lineSegment = null;
        }
      }
    }

    // Trim "Fake" News
    for (JSONArray lineCompleted : results) {
      for (int i = 1; i < lineCompleted.size() - 1; i++) {
        JSONArray point = (JSONArray) lineCompleted.get(i);
        if (point.size() > 2)
          point = (JSONArray) point.subList(0, 2);
      }
    }

    // Return the Results
    Long finishedSeed = s2CellToJson.keySet().iterator().next();
    ArrayList coordinates = new ArrayList();
    for (ArrayList finishedLineSegment : results)
      coordinates.add(finishedLineSegment);
    if (coordinates.size() == 1) coordinates = (JSONArray) coordinates.get(0);
    ((JSONObject) ((JSONObject) s2CellToJson.get(finishedSeed)).get("geometry")).put("coordinates", coordinates);
    ((JSONObject) ((JSONObject) s2CellToJson.get(finishedSeed)).get("geometry")).put("type", coordinates.size() > 1 ? "MultiLineString" : "LineString");
    return s2CellToJson.get(finishedSeed);
  }

  private JSONObject reconstructMultiPolygon(Map<Long, JSONObject> s2CellToJson) {
    // Condense MultiPolygon->MultiLineString, Polygon->LineString
    // Call reconstructMutliLine()
    // Process output by closing each polygon and wrapping corners Clockwise
    // Return Polygon/MultiPolygon object

    return null;
  }
}