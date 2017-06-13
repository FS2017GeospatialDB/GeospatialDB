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
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import edu.mines.csci370.api.Feature;
import edu.mines.csci370.api.GeolocationService;

public class GeoHandler implements GeolocationService.Iface {

  private static final double SCALE_8 = 1945735774.12;
  private static final double SCALE_12 = 7600530.36765;

  @Override
  public List<Feature> getFeatures(double lBox, double rBox, double bBox, double tBox, long timestampMillis) {

    // Build Rectangles
    long start = System.currentTimeMillis();
    S2LatLng bottomLeft = S2LatLng.fromDegrees(bBox, lBox);
    S2LatLng topRight = S2LatLng.fromDegrees(tBox, rBox);
    S2LatLngRect rect = new S2LatLngRect(bottomLeft, topRight);

    // Determine Necessary Level
    double area = rect.area() * S2LatLng.EARTH_RADIUS_METERS * S2LatLng.EARTH_RADIUS_METERS;
    int level = 12;
    if (area / SCALE_12 > 4d) level = 8;
    if (area / SCALE_8 > 4d) level = 4;

    // Get Cells Covering Area
    ArrayList<S2CellId> cells = new ArrayList<>();
    S2RegionCoverer coverer = new S2RegionCoverer();
    coverer.setMinLevel(level);
    coverer.setMaxLevel(level);
    coverer.setMaxCells(Integer.MAX_VALUE);
    coverer.getCovering(rect, cells);

    // Lookup the Cells in the Database
    List<Feature> results = new ArrayList<>();
    Map<String, Map<Long, JSONObject>> pieces = new HashMap<>();
    Map<String, Map<Long, Long>> changeDate = new HashMap<>();
    Map<String, Long> timestamps = new HashMap<>();

    Session session = Database.getSession();
    PreparedStatement statement = Database.prepareFromCache(
      "SELECT unixTimestampOf(time) AS time_unix, json, osm_id FROM global.slave WHERE level=? AND s2_id=? AND time >= ?");

    // Execute the Query
    for (S2CellId cell : cells) {
      ResultSet rs = session.execute(statement.bind(level, cell.id(), UUIDs.startOf(timestampMillis)));

      while (!rs.isExhausted()) {
        Row row = rs.one();
        String json = row.getString("json");
        String osmId = row.getString("osm_id");
        Long timestamp = row.getLong("time_unix");

        try {
          if (!pieces.containsKey(osmId))
            pieces.put(osmId, new HashMap<Long, JSONObject>());
          if (!changeDate.containsKey(osmId))
            changeDate.put(osmId, new HashMap<Long, Long>());

          if (!changeDate.get(osmId).containsKey(cell.id())
            || changeDate.get(osmId).get(cell.id()) > timestamp) {
            pieces.get(osmId).put(cell.id(), (JSONObject) (new JSONParser()).parse(json));
            timestamps.put(osmId, timestamp);

            results.add(new Feature(timestamp, json));
          }
          
        } catch (ParseException e) {
          throw new RuntimeException("JSON Parsing Exception Caught: " + e.getMessage());
        }
      }
    }

    // Recombine Features
    /*for (String osmId : pieces.keySet()) {
      Map<Long, JSONObject> list = pieces.get(osmId);

      JSONObject unified = reconstruct(list);
      results.add(new Feature(
        timestamps.get(osmId),
        unified.toString()
      ));
    }*/

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

  public String updateFeature(String id, String feature) {
		if (id.equals("new")) { // new feature
			// make new osm_id
			id="way/10000000";

			Session session = Database.getSession();
			PreparedStatement statement = Database.prepareFromCache(
			"INSERT INTO global.master(osm_id, json) VALUES(?, ?)");
			session.execute(statement.bind(id, feature));
		} else {
			Session session = Database.getSession();
			PreparedStatement statement = Database.prepareFromCache(
			"UPDATE global.master SET json=? WHERE osm_id=?");
			session.execute(statement.bind(feature, id));
		}
		return id;
  }

  public String deleteFeature(String id) {
		Session session = Database.getSession();
		PreparedStatement statement = Database.prepareFromCache(
		"DELETE FROM global.master WHERE osm_id=?");
		session.execute(statement.bind(id));
		return id;	// success...
  }


  private JSONObject reconstruct(Map<Long, JSONObject> map) {
    return null;
  }
}
