package edu.mines.csci370;

import java.util.ArrayList;
import java.util.List;

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
    Session session = Database.getSession();
    PreparedStatement statement = Database.prepareFromCache(
      "SELECT unixTimestampOf(time) AS time_unix, json FROM global.slave WHERE level=? AND s2_id=? AND time >= ?");

    // Historical Query info
    // System.out.println(timestampMillis);
    // mintimeuuid: YYYY-MM-DD hh:mm+____

    // Execute the Query
    for (S2CellId cell : cells) {
      ResultSet rs = session.execute(statement.bind(level, cell.id(), UUIDs.startOf(timestampMillis)));

      while (!rs.isExhausted()) {
        Row row = rs.one();
        Feature feature = new Feature(
          row.getLong("time_unix"),
          row.getString("json"));

        results.add(feature);
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

    // Historical Query info
    // System.out.println(timestampMillis);
    // mintimeuuid: YYYY-MM-DD hh:mm+____

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
}
