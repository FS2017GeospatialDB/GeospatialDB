package edu.mines.csci370;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import com.google.common.geometry.S2Region;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2RegionCoverer;

import edu.mines.csci370.api.Feature;
import edu.mines.csci370.api.GeolocationService;

public class GeoHandler implements GeolocationService.Iface {

  private static final double SCALE_11 = 30402121.4786;
  private static final double SCALE_15 = 118758.286994;
  private static final double SCALE_19 = 463.899558572;

  @Override
  public List<Feature> getFeatures(double lBox, double rBox, double bBox, double tBox) {

    // Build Rectangles
    S2LatLng bottomLeft = S2LatLng.fromDegrees(bBox, lBox);
    S2LatLng topRight = S2LatLng.fromDegrees(tBox, rBox);
    S2LatLngRect rect = new S2LatLngRect(bottomLeft, topRight);

    // Determine Necessary Level
    double area = rect.area() * S2LatLng.EARTH_RADIUS_METERS * S2LatLng.EARTH_RADIUS_METERS;
    int level = 19;
    if (area / SCALE_19 > 2d) level = 15;
    if (area / SCALE_15 > 2d) level = 11;

    // Get Area Covering
    S2RegionCoverer coverer = new S2RegionCoverer();
    coverer.setMinLevel(level);
    coverer.setMaxLevel(level);
    coverer.setMaxCells(Integer.MAX_VALUE);
    ArrayList<S2CellId> cells = new ArrayList<>();
    System.out.print("starting...");
    coverer.getCovering(rect, cells);

    for (S2CellId id : cells) {
      System.out.println(id.toString());
    }

    /*List<Feature> results = new ArrayList<>();
    Session session = Database.getSession();
    
    ResultSet rs = session.execute("SELECT * FROM features.features");
    while (!rs.isExhausted()) {
        Row row = rs.one();
        Feature feature = new Feature(
              row.getLong("id"),
              row.getDouble("latitude"),
              row.getDouble("longitude"),
              null);

        if (feature.getLatitude() > lBox && feature.getLatitude() < rBox
      && feature.getLongitude() > bBox && feature.getLongitude() < tBox)
        results.add(feature);
    }*/
    
    return new ArrayList<Feature>();
  }
}
