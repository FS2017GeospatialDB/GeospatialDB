package edu.mines.csci370;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import edu.mines.csci370.api.Feature;
import edu.mines.csci370.api.GeolocationService;

public class GeoHandler implements GeolocationService.Iface {

  @Override
  public List<Feature> getFeatures(double lBox, double rBox, double bBox, double tBox) {

    List<Feature> results = new ArrayList<>();
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
      && feature.getLongitude() > bBox && feature.getLatitude() < tBox)
        results.add(feature);
    }
    
    return results;
  }
}
