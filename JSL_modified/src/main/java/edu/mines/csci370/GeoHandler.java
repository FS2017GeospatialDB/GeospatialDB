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
  // public List<S2CellId> get_covering(S2LatLng bottomLeft, S2LatLng topRight){

  //   return new ArrayList<> ();
  // }

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

    /*
    def get_covering(bbox):
    '''Given the boundary box, find the most reasonable region covering
    of the area'''
    top_left = bbox[0]
    bottom_right = bbox[1]
    
    ll_top_left = S2LatLng.FromDegrees(top_left[0], top_left[1])
    ll_bottom_right = S2LatLng.FromDegrees(bottom_right[0], bottom_right[1])
    
    llrect = S2LatLngRect.FromPointPair(ll_top_left, ll_bottom_right)
    digonal_distance = ll_top_left.GetDistance(ll_bottom_right).abs().radians()
    
    level = kAvgDiag.get_min_lv(digonal_distance)
    
    # Restrict the max level to base level, if the generated level is larger than that
    level = BASE_LEVEL if level > BASE_LEVEL else level
    
    covering = []
    size = NUM_COVERING_LIMIT + 1
    while size > NUM_COVERING_LIMIT:
        coverer = S2RegionCoverer()
        coverer.set_max_level(level)
        coverer.set_min_level(level)
        covering = coverer.GetCovering(llrect)
        level = level - 1
        size = len(covering)
        # restrict the min level the seeking loop can go
        if level < MIN_LEVEL:
            print 'A FEATURE HITS MIN_LEVEL LIMIT:', MIN_LEVEL
            break
    
    ################DEBUG FUNCTION CALLS###########
    __print_new_low_lv(level)
    #__test_point(level)        # make sure when only points, they are on lv 30
    #__print_new_many_covering(len(covering))
    __check_covering_same_level(covering) # check all generated covering are the same level
    ################END DEBUG FUNCTIONS############
    return covering
    */

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
    // PreparedStatement statement = Database.prepareFromCache(
    //   "SELECT unixTimestampOf(time) AS time_unix, json FROM global.slave WHERE level=? AND s2_id=? AND time >= ?");
    PreparedStatement statement = Database.prepareFromCache(
        "SELECT unixTimestampOf(time) AS time_unix, json FROM global.slave WHERE level=? AND s2_id=?");

    // Historical Query info
    // System.out.println(timestampMillis);
    // mintimeuuid: YYYY-MM-DD hh:mm+____

    // Execute the Query
    for (S2CellId cell : cells) {
      ResultSet rs = session.execute(statement.bind(level, cell.id()));

      // ResultSet rs = session.execute(statement.bind(level, cell.id(), UUIDs.startOf(timestampMillis)));

      while (!rs.isExhausted()) {
        Row row = rs.one();
        Feature feature = new Feature(row.getLong("time_unix"), row.getString("json"));

        results.add(feature);
      }
    }

    long finish = System.currentTimeMillis();
    System.out.println(cells.size() + " queries @ scale=" + level + " in " + (finish - start) + "ms");
    return results;
  }

  @Override
  public List<Feature> getCell(double lat, double lng, long timestampMillis) {
    PreparedStatement statement = Database.prepareFromCache(
        "SELECT unixTimestampOf(time) AS time_unix, json FROM global.slave WHERE level=? AND s2_id=? AND time >= ?");

    // Find cell
    long start = System.currentTimeMillis();
    S2LatLng loc = S2LatLng.fromDegrees(lat, lng);
    List<Feature> results = new ArrayList<>();

    Session session = Database.getSession();
    for (int i = 16; i > 0; i--) {
      S2CellId cell = new S2Cell(loc).id().parent(i);
        System.out.println("Current level: " + i);

      // Lookup the Cells in the Database

      // Historical Query info
      // System.out.println(timestampMillis);
      // mintimeuuid: YYYY-MM-DD hh:mm+____

      // Execute the Query
      ResultSet rs = session.execute(statement.bind(cell.level(), cell.id(), UUIDs.startOf(timestampMillis)));

      int counter = 0;
      while (!rs.isExhausted()) {
        counter++;
        Row row = rs.one();
        Feature feature = new Feature(row.getLong("time_unix"), row.getString("json"));
        results.add(feature);
      }
      System.out.println("Contains: " + counter + " features");
    }

    long finish = System.currentTimeMillis();
    //System.out.println("1 query @ scale=" + cell.level() + " in " + (finish - start) + "ms");
    return results;
  }
}
