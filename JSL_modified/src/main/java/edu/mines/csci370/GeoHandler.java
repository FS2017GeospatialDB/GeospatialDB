package edu.mines.csci370;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.geometry.*;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import edu.mines.csci370.api.Feature;
import edu.mines.csci370.api.GeolocationService;
import org.geotools.geojson.geom.GeometryJSON;
import org.geotools.geometry.jts.JTSFactoryFinder;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

public class GeoHandler implements GeolocationService.Iface {
    private static int OFFSET = 2;
    private static int BASE_LEVEL = 13;
    private static int UPPER_LEVEL = 4;
    private static int NUM_COVERING_LIMIT = 1;

    /**
     * Helper function to test if the cell ids are on the same level
     *
     * @param cellIds a list of cellIds
     * @return bool
     */
    private boolean isCellsTheSameLevel(List<S2CellId> cellIds) {
        int level = -1;
        for (S2CellId cellId : cellIds) {
            int cur_lv = cellId.level();
            if (level == -1) level = cur_lv;
            else if (level != cur_lv) return false;
        }
        return true;
    }

    private boolean isCellsTheSameLevel(S2CellUnion cellUnion) {
        return isCellsTheSameLevel(cellUnion.cellIds());
    }


    /**
     * Calculate the approximate level of the region given the diagonal distance.
     * A helper function originally found in s2 c++ library, ported to Java
     *
     * @param val diagonal distance
     * @return the approximated level
     */
    private int kAvgDiagGetMinLevel(double val) {
        if (val <= 0) return 30;
        double derive = 2.060422738998471683;
        FRexpResult result = FRexpResult.frexp(val / derive);
        return Math.max(0, Math.min(30, -1 * (result.exponent - 1)));
    }

    /**
     * Get the estimated level of the feature given the diagonal coordinates.
     * The result is bounded by the global parameter: BASE_LEVEL & UPPER_LEVEL.
     * A OFFSET (testing) is used to adjust the level to higher the level in
     * order to get more detailed features.
     *
     * @param bottomLeft
     * @param topRight
     * @return estimated level
     */
    private int getEstimatedLevel(S2LatLng bottomLeft, S2LatLng topRight) {
        double diagonal = Math.abs(bottomLeft.getDistance(topRight).radians());
        int level = kAvgDiagGetMinLevel(diagonal) + OFFSET;
        if (level > BASE_LEVEL) level = BASE_LEVEL;
        else if (level < UPPER_LEVEL) level = UPPER_LEVEL;
        System.out.print("Querying from level " + level + "[");
        return level;
    }

    private TreeMap<Integer, ArrayList<S2CellId>> getCoverings(int baseLevel, S2LatLngRect rect) {
        TreeMap<Integer, ArrayList<S2CellId>> coverMap = new TreeMap<Integer, ArrayList<S2CellId>>();

        int size = NUM_COVERING_LIMIT + 1;
        while (size > NUM_COVERING_LIMIT) {
            ArrayList<S2CellId> covering = new ArrayList<>();
            S2RegionCoverer coverer = new S2RegionCoverer();
            coverer.setMaxLevel(baseLevel);
            coverer.setMinLevel(baseLevel);
            coverer.getCovering(rect, covering);
            coverMap.put(baseLevel, covering);

            assert isCellsTheSameLevel(covering);

            size = covering.size();
            System.out.print(" size = " + size);
            baseLevel--;

            if (baseLevel < UPPER_LEVEL) {
                System.out.println("] MIN LEVEL REACHED");
                break;
            }
        }
        if (baseLevel > UPPER_LEVEL) System.out.println("] to " + (baseLevel + 1));
        return coverMap;
    }

    public String getGeoJsonStrFromLLRect(S2LatLngRect rect) {
        S2LatLng vert0 = rect.getVertex(0);
        S2LatLng vert1 = rect.getVertex(1);
        S2LatLng vert2 = rect.getVertex(2);
        S2LatLng vert3 = rect.getVertex(3);
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
        Coordinate[] coords =
                new Coordinate[]{
                        new Coordinate(vert0.lngDegrees(), vert0.latDegrees()),
                        new Coordinate(vert1.lngDegrees(), vert1.latDegrees()),
                        new Coordinate(vert2.lngDegrees(), vert2.latDegrees()),
                        new Coordinate(vert3.lngDegrees(), vert3.latDegrees()),
                        new Coordinate(vert0.lngDegrees(), vert0.latDegrees())};

        LinearRing ring = geometryFactory.createLinearRing(coords);
        LinearRing holes[] = null; // use LinearRing[] to represent holes
        Polygon polygon = geometryFactory.createPolygon(ring, holes);

        GeometryJSON geojson = new GeometryJSON();
        StringWriter writer = new StringWriter();
        try {
            geojson.writePolygon(polygon, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return writer.toString();
    }

    private List<Feature> letMeMessUpThings(TreeMap<Integer, ArrayList<Feature>> featureMap, TreeMap<Integer, ArrayList<String>> osmIdMap) {
        // Putting everything into the holder
        HashMap<String, List<Feature>> holder = new HashMap<>();
        for (int level : featureMap.keySet()) {
            for (int i = 0; i < featureMap.get(level).size(); i++) {
                Feature feature = featureMap.get(level).get(i);
                String osmId = osmIdMap.get(level).get(i);
                System.out.println(osmId);
                if (!holder.containsKey(osmId)) {
                    holder.put(osmId, new ArrayList<>());
                }
                holder.get(osmId).add(feature);
            }
        }
        /* Good above this line */

//        List<Feature> result = new ArrayList<>();
//        for (String osm_id : holder.keySet()) {
//            Feature largest_feature = null;
//            for (Feature feature : holder.get(osm_id)) {
//                if (largest_feature == null) largest_feature = feature;
//                else {
//                    long size_of_large = largest_feature.json.length();
//                    long size_of_current = feature.json.length();
//                    if (size_of_current > size_of_large) largest_feature = feature;
//                }
//            }
//            result.add(largest_feature);
//        }

        List<List<Feature>> temp = new ArrayList<List<Feature>>(holder.values());
        List<Feature> result = new ArrayList<>();

        for (List<Feature> features: temp){
            result.addAll(features);
        }

        return result;
    }

    private List<Feature> combineMethodResults(TreeMap<Integer, ArrayList<Feature>> featureMap, TreeMap<Integer, ArrayList<String>> osmIdMap) {
        boolean reduce = true;
        List<Feature> results;
        if (reduce) {
            HashMap<String, Feature> resultMap = new HashMap<>();
            for (int level : featureMap.keySet()) {
                for (int i = 0; i < featureMap.get(level).size(); i++) {
                    Feature feature = featureMap.get(level).get(i);
                    String osmId = osmIdMap.get(level).get(i);
                    if (!resultMap.containsKey(osmId)) {
                        resultMap.put(osmId, feature);
                    } else {
                        // Compare the size of the feature with the new feature
                        Feature existing_feature = resultMap.get(osmId);
                        long size_of_existing = existing_feature.getJson().length();
                        long size_of_new = feature.getJson().length();
                        if (size_of_new > size_of_existing) {
                            resultMap.put(osmId, feature);
                        }
                    }
                }
            }
            results = new ArrayList<>(resultMap.values());
        } else {
            results = new ArrayList<>();
            for (int level : featureMap.keySet()) {
                results.addAll(featureMap.get(level));
            }
        }
        return results;
    }

    @Override
    public List<Feature> getFeatures(double lBox, double rBox, double bBox, double tBox, long timestampMillis) {
        long start = System.currentTimeMillis();

        S2LatLng bottomLeft = S2LatLng.fromDegrees(bBox, lBox);
        S2LatLng topRight = S2LatLng.fromDegrees(tBox, rBox);
        S2LatLngRect rect = S2LatLngRect.fromPointPair(bottomLeft, topRight);

        int baseLevel = getEstimatedLevel(bottomLeft, topRight);

        TreeMap<Integer, ArrayList<S2CellId>> coveringS2Cells = getCoverings(baseLevel, rect);

        // Lookup the Cells in the Database
        Session session = Database.getSession();
        PreparedStatement statement = Database.prepareFromCache(
                "SELECT unixTimestampOf(time) AS time_unix, osm_id, json FROM global.slave WHERE level=? AND s2_id=? AND time >= ?");

        TreeMap<Integer, ArrayList<Feature>> featureMap = new TreeMap<Integer, ArrayList<Feature>>();
        TreeMap<Integer, ArrayList<String>> osmIdMap = new TreeMap<Integer, ArrayList<String>>();


        int coveringCounter = 0;
        int featureCounter = 0;
        for (int level : coveringS2Cells.keySet()) {
            for (S2CellId cell : coveringS2Cells.get(level)) {
                coveringCounter++;
                ResultSet rs = session.execute(statement.bind(cell.level(), cell.id(), UUIDs.startOf(timestampMillis)));
                for (Row row : rs.all()) {
                    featureCounter++;
                    Feature feature = new Feature(row.getLong("time_unix"), row.getString("json"));
                    if (!featureMap.containsKey(level)) {
                        featureMap.put(level, new ArrayList<>());
                        osmIdMap.put(level, new ArrayList<>());
                    }
                    featureMap.get(level).add(feature);
                    osmIdMap.get(level).add(row.getString("osm_id"));
                }
            }
        }
        System.out.println("Contains: " + featureCounter + " features");
        System.out.println("Total covering processed: " + coveringCounter);

        long finish = System.currentTimeMillis();
        //return combineMethodResults(featureMap, osmIdMap);
        return letMeMessUpThings(featureMap, osmIdMap);
    }

    @Override
    public List<Feature> getCell(double lat, double lng, long timestampMillis) {
        long start = System.currentTimeMillis();

        PreparedStatement statement = Database.prepareFromCache(
                "SELECT unixTimestampOf(time) AS time_unix, json FROM global.slave WHERE level=? AND s2_id=? AND time >= ?");

        S2LatLng loc = S2LatLng.fromDegrees(lat, lng);
        List<Feature> results = new ArrayList<>();

        Session session = Database.getSession();
        for (int i = BASE_LEVEL; i > BASE_LEVEL - 1; i--) {
            S2CellId cell = S2CellId.fromLatLng(loc).parent(i);
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
        return results;
    }
}
