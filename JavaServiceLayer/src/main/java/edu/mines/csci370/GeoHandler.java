package edu.mines.csci370;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.geometry.*;
import edu.mines.csci370.api.Feature;
import edu.mines.csci370.api.GeolocationService;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

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

    private List<Feature> letMeMessUpThings(TreeMap<Integer, ArrayList<Feature>> featureMap, TreeMap<Integer, ArrayList<String>> osmIdMap) {
        // Putting everything into the holder
        HashMap<String, List<Feature>> holder = new HashMap<>();
        for (int level : featureMap.keySet()) {
            for (int i = 0; i < featureMap.get(level).size(); i++) {
                Feature feature = featureMap.get(level).get(i);
                String osmId = osmIdMap.get(level).get(i);
//                System.out.println(osmId);
//                if (osmId.equals("way/33113233")) {
//                    System.out.println(feature.json);
//                }
                if (!holder.containsKey(osmId)) {
                    holder.put(osmId, new ArrayList<>());
                }
                holder.get(osmId).add(feature);
            }
        }
        /* Good above this line */

        List<Feature> result = new ArrayList<>();
        for (String osm_id : holder.keySet()) {
            Feature largest_feature = null;
            for (Feature feature : holder.get(osm_id)) {
                if (largest_feature == null) largest_feature = feature;
                else {
                    long size_of_large = largest_feature.json.length();
                    long size_of_current = feature.json.length();
                    if (size_of_current > size_of_large) largest_feature = feature;
                }
            }
            result.add(largest_feature);
        }

//        List<List<Feature>> temp = new ArrayList<List<Feature>>(holder.values());
//        List<Feature> result = new ArrayList<>();
//
//        for (List<Feature> features: temp){
//            result.addAll(features);
//        }

        return result;
    }

    private List<Feature> justDupeMethod(TreeMap<Integer, ArrayList<Feature>> featureMap,
                                         TreeMap<Integer, ArrayList<String>> osmIdMap) {
        HashMap<String, HashSet<Feature>> resultMap = new HashMap<>();
        for (int level : featureMap.keySet()) {
            for (int i = 0; i < featureMap.get(level).size(); i++) {
                Feature feature = featureMap.get(level).get(i);
                String osmId = osmIdMap.get(level).get(i);
                if (!resultMap.containsKey(osmId)) {
                    resultMap.put(osmId, new HashSet<>());
                }
                resultMap.get(osmId).add(feature);
            }
        }

        // Adding all unique features to results array
        ArrayList<Feature> results = new ArrayList<>();
        for (String osmId : resultMap.keySet()) {
            for (Feature feature : resultMap.get(osmId)) {
                results.add(feature);
            }
        }
        return results;
    }

    private List<Feature> combineMethodResults(HashMap<Long, HashMap<String, Feature>> featureMap) {
        // TODO: FIX! this function is disregarding the smaller part of cut features
        List<Feature> results;
        HashMap<String, Feature> resultMap = new HashMap<>();
        for (long s2_id : featureMap.keySet()) {
            HashMap<String, Feature> features = featureMap.get(s2_id);
            for (String id : features.keySet()) {
                Feature f = features.get(id);
                if (f.getJson() == null) continue;
                if (!resultMap.containsKey(id)) {
                    resultMap.put(id, f);
                } else {
                    // Compare the size of the feature with the new feature
                    Feature existing_feature = resultMap.get(id);
                    if (f.getJson().length() > existing_feature.getJson().length()) {
                        resultMap.put(id, f);
                    }
                }
            }
        }
        results = new ArrayList<>(resultMap.values());
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
                "SELECT unixTimestampOf(time) AS time_unix, osm_id, json FROM global01.slave WHERE level=? AND s2_id=? AND time>=?");

        HashMap<Long, HashMap<String, Feature>> featureMap = new HashMap<Long, HashMap<String, Feature>>();

        int coveringCounter = 0;
        int featureCounter = 0;
        for (int level : coveringS2Cells.keySet()) {
            HashMap<String, Feature> features = new HashMap<String, Feature>();
            for (S2CellId cell : coveringS2Cells.get(level)) {
                coveringCounter++;
                ResultSet rs = session.execute(statement.bind(cell.level(), cell.id(), UUIDs.startOf(timestampMillis)));
                for (Row row : rs.all()) {
                    featureCounter++;
                    Feature f = new Feature(row.getLong("time_unix"), row.getString("json"));
                    String id = row.getString("osm_id");
                    if (features.containsKey(id)) {
                        if (f.getTime() < features.get(id).getTime()) {
                            features.put(id, f);
                        } else continue;
                    } else {
                        features.put(id, f);
                    }
                }
                if (!features.isEmpty()) {
                    featureMap.put(cell.id(), features);
                }
            }
        }
        System.out.println("Contains: " + featureCounter + " features");
        System.out.println("Total covering processed: " + coveringCounter);

        long finish = System.currentTimeMillis();
        System.out.println("Time taken: " + (finish - start));
        return combineMethodResults(featureMap);
        //return letMeMessUpThings(featureMap, osmIdMap);
        //return justDupeMethod(featureMap, osmIdMap);
    }

    @Override
    public List<Feature> getCell(double lat, double lng, long timestampMillis) {
        long start = System.currentTimeMillis();

        PreparedStatement statement = Database.prepareFromCache(
                "SELECT unixTimestampOf(time) AS time_unix, json FROM global01.slave WHERE level=? AND s2_id=? AND time >= ?");

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


    @Override
    public String updateFeature(String id, String feature) {
        if (id.equals("new")) { // new feature
            // make new osm_id based on timestamp
            id = Long.toString(System.currentTimeMillis());
            feature = feature.replaceAll("tempOSM_ID_placeholder", id);

            // insert into slave/master by calling python script
            try {
                // TODO FIX THE ADDRESS FOR PYTHON 2.7 (Also in update and delete)
                ProcessBuilder pb = new ProcessBuilder("/usr/bin/python2.7", "jsl.py", id, feature, "new");
                String path = "";
                try {
                    path = (new File(getClass().getProtectionDomain().getCodeSource().getLocation().toURI())).getParent().toString();
                } catch (URISyntaxException e) {
                    path = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
                    path = path.substring(0, path.lastIndexOf("/"));
                } finally {
                    path = path.substring(0, path.lastIndexOf("/")) + "/src/py_interface/";
                    pb.directory(new File(path));
                }
                Process p = pb.start();
            } catch (IOException e) {
                System.out.println(e);
            }
        } else { // updating feature
            // insert into slave/master by calling python script
            try {
                ProcessBuilder pb = new ProcessBuilder("/usr/bin/python2.7", "jsl.py", id, feature, "modify");
                String path = "";
                try {
                    path = (new File(getClass().getProtectionDomain().getCodeSource().getLocation().toURI())).getParent().toString();
                } catch (URISyntaxException e) {
                    path = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
                    path = path.substring(0, path.lastIndexOf("/"));
                } finally {
                    path = path.substring(0, path.lastIndexOf("/")) + "/src/py_interface/";
                    pb.directory(new File(path));
                }
                Process p = pb.start();
            } catch (IOException e) {
                System.out.println(e);
            }
        }
        return id;
    }

    @Override
    public String deleteFeature(String id) {
        // delete from slave/master by calling python script
        try {
            ProcessBuilder pb = new ProcessBuilder("/usr/bin/python2.7", "jsl.py", id, "", "delete");
            String path = "";
            try {
                path = (new File(getClass().getProtectionDomain().getCodeSource().getLocation().toURI())).getParent().toString();
            } catch (URISyntaxException e) {
                path = getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
                path = path.substring(0, path.lastIndexOf("/"));
            } finally {
                path = path.substring(0, path.lastIndexOf("/")) + "/src/py_interface/";
                pb.directory(new File(path));
            }
            System.out.println(id);
            Process p = pb.start();
        } catch (IOException e) {
            System.out.println(e);
        }
        return id;    // success...
    }
}
