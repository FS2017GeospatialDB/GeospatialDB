package edu.mines.csci370;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.HashMap;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.utils.UUIDs;

import com.google.common.geometry.S2;
import com.google.common.geometry.S2Cell;
import com.google.common.geometry.S2Region;
import com.google.common.geometry.S2CellId;
import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2LatLngRect;
import com.google.common.geometry.S2RegionCoverer;

import edu.mines.csci370.api.Feature;
import edu.mines.csci370.api.GeolocationService;
import edu.mines.csci370.api.FRexpResult;

public class GeoHandler implements GeolocationService.Iface {
	private static int BASE_LEVEL = 13;
	private static int UPPER_LEVEL = 4;
	private static int NUM_COVERING_LIMIT = 3;

	public int getMinLevel(double val) {
		if (val <= 0) {
			return 30;
		}
		double derive = 2.060422738998471683;
		FRexpResult result = FRexpResult.frexp(val / derive);
		int level = (int)(Math.max(0, Math.min(30, -1 * (result.exponent - 1)))); 
		return level; 
	}

	public int getBase(S2LatLng bottomLeft, S2LatLng topRight) {
		double diagonal = Math.abs(bottomLeft.getDistance(topRight).radians());
		int level = getMinLevel(diagonal);
		if (level > BASE_LEVEL) level = BASE_LEVEL;
		else if(level < UPPER_LEVEL) level = UPPER_LEVEL;
		System.out.print("Querying from level " + level);
		return level;
	}

	public TreeMap<Integer, ArrayList<S2CellId>> getCovers(int baseLevel, S2LatLngRect rect) {
		TreeMap<Integer, ArrayList<S2CellId>> coverMap = new TreeMap<Integer, ArrayList<S2CellId>>();
		ArrayList<S2CellId> covering = new ArrayList<S2CellId>();
		int size = NUM_COVERING_LIMIT + 1;

		while (size > NUM_COVERING_LIMIT) {
			S2RegionCoverer coverer = new S2RegionCoverer();
			coverer.setMaxLevel(baseLevel);
			coverer.setMinLevel(baseLevel);
			coverer.getCovering(rect, covering);
			coverMap.put(baseLevel, covering);
			baseLevel = baseLevel - 1;
			size = covering.size();
			covering = new ArrayList<S2CellId>();

			if (baseLevel < UPPER_LEVEL) {
				System.out.println("MIN LEVEL REACHED");
				break;
			}
		}
		if (baseLevel > UPPER_LEVEL) System.out.println(" to " + (baseLevel + 1));
		return coverMap;
	}

	@Override
	public List<Feature> getFeatures(double lBox, double rBox, double bBox, double tBox, long timestampMillis) {

		// Build Rectangles
		long start = System.currentTimeMillis();
		S2LatLng bottomLeft = S2LatLng.fromDegrees(bBox, lBox);
		S2LatLng topRight = S2LatLng.fromDegrees(tBox, rBox);
		S2LatLngRect rect = new S2LatLngRect(bottomLeft, topRight);
		int baseLevel = getBase(bottomLeft, topRight);
		
		TreeMap<Integer, ArrayList<S2CellId>> coveringS2Cells = getCovers(baseLevel, rect);
		System.out.println("Key set size: " + coveringS2Cells.keySet().size());

		// Lookup the Cells in the Database
		Session session = Database.getSession();
		// PreparedStatement statement = Database.prepareFromCache(
		//   "SELECT unixTimestampOf(time) AS time_unix, json FROM global.slave WHERE level=? AND s2_id=? AND time >= ?");
		PreparedStatement statement = Database.prepareFromCache(
				"SELECT unixTimestampOf(time) AS time_unix, osm_id, json FROM global.slave WHERE level=? AND s2_id=?");

		TreeMap<Integer, ArrayList<Feature>> featureMap = new TreeMap<Integer, ArrayList<Feature>>();
		TreeMap<Integer, ArrayList<String>> osmIdMap = new TreeMap<Integer, ArrayList<String>>();

		for (int level: coveringS2Cells.keySet()) {
			for (S2CellId cell: coveringS2Cells.get(level)) {
				ResultSet rs = session.execute(statement.bind(level, cell.id()));

				while (!rs.isExhausted()) {
					Row row = rs.one();
					Feature feature = new Feature(row.getLong("time_unix"), row.getString("json"));
					if (!featureMap.containsKey(level)) {
						featureMap.put(level, new ArrayList<Feature>());
						osmIdMap.put(level, new ArrayList<String>());
					}
					featureMap.get(level).add(feature);
					osmIdMap.get(level).add(row.getString("osm_id"));
				}
			}
		}

		HashMap<String, Feature> resultMap = new HashMap<String, Feature>();
		for (int level: featureMap.keySet()) {
			System.out.println(level);
			for (int i = 0; i < featureMap.get(level).size(); i++) {
				Feature feature = featureMap.get(level).get(i);
				String osmId = osmIdMap.get(level).get(i);
				if (!resultMap.containsKey(osmId)) {
					resultMap.put(osmId, feature);
				}
			}
		}

		List<Feature> results = new ArrayList<Feature>(resultMap.values());
		//		for (int i = baseLevel; i >= upLevel; i--) {
		//			System.out.println("Querying on level " + i);
		//
		//			ArrayList<S2CellId> cells = new ArrayList<>();
		//			S2RegionCoverer iterCover = new S2RegionCoverer();
		//			iterCover.setMinLevel(i);
		//			iterCover.setMaxLevel(i);
		//			iterCover.getCovering(rect, cells);
		//
		//
		//			// Historical Query info
		//			// System.out.println(timestampMillis);
		//			// mintimeuuid: YYYY-MM-DD hh:mm+____
		//
		//			// Execute the Queries
		//			for (S2CellId cell : cells) {
		//				ResultSet rs = session.execute(statement.bind(i, cell.id()));
		//
		//				// ResultSet rs = session.execute(statement.bind(level, cell.id(), UUIDs.startOf(timestampMillis)));
		//
		//				while (!rs.isExhausted()) {
		//					Row row = rs.one();
		//					Feature feature = new Feature(row.getLong("time_unix"), row.getString("json"));
		//
		//					results.add(feature);
		//				}
		//			}
		//		}

		long finish = System.currentTimeMillis();
		//System.out.println(cells.size() + " queries @ scale=" + level + " in " + (finish - start) + "ms");
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
		for (int i = BASE_LEVEL; i > 0; i--) {
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
