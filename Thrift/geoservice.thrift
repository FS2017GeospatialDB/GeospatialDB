namespace java edu.mines.csci370.api

/**
 * Feature struct for communication with client.
 */
struct Feature {
      1: i64 time,
      2: string json
}

/**
 * This is the primary service. It is responsible for responding to feature
 * requests from clients and handling them appropriately by interfacing
 * with Cassandra.
 */
service GeolocationService {

	/**
	 * Primary geolocation API call.
	 */
	list<Feature> getFeatures(1:double lBox, 2:double rBox, 3:double bBox, 4:double tBox, 5:i64 timestamp);

	list<Feature> getCell(1:double lat, 2:double lng, 3:i64 timestamp);

	/**
	 * Feature modification calls, return id or failure code
	 */
	i32 deleteFeature(1:i32 id);
	i32 updateFeature(1:i32 id, 2:string feature);
}
