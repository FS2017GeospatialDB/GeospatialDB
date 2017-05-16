namespace java api

/**
 * Feature struct for communication with client.
 */
struct Feature {
      1: i64 id,
      2: double latitude,
      3: double longitude,
      4: binary payload
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
	list<Feature> getFeatures(1:double lBox, 2:double rBox, 3:double bBox, 4:double tBox);
}