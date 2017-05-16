package edu.mines.csci370;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class Database {
    private static Cluster _cluster;
    private static Session _session;

    /**
     * Disable Direct Instantiation
     */
    private Database() {}

    /**
     * Initialize the DB connection.
     */
    public static void initialize() {
	_cluster = Cluster.builder()
	    .addContactPoints("127.0.0.1")
	    .build();

	_session = _cluster.connect();
    }

    /**
     * Get a reference to the reused session object.
     */
    public static Session getSession() {
	return _session;
    }

    /**
     * Cleanup the DB connection.
     */
    public static void cleanup() {
	if (_cluster != null)
	    _cluster.close();
    }
}
