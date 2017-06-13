package edu.mines.csci370;

import java.util.Map;
import java.util.HashMap;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.PreparedStatement;

public class Database {
    private static Cluster _cluster;
    private static Session _session;
    private static Map<String, PreparedStatement> _cache = new HashMap<>();

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

    /**
     * Auto cleanup the DB connection
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (_cluster != null)
            _cluster.close();
    }

    /**
     * Prepare a statement (or retrieve it from the cache).
     */
    public static PreparedStatement prepareFromCache(String statement) {
        if (_cache.containsKey(statement))
            return _cache.get(statement);

        _cache.put(statement, _session.prepare(statement));
        return _cache.get(statement);
    }
}
