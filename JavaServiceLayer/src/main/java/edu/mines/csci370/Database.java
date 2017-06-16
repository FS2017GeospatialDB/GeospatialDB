package edu.mines.csci370;

import com.datastax.driver.core.*;

import java.io.File;
import java.io.FileReader;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

@SuppressWarnings("WeakerAccess")
public class Database {
    private static Cluster _cluster;
    private static Session _session;
    private static Map<String, PreparedStatement> _cache = new HashMap<>();
    private static List<InetAddress> db_contact_points;
    private static String keyspace;

    /**
     * Disable Direct Instantiation
     */
    private Database() {
    }

    /**
     * given the string of the db host (separated by ','), parse the
     * address of the hosts and stores in to db contact points.
     */
    private static void parseDBContactPts(String contact_str) {
        String[] contactpts;
        if (contact_str == null) {
            System.err.println("CONTACT PTS DEFAULT: 127.0.0.1");
            contactpts = new String[]{"127.0.0.1"};
        } else {
            contactpts = contact_str.split(",");
        }
        db_contact_points = new ArrayList<>();
        for (String host : contactpts) {
            try {
                db_contact_points.add(Inet4Address.getByName(host));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
    }

    private static void parseKeyspace(String ks) {
        if (ks == null) {
            System.err.println("KEYSPACE DEFAULT: global");
            keyspace = "global";
        } else
            keyspace = ks;
    }


    /**
     * Given the filename of the config file, parse the file.
     */
    public static void initialize(String filename) {
        if (filename == null) {
            parseDBContactPts(null);
            parseKeyspace(null);
        } else {
            File configFile = new File(filename);
            try {
                FileReader reader = new FileReader(configFile);
                Properties props = new Properties();
                props.load(reader);

                String contactPts = props.getProperty("contact_points");
                String keySpace = props.getProperty("key_space");

                parseDBContactPts(contactPts);
                parseKeyspace(keySpace);

                reader.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        // post behavior
        initialize(db_contact_points, keyspace);
    }


    /**
     * Initialize the DB connection.
     */
    public static void initialize(List<InetAddress> contactPts, String keyspace) {
        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions
                .setConnectionsPerHost(HostDistance.LOCAL, 2, 8);
        _cluster = Cluster.builder()
                .withPoolingOptions(poolingOptions)
                .addContactPoints(contactPts)
                .build();
        _session = _cluster.connect(keyspace);
    }

    /**
     * Get a reference to the reused session object.
     */
    public static Session getSession() {
        return _session;
    }

    /**
     * Get a reference to the static cluster object.
     */
    public static Cluster getCluster() {
        return _cluster;
    }

    /**
     * Prepare a statement (or retrieve it from the cache if cached).
     */
    public static PreparedStatement prepareFromCache(String statement) {
        if (_cache.containsKey(statement))
            return _cache.get(statement);
        _cache.put(statement, _session.prepare(statement));
        return _cache.get(statement);
    }

    /**
     * Auto close the DB connection when the program quits
     */
    @Override
    protected void finalize() {
        try {
            super.finalize();
            if (_cluster != null)
                _cluster.close();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }
}
