//////////////////////////////////////////////////////////////////
//
//                   SCHEMA DESIGN V3_ryan
//
//////////////////////////////////////////////////////////////////

DROP KEYSPACE IF EXISTS GLOBAL;
CREATE KEYSPACE global WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

DROP TABLE IF EXISTS global.master;
CREATE TABLE global.master (
  osm_id      text,
  json        text,

  PRIMARY KEY (osm_id)
);

DROP TABLE IF EXISTS global.slave;
CREATE TABLE global.slave (
  level       int,
  s2_id       bigint,
  time        timeuuid,
  osm_id      text,
  is_cut      boolean,
  json        text,
  
  PRIMARY KEY ((level, s2_id), time, is_cut, osm_id)
) WITH CLUSTERING ORDER BY (time DESC);