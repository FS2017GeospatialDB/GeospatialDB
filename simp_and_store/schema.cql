//////////////////////////////////////////////////////////////////
//
//                   SCHEMA DESIGN V3_ryan
//
//////////////////////////////////////////////////////////////////

DROP KEYSPACE IF EXISTS GLOBAL01;
CREATE KEYSPACE global01 WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

DROP TABLE IF EXISTS global01.master;
CREATE TABLE global01.master (
  osm_id      text,
  json        text,

  PRIMARY KEY (osm_id)
);

DROP TABLE IF EXISTS global01.slave;
CREATE TABLE global01.slave (
  level       int,
  s2_id       bigint,
  time        timeuuid,
  osm_id      text,
  json        text,
  
  PRIMARY KEY ((level, s2_id), time, osm_id)
) WITH CLUSTERING ORDER BY (time DESC);