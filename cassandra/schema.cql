CREATE KEYSPACE IF NOT EXISTS myapp
WITH replication = {'class':'SimpleStrategy','replication_factor':1};

USE myapp;

CREATE TABLE IF NOT EXISTS user_sources (
    userid UUID,
    sourceid UUID,
    type TEXT,
    PRIMARY KEY (userid, sourceid)
);
