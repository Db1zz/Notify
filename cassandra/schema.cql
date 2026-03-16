CREATE KEYSPACE IF NOT EXISTS notify
WITH replication = {'class':'SimpleStrategy','replication_factor':1};

USE notify;

CREATE TABLE IF NOT EXISTS user_sources (
    userid UUID,
    sourceid UUID,
    type TEXT,
    PRIMARY KEY (userid, sourceid)
);
