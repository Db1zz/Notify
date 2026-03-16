CREATE KEYSPACE IF NOT EXISTS notify
WITH replication = {'class':'SimpleStrategy','replication_factor':1};

USE notify;

CREATE TABLE IF NOT EXISTS blocked_notifs (
    userid UUID,
    sourceid UUID,
    type TEXT,
    PRIMARY KEY (userid, sourceid)
);

CREATE TABLE IF NOT EXISTS notifs_to_send (
    userid UUID,
    sourceid UUID,
    type TEXT,
    PRIMARY KEY (userid, sourceid)
);
