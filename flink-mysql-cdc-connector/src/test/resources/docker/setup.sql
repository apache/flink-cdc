-- In production you would almost certainly limit the replication user must be on the follower (slave) machine,
-- to prevent other clients accessing the log from other machines. For example, 'replicator'@'follower.acme.com'.
-- However, in this database we'll grant 2 users different privileges:
--
-- 1) 'flinkuser' - all privileges required by the snapshot reader AND binlog reader (used for testing)
-- 2) 'mysqluser' - all privileges
--
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES  ON *.* TO 'flinkuser'@'%';
CREATE USER 'mysqluser' IDENTIFIED BY 'mysqlpw';
GRANT ALL PRIVILEGES ON *.* TO 'mysqluser'@'%';

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  emptydb
-- ----------------------------------------------------------------------------------------------------------------
CREATE DATABASE emptydb;
