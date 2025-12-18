# GaussDB CDC Connector - Troubleshooting Guide

This guide helps diagnose and resolve common issues with the GaussDB CDC connector.

---

## Table of Contents

1. [Connection Issues](#connection-issues)
2. [Authentication Problems](#authentication-problems)
3. [Replication Slot Issues](#replication-slot-issues)
4. [Data Type Conversion Errors](#data-type-conversion-errors)
5. [Performance Problems](#performance-problems)
6. [Checkpoint and State Issues](#checkpoint-and-state-issues)
7. [Common Error Messages](#common-error-messages)

---

## Connection Issues

### Symptom: `java.io.EOFException` during connection

**Error Message**:
```
java.io.EOFException
	at org.postgresql.core.PGStream.receiveChar(PGStream.java:469)
	at org.postgresql.core.v3.ConnectionFactoryImpl.doAuthentication(ConnectionFactoryImpl.java:683)
```

**Root Causes**:
1. Network connectivity problems
2. Firewall blocking port 8000
3. GaussDB server not listening on the specified port
4. SSL/TLS configuration mismatch

**Solutions**:

1. **Test Network Connectivity**:
   ```bash
   # Test if port is reachable
   telnet 10.250.0.51 8000

   # Or using nc
   nc -zv 10.250.0.51 8000

   # Test ping
   ping 10.250.0.51
   ```

2. **Check Firewall Rules**:
   ```bash
   # On Linux, check iptables
   sudo iptables -L -n | grep 8000

   # On GaussDB server, ensure port is listening
   netstat -tuln | grep 8000
   ```

3. **Verify GaussDB Server Status**:
   ```bash
   # Check if GaussDB is running
   gs_ctl status -D /path/to/data

   # Check listener configuration
   grep "listen_addresses" /path/to/postgresql.conf
   grep "port" /path/to/postgresql.conf
   ```

4. **Try Different SSL Modes**:
   ```java
   // In JDBC URL, try these variations:
   jdbc:gaussdb://10.250.0.51:8000/db1?sslmode=disable
   jdbc:gaussdb://10.250.0.51:8000/db1?ssl=false
   jdbc:gaussdb://10.250.0.51:8000/db1?sslmode=allow
   jdbc:gaussdb://10.250.0.51:8000/db1?sslmode=prefer
   ```

### Symptom: Connection timeout

**Error Message**:
```
org.postgresql.util.PSQLException: Connection to 10.250.0.51:8000 refused.
Check that the hostname and port are correct and that the postmaster is accepting TCP/IP connections.
```

**Solutions**:

1. **Increase Connection Timeout**:
   ```java
   properties.put("connectTimeout", "60"); // 60 seconds
   properties.put("socketTimeout", "60");
   ```

2. **Check Network Latency**:
   ```bash
   # Measure network latency
   ping -c 10 10.250.0.51

   # Trace route to server
   traceroute 10.250.0.51
   ```

3. **Verify DNS Resolution**:
   ```bash
   # If using hostname instead of IP
   nslookup your-gaussdb-hostname
   dig your-gaussdb-hostname
   ```

---

## Authentication Problems

### Symptom: Authentication failed

**Error Message**:
```
org.postgresql.util.PSQLException: FATAL: password authentication failed for user "tom"
```

**Solutions**:

1. **Verify Credentials**:
   ```bash
   # Test with psql
   psql -h 10.250.0.51 -p 8000 -U tom -d db1

   # Or with GaussDB gsql
   gsql -h 10.250.0.51 -p 8000 -U tom -d db1 -W
   ```

2. **Check User Permissions**:
   ```sql
   -- Connect as admin and check user
   SELECT rolname, rolsuper, rolcanlogin FROM pg_roles WHERE rolname = 'tom';

   -- Grant necessary permissions
   GRANT CONNECT ON DATABASE db1 TO tom;
   GRANT USAGE ON SCHEMA public TO tom;
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO tom;
   ```

3. **Review pg_hba.conf**:
   ```bash
   # Check authentication method for your client IP
   cat /path/to/pg_hba.conf | grep -v "^#"
   ```

   Required entry example:
   ```
   # TYPE  DATABASE   USER   ADDRESS          METHOD
   host    db1        tom    0.0.0.0/0        md5
   # Or for specific IP:
   host    db1        tom    192.168.1.0/24   md5
   ```

4. **Reset Password**:
   ```sql
   -- As superuser
   ALTER USER tom WITH PASSWORD 'new_password';
   ```

### Symptom: Permission denied for replication

**Error Message**:
```
org.postgresql.util.PSQLException: FATAL: must be superuser or replication role to start walsender
```

**Solutions**:

1. **Grant Replication Permission**:
   ```sql
   ALTER USER tom WITH REPLICATION;
   ```

2. **Verify Replication Permission**:
   ```sql
   SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'tom';
   ```

3. **Add to pg_hba.conf**:
   ```
   # TYPE  DATABASE    USER   ADDRESS       METHOD
   host    replication tom    0.0.0.0/0     md5
   ```

---

## Replication Slot Issues

### Symptom: Replication slot already exists

**Error Message**:
```
org.postgresql.util.PSQLException: ERROR: replication slot "flink_1234" already exists
```

**Solutions**:

1. **List Existing Slots**:
   ```sql
   SELECT slot_name, plugin, slot_type, active, restart_lsn
   FROM pg_replication_slots;
   ```

2. **Drop Unused Slot**:
   ```sql
   SELECT pg_drop_replication_slot('flink_1234');
   ```

3. **Use Unique Slot Names**:
   ```java
   // In GaussDBTestBase.java, slot names are generated with random IDs
   public static String getSlotName() {
       final Random random = new Random();
       int id = random.nextInt(10000);
       return "flink_" + id;
   }
   ```

### Symptom: Replication slot inactive or lagging

**Error Message**:
```
WARNING: replication slot "flink_1234" is inactive and consuming disk space
```

**Solutions**:

1. **Check Slot Status**:
   ```sql
   SELECT slot_name, active, active_pid,
          pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
   FROM pg_replication_slots
   WHERE slot_name = 'flink_1234';
   ```

2. **Restart Flink Job**:
   - Ensure Flink job is running and connected
   - Check Flink logs for connection errors

3. **Drop and Recreate Slot**:
   ```sql
   -- Only if job is stopped
   SELECT pg_drop_replication_slot('flink_1234');

   -- Recreate (usually done automatically by connector)
   SELECT * FROM pg_create_logical_replication_slot('flink_1234', 'mppdb_decoding');
   ```

### Symptom: Replication slot limit reached

**Error Message**:
```
org.postgresql.util.PSQLException: ERROR: all replication slots are in use
```

**Solutions**:

1. **Check Current Slot Count**:
   ```sql
   SELECT count(*) FROM pg_replication_slots;
   SHOW max_replication_slots;
   ```

2. **Increase Slot Limit**:
   ```bash
   # Edit postgresql.conf
   max_replication_slots = 20  # Default is 10

   # Restart GaussDB
   gs_ctl restart -D /path/to/data
   ```

3. **Clean Up Old Slots**:
   ```sql
   -- Drop inactive slots older than 1 day
   SELECT pg_drop_replication_slot(slot_name)
   FROM pg_replication_slots
   WHERE NOT active AND restart_lsn < pg_current_wal_lsn();
   ```

---

## Data Type Conversion Errors

### Symptom: Unsupported data type

**Error Message**:
```
java.lang.IllegalArgumentException: Unsupported data type: BYTEA
```

**Solutions**:

1. **Check Supported Types**:
   See TEST_SUITE_SUMMARY.md for complete list of supported types.

2. **Use Compatible Type Mapping**:
   ```sql
   -- Instead of BYTEA, use VARCHAR for binary data
   col_binary VARCHAR(100)

   -- Instead of JSON, use TEXT
   col_json TEXT
   ```

3. **Custom Type Converter** (Advanced):
   Implement custom `DebeziumDeserializationSchema` for special types.

### Symptom: Numeric precision loss

**Error Message**:
```
WARNING: Numeric value 12345.67890 truncated to 12345.68
```

**Solutions**:

1. **Match Precision in Flink DDL**:
   ```sql
   -- GaussDB table
   CREATE TABLE test (amount NUMERIC(10,5));

   -- Flink DDL (must match!)
   CREATE TABLE source (amount DECIMAL(10,5));
   ```

2. **Use STRING for High Precision**:
   ```sql
   -- If precision must be exact
   CREATE TABLE source (amount STRING);
   -- Then parse in Flink with UDF
   ```

---

## Performance Problems

### Symptom: Slow snapshot reading

**Observations**:
- Initial snapshot takes hours
- High memory usage during snapshot
- Frequent GC pauses

**Solutions**:

1. **Tune Chunk Size**:
   ```sql
   'scan.incremental.snapshot.chunk.size' = '1024'  -- Default is 8096
   ```

2. **Increase Parallelism**:
   ```java
   env.setParallelism(8);  // Match available task slots
   ```

3. **Add Indexes on Split Keys**:
   ```sql
   -- On GaussDB
   CREATE INDEX idx_test_id ON test_table(id);
   ```

4. **Use Split Key Optimization**:
   ```sql
   'scan.incremental.snapshot.chunk.key-column' = 'id'
   ```

### Symptom: High replication lag

**Observations**:
- CDC events delayed by minutes/hours
- WAL files accumulating on server

**Solutions**:

1. **Check Network Bandwidth**:
   ```bash
   # Monitor network usage
   iftop -i eth0
   ```

2. **Increase Fetch Size**:
   ```sql
   'debezium.max.batch.size' = '4096'  -- Default is 2048
   'debezium.poll.interval.ms' = '500'
   ```

3. **Enable Compression**:
   ```sql
   'debezium.snapshot.mode' = 'initial'
   'debezium.binary.handling.mode' = 'bytes'
   ```

---

## Checkpoint and State Issues

### Symptom: Checkpoint timeout

**Error Message**:
```
java.util.concurrent.TimeoutException: Checkpoint did not complete within 600000 ms
```

**Solutions**:

1. **Increase Checkpoint Timeout**:
   ```java
   env.getCheckpointConfig().setCheckpointTimeout(1200000L); // 20 minutes
   ```

2. **Reduce Checkpoint Interval**:
   ```java
   env.enableCheckpointing(60000L); // 1 minute instead of default
   ```

3. **Use Incremental Checkpoints**:
   ```java
   env.setStateBackend(new EmbeddedRocksDBStateBackend(true)); // true = incremental
   ```

### Symptom: Cannot restore from savepoint

**Error Message**:
```
org.apache.flink.util.StateMigrationException: The new key serializer must be compatible
```

**Solutions**:

1. **Use State Schema Evolution**:
   - Ensure key types are compatible
   - Use POJO serializers with versioning

2. **Allow Non-Restored State**:
   ```bash
   flink run -s /path/to/savepoint --allowNonRestoredState ...
   ```

3. **Manual State Migration**:
   - Export state to external system
   - Restart with new schema
   - Reimport data

---

## Common Error Messages

### "FATAL: database does not exist"

**Cause**: Database name in configuration doesn't match server.

**Solution**:
```sql
-- List databases
SELECT datname FROM pg_database;

-- Update configuration
'database-name' = 'correct_database_name'
```

### "relation does not exist"

**Cause**: Table name or schema incorrect.

**Solution**:
```sql
-- List tables
SELECT schemaname, tablename FROM pg_tables WHERE schemaname = 'public';

-- Check schema
SELECT nspname FROM pg_namespace;

-- Update configuration
'schema-name' = 'correct_schema'
'table-name' = 'correct_table_name'
```

### "mppdb_decoding plugin not found"

**Cause**: Logical decoding plugin not installed.

**Solution**:
```bash
# Check available plugins
SELECT * FROM pg_available_extensions WHERE name LIKE '%decoding%';

# Install plugin (as superuser)
CREATE EXTENSION mppdb_decoding;

# Verify
SELECT * FROM pg_extension WHERE extname = 'mppdb_decoding';
```

### "WAL level insufficient"

**Error**:
```
FATAL: logical decoding requires wal_level >= logical
```

**Solution**:
```bash
# Edit postgresql.conf
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10

# Restart GaussDB
gs_ctl restart -D /path/to/data
```

---

## Diagnostic Commands

### Connection Diagnostics

```bash
# Test PostgreSQL driver connection
psql -h 10.250.0.51 -p 8000 -U tom -d db1 -c "SELECT version();"

# Test GaussDB driver connection (Java)
java -cp gaussdbjdbc.jar:postgresql.jar TestConnection

# Network diagnostics
telnet 10.250.0.51 8000
nc -zv 10.250.0.51 8000
ping -c 5 10.250.0.51
traceroute 10.250.0.51
```

### Database Diagnostics

```sql
-- Check server version
SELECT version();

-- Check current user and permissions
SELECT current_user, session_user;
SELECT * FROM pg_roles WHERE rolname = current_user;

-- Check replication configuration
SHOW wal_level;
SHOW max_wal_senders;
SHOW max_replication_slots;

-- List replication slots
SELECT * FROM pg_replication_slots;

-- Check table exists
SELECT * FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = 'your_table';

-- Check primary key
SELECT a.attname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = 'public.your_table'::regclass AND i.indisprimary;
```

### Flink Diagnostics

```bash
# Check Flink job status
flink list

# View job details
flink info <job-id>

# Check logs
tail -f /path/to/flink/log/flink-*-taskexecutor-*.log

# Enable debug logging
log4j.logger.org.apache.flink.cdc=DEBUG
log4j.logger.io.debezium=DEBUG
```

---

## Getting Help

### Before Asking for Help

1. Check this troubleshooting guide
2. Review CONNECTIVITY_DIAGNOSIS.md
3. Check Flink CDC documentation
4. Search GitHub issues

### Information to Provide

When reporting an issue, include:

1. **Environment**:
   - Flink version
   - GaussDB version
   - Connector version
   - Java version

2. **Configuration**:
   - JDBC URL (mask sensitive data)
   - Connector properties
   - Table DDL

3. **Error Details**:
   - Full error message and stack trace
   - Flink job logs
   - GaussDB server logs (if accessible)

4. **Steps to Reproduce**:
   - Minimal reproducible example
   - Test data (if applicable)

### Contact

- Flink CDC Issues: https://github.com/apache/flink-cdc/issues
- Flink Mailing List: user@flink.apache.org
- GaussDB Support: Huawei Cloud support portal

---

**Document Version**: 1.0
**Last Updated**: 2025-12-17
**Maintained By**: GaussDB CDC Connector Team
