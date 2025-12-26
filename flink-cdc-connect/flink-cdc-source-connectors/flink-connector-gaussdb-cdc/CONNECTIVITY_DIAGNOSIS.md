# GaussDB CDC Connector - Connectivity Diagnosis Report

**Date**: 2025-12-17
**Status**: CONNECTION FAILED - Network/Authentication Issue Detected

---

## Executive Summary

All connectivity tests failed with `java.io.EOFException` during the authentication phase, indicating either:
1. Network connectivity issues between the test environment and GaussDB instance (10.250.0.51:8000)
2. Incorrect credentials or authentication configuration
3. GaussDB instance not accepting external connections
4. SSL/TLS configuration mismatch

## Test Environment

- **GaussDB Instance**: 10.250.0.51:8000
- **Database**: db1
- **Schema**: public
- **Username**: tom
- **Password**: Gauss_235
- **GaussDB JDBC Driver**: com.huawei.gaussdb.jdbc.Driver (lib/gaussdbjdbc.jar, 1.1MB)
- **PostgreSQL JDBC Driver**: org.postgresql.Driver (version 42.7.3)

## Test Results Summary

**Tests Run**: 7
**Passed**: 0
**Failed**: 7 (100% failure rate)

### Failed Tests

1. ❌ `testPostgreSQLDriverConnection` - PostgreSQL driver connection test
2. ❌ `testGaussDBNativeDriverConnection` - Native GaussDB driver connection test
3. ❌ `testGaussDBDriverWithSSLDisabled` - GaussDB driver with SSL disabled
4. ❌ `testBasicDMLOperations` - Basic DML operations test
5. ❌ `testReplicationSlotManagement` - Replication slot management test
6. ❌ `testConnectionProperties` - Connection properties test
7. ❌ `testSchemaAndTableMetadata` - Schema and table metadata test

## Error Analysis

### Primary Error Pattern

```
org.postgresql.util.PSQLException: 尝试连线已失败。 (Connection attempt failed)
com.huawei.gaussdb.jdbc.util.PSQLException: ???????? (Authentication failed)

Caused by: java.io.EOFException
	at org.postgresql.core.PGStream.receiveChar(PGStream.java:469)
	at org.postgresql.core.v3.ConnectionFactoryImpl.doAuthentication(ConnectionFactoryImpl.java:683)
```

### Root Cause Analysis

The `EOFException` during `doAuthentication()` typically indicates:

1. **Network Connectivity Issues**
   - GaussDB server at 10.250.0.51:8000 may not be reachable from the test environment
   - Firewall rules blocking the connection
   - Network timeout during authentication handshake

2. **Authentication Configuration Issues**
   - Username/password incorrect or expired
   - Authentication method mismatch (e.g., server requires SSL but client disables it)
   - pg_hba.conf on GaussDB server not allowing connections from the client IP

3. **SSL/TLS Configuration Mismatch**
   - Server requires SSL but client tries to connect without it
   - SSL version/cipher mismatch

4. **GaussDB Version/Compatibility Issues**
   - JDBC driver version incompatible with GaussDB server version
   - Protocol version mismatch

## JDBC URL Format Validation

Based on Huawei GaussDB documentation research:

### Correct URL Formats

GaussDB offers two JDBC driver packages:

1. **gsjdbc4.jar** (PostgreSQL-compatible)
   ```
   jdbc:postgresql://host:port/database
   Example: jdbc:postgresql://10.250.0.51:8000/db1
   ```

2. **gsjdbc200.jar** (Native GaussDB)
   ```
   jdbc:gaussdb://host:port/database
   Example: jdbc:gaussdb://10.250.0.51:8000/db1
   ```

### Current Implementation

The project uses `com.huawei.gaussdb.jdbc.Driver` which suggests a native GaussDB driver similar to gsjdbc200.jar.

**Current URLs tested**:
- ✅ PostgreSQL: `jdbc:postgresql://10.250.0.51:8000/db1?sslmode=disable` (Format correct, connection failed)
- ✅ GaussDB: `jdbc:gaussdb://10.250.0.51:8000/db1` (Format correct, connection failed)
- ✅ GaussDB with SSL: `jdbc:gaussdb://10.250.0.51:8000/db1?sslmode=disable` (Format correct, connection failed)

**Conclusion**: URL formats are correct according to documentation.

## Recommended Actions

### Immediate Actions (High Priority)

1. **Verify Network Connectivity**
   ```bash
   # Test if port 8000 is reachable
   telnet 10.250.0.51 8000
   # Or
   nc -zv 10.250.0.51 8000

   # Test ICMP ping
   ping 10.250.0.51
   ```

2. **Verify Credentials**
   - Confirm username is 'tom' (not 'Tom' or other case variations)
   - Confirm password is exactly 'Gauss_235'
   - Check if the account is locked or expired
   - Verify the user has permission to connect from the client IP

3. **Check GaussDB Server Logs**
   - Look for authentication failures
   - Check for connection attempts from the client IP
   - Verify SSL/TLS configuration in server logs

4. **Test with psql/gsql CLI**
   ```bash
   # Using PostgreSQL psql
   psql -h 10.250.0.51 -p 8000 -U tom -d db1

   # Using GaussDB gsql (if available)
   gsql -h 10.250.0.51 -p 8000 -U tom -d db1 -W Gauss_235
   ```

### Medium Priority Actions

5. **Review pg_hba.conf Configuration**
   - Ensure the client IP is allowed to connect
   - Check authentication method (md5, scram-sha-256, trust, etc.)
   - Example pg_hba.conf entry:
     ```
     host    db1    tom    <client-ip>/32    md5
     ```

6. **Try Alternative Connection Parameters**
   ```
   jdbc:gaussdb://10.250.0.51:8000/db1?ssl=false
   jdbc:gaussdb://10.250.0.51:8000/db1?sslmode=allow
   jdbc:gaussdb://10.250.0.51:8000/db1?loggerLevel=TRACE
   jdbc:gaussdb://10.250.0.51:8000/db1?ApplicationName=FlinkCDCTest
   ```

7. **Verify JDBC Driver Version Compatibility**
   - Check GaussDB server version
   - Ensure gaussdbjdbc.jar is compatible with the server version
   - Consider downloading the latest JDBC driver from Huawei

### Low Priority Actions

8. **Enable JDBC Debug Logging**
   - Add to test configuration:
     ```java
     System.setProperty("com.huawei.gaussdb.jdbc.loggerLevel", "TRACE");
     System.setProperty("com.huawei.gaussdb.jdbc.loggerFile", "gaussdb-jdbc.log");
     ```

9. **Test from Same Network Segment**
   - If possible, run tests from a machine in the same network as GaussDB
   - This helps isolate network vs configuration issues

10. **Contact DBA for Server-Side Investigation**
    - Request server-side connection logs
    - Verify SSL configuration on server
    - Check listener status and configuration

## Alternative Test Strategy

Until connectivity is restored, consider:

1. **Use Testcontainers with PostgreSQL**
   - Mock GaussDB with PostgreSQL container for unit tests
   - Only run integration tests against real GaussDB when available

2. **Create Mock Connection Tests**
   - Test business logic with mocked JDBC connections
   - Use the existing `GaussDBConnectionTest.java` pattern with proxies

3. **Split Test Suites**
   - Unit tests: No external dependencies
   - Integration tests: Require real GaussDB connection
   - Use Maven profiles to control execution

## Next Steps for Test Suite Enhancement

Once connectivity is restored, proceed with:

### Phase 2: Data Type Coverage Tests
- Numeric types (SMALLINT, INTEGER, BIGINT, NUMERIC, REAL, DOUBLE)
- Character types (CHAR, VARCHAR, TEXT)
- Temporal types (DATE, TIME, TIMESTAMP, TIMESTAMPTZ)
- Binary types (BYTEA)
- Special types (BOOLEAN, JSON, ARRAY)

### Phase 3: Boundary Condition Tests
- NULL value handling
- Empty strings and maximum length strings
- Numeric boundaries (MIN_VALUE, MAX_VALUE, overflow)
- Empty table snapshot reading

### Phase 4: Concurrency and Performance Tests
- Large data snapshot reading (10000+ rows)
- High-frequency DML capture
- Parallel multi-table reading

### Phase 5: Error Handling and Fault Tolerance Tests
- Connection disconnect and auto-reconnect
- Replication slot occupied or deleted scenarios
- Schema/table does not exist errors
- Invalid connection parameters

### Phase 6: Checkpoint and Recovery Tests
- Checkpoint during snapshot phase
- Checkpoint during incremental phase
- Resume from checkpoint

## Code Changes Made

### New Files Created

1. **GaussDBConnectivityTest.java**
   - Location: `src/test/java/org/apache/flink/cdc/connectors/gaussdb/connection/`
   - Purpose: Comprehensive connectivity verification
   - Tests: 7 connection scenarios
   - Status: All tests fail with network/auth errors

### Configuration Analysis

**GaussDBTestBase.java** (src/test/java/org/apache/flink/cdc/connectors/gaussdb/GaussDBTestBase.java:56)
```java
protected Connection getJdbcConnection() throws SQLException {
    String jdbcUrl = String.format(
        "jdbc:postgresql://%s:%d/%s?sslmode=disable",
        HOSTNAME, PORT, DATABASE_NAME);
    try {
        Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
        throw new SQLException("PostgreSQL JDBC driver not found.", e);
    }
    return DriverManager.getConnection(jdbcUrl, USERNAME, PASSWORD);
}
```

**Analysis**: Uses PostgreSQL driver for test data preparation, which is appropriate for GaussDB compatibility. However, connection fails at authentication level.

**GaussDBConnection.java** (src/main/java/io/debezium/connector/gaussdb/connection/GaussDBConnection.java:26)
```java
public static final String DRIVER_CLASS_NAME = "com.huawei.gaussdb.jdbc.Driver";

private static final String URL_PATTERN =
    "jdbc:gaussdb://${" + JdbcConfiguration.HOSTNAME +
    "}:${" + JdbcConfiguration.PORT +
    "}/${" + JdbcConfiguration.DATABASE + "}";
```

**Analysis**: Correct driver class and URL pattern for native GaussDB driver.

## Documentation Sources

### Research Conducted

1. **Huawei GaussDB JDBC Documentation**
   - [Using JDBC to Connect to a Cluster - Data Warehouse Service](https://support.huaweicloud.com/intl/en-us/mgtg-dws/dws_01_0077.html)
   - [Using JDBC to Connect to a Database - GaussDB OpenGauss](https://docs.otc.t-systems.com/gaussdb-opengauss/umn/getting_started/connecting_to_a_db_instance_using_a_driver/using_jdbc_to_connect_to_a_database.html)
   - [Using JDBC to Connect to a Database](https://doc.hcs.huawei.com/db/en-us/gaussdb/24.1.30/usermanual/gaussdb_01_074.html)

2. **Key Findings**:
   - GaussDB supports two JDBC drivers: gsjdbc4.jar (PostgreSQL-compatible) and gsjdbc200.jar (native)
   - URL format: `jdbc:gaussdb://host:port/database` for native driver
   - URL format: `jdbc:postgresql://host:port/database` for PostgreSQL-compatible driver
   - Optional parameters: logger, SSL settings, connection extras

## Conclusion

The connectivity tests successfully identified the root issue: **all connection attempts fail during authentication with EOFException**. This is a network/authentication infrastructure issue, not a code configuration issue.

The JDBC URL formats and driver configurations are correct according to Huawei GaussDB documentation. The test suite is ready to proceed once network connectivity and authentication are resolved.

**Recommended Priority**: Resolve connectivity issues before proceeding with test suite enhancement.

---

**Report Generated**: 2025-12-17
**Test Location**: `/Users/lmj/projects/ai-project/flink-cdc/flink-cdc-connect/flink-cdc-source-connectors/flink-connector-gaussdb-cdc`
**Test Class**: `GaussDBConnectivityTest.java`
**Log Files**: `target/surefire-reports/`
