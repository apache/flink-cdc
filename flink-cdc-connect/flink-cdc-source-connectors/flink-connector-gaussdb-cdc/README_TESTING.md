# GaussDB CDC Connector - Testing Guide

This guide provides comprehensive information about testing the Flink CDC connector for Huawei GaussDB.

---

## Quick Start

### Running Tests

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=GaussDBConnectivityTest

# Run specific test method
mvn test -Dtest=GaussDBDataTypeTest#testNumericTypes

# Run with debug output
mvn test -X
```

### Prerequisites

Before running tests, ensure:

1. ✅ GaussDB instance is accessible at 10.250.0.51:8000
2. ✅ Credentials are valid: tom / Gauss_235
3. ✅ Database 'db1' exists with schema 'public'
4. ✅ User 'tom' has REPLICATION permission
5. ✅ WAL level is set to 'logical'
6. ✅ mppdb_decoding plugin is installed

---

## Test Suite Overview

### Test Categories

| Category | Test Class | Tests | Purpose |
|----------|-----------|-------|---------|
| **Connection** | GaussDBConnectionTest | 3 | Unit tests with mocked connections |
| **Connectivity** | GaussDBConnectivityTest | 7 | Integration tests for JDBC connectivity |
| **Snapshot & CDC** | GaussDBSourceITCase | 3 | Basic snapshot and incremental reading |
| **Data Types** | GaussDBDataTypeTest | 5 | Data type conversion validation |
| **Boundaries** | GaussDBBoundaryConditionTest | 6 | Edge cases and boundary conditions |

**Total**: 24 tests across 5 test classes

### Test Status

- ✅ **Unit Tests**: 3/3 passing (using mocked connections)
- ⏸️ **Integration Tests**: 21 tests ready, awaiting connectivity resolution

---

## Test Documentation

### Detailed Guides

1. **[CONNECTIVITY_DIAGNOSIS.md](./CONNECTIVITY_DIAGNOSIS.md)**
   - Comprehensive analysis of connectivity issues
   - Root cause identification
   - Step-by-step resolution guide
   - Includes error logs and diagnostic commands

2. **[TEST_SUITE_SUMMARY.md](./TEST_SUITE_SUMMARY.md)**
   - Complete test suite overview
   - Test coverage matrix
   - Data type coverage details
   - Execution instructions

3. **[TROUBLESHOOTING_GUIDE.md](./TROUBLESHOOTING_GUIDE.md)**
   - Common issues and solutions
   - Error message reference
   - Diagnostic commands
   - Best practices

---

## Test Classes

### 1. GaussDBConnectionTest

**Location**: `src/test/java/io/debezium/connector/gaussdb/connection/`

**Purpose**: Unit tests for JDBC connection logic using mocked connections.

**Tests**:
- `connectsSuccessfullyAndPassesHealthCheck()` - Verifies connection establishment and health check
- `retriesConnectionThreeTimesAndThenThrows()` - Validates retry mechanism
- `managesConnectionPoolLifecycle()` - Tests HikariCP connection pool management

**Status**: ✅ All passing

**Run**:
```bash
mvn test -Dtest=GaussDBConnectionTest
```

---

### 2. GaussDBConnectivityTest ⭐ NEW

**Location**: `src/test/java/org/apache/flink/cdc/connectors/gaussdb/connection/`

**Purpose**: Comprehensive connectivity verification with real database.

**Tests**:
1. `testPostgreSQLDriverConnection()` - Tests PostgreSQL-compatible JDBC driver
2. `testGaussDBNativeDriverConnection()` - Tests native GaussDB JDBC driver
3. `testGaussDBDriverWithSSLDisabled()` - Verifies SSL configuration handling
4. `testBasicDMLOperations()` - Validates CREATE/INSERT/UPDATE/DELETE operations
5. `testReplicationSlotManagement()` - Tests replication slot lifecycle
6. `testConnectionProperties()` - Verifies database metadata retrieval
7. `testSchemaAndTableMetadata()` - Tests schema introspection capabilities

**Status**: ⏸️ Blocked by connectivity issues

**Run**:
```bash
mvn test -Dtest=GaussDBConnectivityTest
```

**Expected Output** (when connectivity works):
```
[INFO] GaussDBConnectivityTest - Testing PostgreSQL driver connection
[INFO] GaussDBConnectivityTest - Connected using PostgreSQL driver - Database: GaussDB, Version: x.x.x
[INFO] GaussDBConnectivityTest - PostgreSQL driver connection test PASSED
```

---

### 3. GaussDBSourceITCase

**Location**: `src/test/java/org/apache/flink/cdc/connectors/gaussdb/source/`

**Purpose**: Integration tests for snapshot and incremental CDC reading.

**Tests**:
1. `testReadSingleTableWithSingleParallelism()` - Snapshot reading with parallelism=1
2. `testReadSingleTableWithMultipleParallelism()` - Snapshot reading with parallelism=4
3. `testInsertUpdateDelete()` - CDC capture of INSERT/UPDATE/DELETE operations

**Status**: ⏸️ Blocked by connectivity issues

**Features Tested**:
- Initial snapshot reading
- Incremental snapshot with chunking
- CDC event capture (INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE)
- Row kind validation
- Replication slot management

**Run**:
```bash
mvn test -Dtest=GaussDBSourceITCase
```

---

### 4. GaussDBDataTypeTest ⭐ NEW

**Location**: `src/test/java/org/apache/flink/cdc/connectors/gaussdb/source/`

**Purpose**: Validate data type conversion from GaussDB to Flink.

**Tests**:
1. `testNumericTypes()` - SMALLINT, INTEGER, BIGINT, NUMERIC, REAL, DOUBLE
2. `testCharacterTypes()` - CHAR, VARCHAR, TEXT
3. `testTemporalTypes()` - DATE, TIME, TIMESTAMP, TIMESTAMPTZ
4. `testBooleanType()` - BOOLEAN with true/false/null
5. `testNullValues()` - NULL handling across different types

**Status**: ⏸️ Blocked by connectivity issues

**Coverage**:
- 14 SQL data types
- Type conversion accuracy
- NULL value handling
- Precision and scale preservation

**Run**:
```bash
mvn test -Dtest=GaussDBDataTypeTest
```

---

### 5. GaussDBBoundaryConditionTest ⭐ NEW

**Location**: `src/test/java/org/apache/flink/cdc/connectors/gaussdb/source/`

**Purpose**: Test edge cases and boundary conditions.

**Tests**:
1. `testEmptyTableSnapshot()` - Handling of empty tables
2. `testEmptyStrings()` - Empty string vs NULL distinction
3. `testMaxLengthStrings()` - VARCHAR(1000) maximum length
4. `testNumericBoundaries()` - MIN_VALUE and MAX_VALUE for all numeric types
5. `testSpecialCharacters()` - Special characters and Unicode support
6. `testLargeBatchInsert()` - Bulk insert of 1000 rows with chunking

**Status**: ⏸️ Blocked by connectivity issues

**Coverage**:
- Empty data scenarios
- String length boundaries
- Numeric type boundaries (Short.MIN_VALUE to Long.MAX_VALUE)
- Special characters: `!@#$%^&*()_+-=[]{}|;:',.<>?/~``
- Unicode: Chinese characters (中文), emojis (😀)
- Large datasets: 1000+ rows

**Run**:
```bash
mvn test -Dtest=GaussDBBoundaryConditionTest
```

---

## Resolving Connectivity Issues

### Current Problem

All integration tests fail with:
```
java.io.EOFException
	at org.postgresql.core.PGStream.receiveChar(PGStream.java:469)
	at org.postgresql.core.v3.ConnectionFactoryImpl.doAuthentication(ConnectionFactoryImpl.java:683)
```

### Quick Diagnostics

1. **Test Network Connectivity**:
   ```bash
   telnet 10.250.0.51 8000
   # Expected: Connected to 10.250.0.51
   ```

2. **Test Database Login**:
   ```bash
   psql -h 10.250.0.51 -p 8000 -U tom -d db1
   # Enter password: Gauss_235
   ```

3. **Check Firewall**:
   ```bash
   # On client machine
   sudo iptables -L -n | grep 8000

   # On GaussDB server
   netstat -tuln | grep 8000
   ```

4. **Verify GaussDB Status**:
   ```bash
   # On GaussDB server
   gs_ctl status -D /path/to/data
   ```

### Resolution Steps

See **[CONNECTIVITY_DIAGNOSIS.md](./CONNECTIVITY_DIAGNOSIS.md)** for:
- Detailed error analysis
- Root cause identification
- Step-by-step resolution procedures
- Alternative configuration options

---

## Test Configuration

### Database Connection

Tests use these connection parameters (defined in `GaussDBTestBase.java`):

```java
protected static final String HOSTNAME = "10.250.0.51";
protected static final int PORT = 8000;
protected static final String USERNAME = "tom";
protected static final String PASSWORD = "Gauss_235";
protected static final String DATABASE_NAME = "db1";
protected static final String SCHEMA_NAME = "public";
```

### JDBC Drivers

Two drivers are used:

1. **PostgreSQL Driver** (for test data preparation):
   - Artifact: `org.postgresql:postgresql:42.7.3`
   - URL: `jdbc:postgresql://10.250.0.51:8000/db1?sslmode=disable`
   - Usage: DDL and DML for test setup

2. **GaussDB Driver** (for CDC connector):
   - File: `lib/gaussdbjdbc.jar` (1.1MB)
   - Class: `com.huawei.gaussdb.jdbc.Driver`
   - URL: `jdbc:gaussdb://10.250.0.51:8000/db1`
   - Usage: Replication and CDC operations

### Connector Configuration

Example Flink SQL DDL:

```sql
CREATE TABLE customers (
  id BIGINT NOT NULL,
  name STRING,
  city STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'gaussdb-cdc',
  'scan.incremental.snapshot.enabled' = 'true',
  'hostname' = '10.250.0.51',
  'port' = '8000',
  'username' = 'tom',
  'password' = 'Gauss_235',
  'database-name' = 'db1',
  'schema-name' = 'public',
  'table-name' = 'customers',
  'scan.startup.mode' = 'initial',
  'scan.incremental.snapshot.chunk.size' = '3',
  'decoding.plugin.name' = 'mppdb_decoding',
  'slot.name' = 'flink_1234'
);
```

---

## Test Best Practices

### Writing New Tests

1. **Isolation**: Each test should use unique table and slot names
   ```java
   private String tableName = "test_" + System.currentTimeMillis();
   private String slotName = getSlotName(); // Generates random ID
   ```

2. **Cleanup**: Always clean up resources in `@AfterEach`
   ```java
   @AfterEach
   void afterEach() throws Exception {
       dropTestTableIfExists();
       dropReplicationSlotIfExists(slotName);
       Thread.sleep(1000L); // Wait for connections to close
   }
   ```

3. **Timeouts**: Set reasonable timeouts
   ```java
   @Timeout(value = 300, unit = TimeUnit.SECONDS)
   class MyTest { ... }
   ```

4. **Assertions**: Use AssertJ for readable assertions
   ```java
   assertThat(rows).hasSize(10);
   assertThat(row.getField("name")).isEqualTo("expected");
   ```

### Running Tests Efficiently

1. **Run Unit Tests Only** (no database required):
   ```bash
   mvn test -Dtest=GaussDBConnectionTest
   ```

2. **Run Specific Integration Test**:
   ```bash
   mvn test -Dtest=GaussDBConnectivityTest#testPostgreSQLDriverConnection
   ```

3. **Parallel Execution** (when connectivity works):
   ```bash
   mvn test -DforkCount=4
   ```

4. **Skip Tests**:
   ```bash
   mvn install -DskipTests
   ```

---

## Continuous Integration

### GitHub Actions Workflow (Example)

```yaml
name: GaussDB CDC Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      gaussdb:
        image: gaussdb-docker-image
        ports:
          - 8000:8000
        env:
          GAUSSDB_PASSWORD: Gauss_235
    steps:
      - uses: actions/checkout@v2
      - name: Set up JDK 11
        uses: actions/setup-java@v2
        with:
          java-version: '11'
      - name: Run Tests
        run: mvn test
```

---

## Performance Benchmarking

### Metrics to Track

1. **Snapshot Performance**:
   - Time to read 10,000 rows
   - Memory usage during snapshot
   - Throughput (rows/second)

2. **CDC Performance**:
   - Replication lag
   - Event processing rate
   - Checkpoint duration

3. **Resource Usage**:
   - CPU utilization
   - Memory footprint
   - Network bandwidth

### Benchmark Tests (Future)

```bash
# Run performance tests (when implemented)
mvn test -Dtest=GaussDBPerformanceTest
```

---

## Frequently Asked Questions

### Q: Why do all integration tests fail?

**A**: Currently, there are network/authentication issues connecting to the GaussDB instance at 10.250.0.51:8000. See [CONNECTIVITY_DIAGNOSIS.md](./CONNECTIVITY_DIAGNOSIS.md) for details.

### Q: Can I run tests without a real GaussDB instance?

**A**: Yes, run unit tests only:
```bash
mvn test -Dtest=GaussDBConnectionTest
```
These use mocked connections and don't require a database.

### Q: How do I test against a different GaussDB instance?

**A**: Modify connection parameters in `GaussDBTestBase.java`:
```java
protected static final String HOSTNAME = "your-host";
protected static final int PORT = your-port;
protected static final String USERNAME = "your-user";
protected static final String PASSWORD = "your-password";
```

### Q: What if I get "replication slot already exists"?

**A**: Drop the slot manually:
```sql
SELECT pg_drop_replication_slot('slot_name');
```
Or use the cleanup helper in tests:
```java
dropReplicationSlotIfExists(slotName);
```

### Q: How do I enable debug logging?

**A**: Add to your test method:
```java
System.setProperty("com.huawei.gaussdb.jdbc.loggerLevel", "TRACE");
```

---

## Contributing

### Adding New Tests

1. Follow existing test patterns (see GaussDBDataTypeTest as example)
2. Use descriptive test method names: `testFeatureScenario()`
3. Add proper documentation in TEST_SUITE_SUMMARY.md
4. Ensure proper cleanup in `@AfterEach`
5. Run `mvn spotless:apply` to format code

### Reporting Issues

When reporting test failures:

1. Run test with `-X` flag for debug output
2. Include full stack trace
3. Provide GaussDB version and configuration
4. Attach relevant logs from `target/surefire-reports/`

---

## Resources

### Official Documentation

- [Flink CDC Documentation](https://nightlies.apache.org/flink/flink-cdc-docs-stable/)
- [GaussDB JDBC Documentation](https://doc.hcs.huawei.com/db/en-us/gaussdb/24.1.30/usermanual/gaussdb_01_074.html)
- [Debezium Documentation](https://debezium.io/documentation/)

### Reference Implementations

- MySQL Connector Tests: `flink-connector-mysql-cdc/src/test/`
- PostgreSQL Connector Tests: `flink-connector-postgres-cdc/src/test/`

### Support

- GitHub Issues: https://github.com/apache/flink-cdc/issues
- Mailing List: user@flink.apache.org

---

**Document Version**: 1.0
**Last Updated**: 2025-12-17
**Status**: Tests ready, awaiting connectivity resolution

For immediate assistance, see [TROUBLESHOOTING_GUIDE.md](./TROUBLESHOOTING_GUIDE.md).
