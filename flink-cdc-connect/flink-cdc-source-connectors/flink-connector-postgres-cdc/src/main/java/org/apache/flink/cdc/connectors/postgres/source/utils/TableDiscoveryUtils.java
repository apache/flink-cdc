package org.apache.flink.cdc.connectors.postgres.source.utils;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/** A utility class for table discovery. */
public class TableDiscoveryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(TableDiscoveryUtils.class);

    /**
     * Lists all tables from the given databases.
     *
     * @param jdbc PostgresConnection to interact with the database
     * @param databases Varargs list of database names
     * @return Unmodifiable list of all TableId objects
     * @throws SQLException If an SQL error occurs during retrieval
     */
    public static List<TableId> listTables(PostgresConnection jdbc, String... databases)
            throws SQLException {
        List<TableId> tableIds = new ArrayList<>();
        for (String database : databases) {
            // Directly adding all table ids from the specified database
            tableIds.addAll(
                    listTables(database, jdbc, null)); // Using jdbc directly without reconnecting
        }
        return Collections.unmodifiableList(tableIds); // Returning an unmodifiable list
    }

    /**
     * Lists tables based on database, filtering by table filters.
     *
     * @param database The database to list tables from
     * @param jdbc JdbcConnection to interact with the database
     * @param tableFilters The relational table filters to apply
     * @return Unmodifiable set of captured TableIds after applying filters
     * @throws SQLException If an SQL error occurs during retrieval
     */
    public static Set<TableId> listTables(
            String database, JdbcConnection jdbc, @Nullable RelationalTableFilters tableFilters)
            throws SQLException {

        // Retrieve all table ids, filtering by TABLE and PARTITIONED TABLE
        Set<TableId> allTableIds =
                jdbc.readTableNames(
                        database, null, null, new String[] {"TABLE", "PARTITIONED TABLE"});

        // Filter tables based on the data collection filter, if provided, otherwise return all
        // tables
        Set<TableId> capturedTables =
                (tableFilters == null)
                        ? new TreeSet<>(allTableIds) // Automatically sorts the tables
                        : allTableIds.stream()
                                .filter(t -> tableFilters.dataCollectionFilter().isIncluded(t))
                                .collect(
                                        Collectors.toCollection(
                                                TreeSet::new)); // Ensure sorted order

        // Log the captured tables or warn if none were captured
        if (capturedTables.isEmpty()) {
            LOG.warn("No tables captured. Please check the table filters.");
        } else {
            LOG.debug(
                    "Postgres captured tables ({}): {}",
                    capturedTables.size(),
                    capturedTables.stream()
                            .map(TableId::toString)
                            .collect(Collectors.joining(", ")));
        }

        return Collections.unmodifiableSet(capturedTables); // Return an unmodifiable set
    }
}
