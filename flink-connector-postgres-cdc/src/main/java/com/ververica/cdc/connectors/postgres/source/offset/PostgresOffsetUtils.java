package com.ververica.cdc.connectors.postgres.source.offset;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import io.debezium.connector.postgresql.PostgresOffsetContext;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Utils for handling {@link PostgresOffset} */
public class PostgresOffsetUtils {
    public static PostgresOffsetContext getPostgresOffsetContext(
            PostgresOffsetContext.Loader loader, Offset offset) {

        Map<String, String> offsetStrMap =
                Objects.requireNonNull(offset, "offset is null for the sourceSplitBase")
                        .getOffset();
        // all the keys happen to be long type for PostgresOffsetContext.Loader.load
        Map<String, Object> offsetMap =
                offsetStrMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, e -> Long.parseLong(e.getValue())));
        return loader.load(offsetMap);
    }
}
