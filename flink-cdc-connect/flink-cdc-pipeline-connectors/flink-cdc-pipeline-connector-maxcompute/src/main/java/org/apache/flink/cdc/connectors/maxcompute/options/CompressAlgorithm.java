package org.apache.flink.cdc.connectors.maxcompute.options;

/** Compress algorithm for MaxCompute table. */
public enum CompressAlgorithm {
    /** No compress. */
    RAW("raw"),

    /** Zlib compress. */
    ZLIB("zlib"),

    /** LZ4 compress. */
    LZ4("lz4"),

    /** Snappy compress. */
    @Deprecated
    SNAPPY("snappy");

    private final String value;

    CompressAlgorithm(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
