package org.apache.flink.cdc.composer.definition;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Objects;

public class UdfDef {
    private static final String PARAM_SEPARATOR = ":::";
    private static final String MODEL_UDF_CLASSPATH =
            "org.apache.flink.cdc.runtime.operators.model.ModelUdf";

    private final String name;
    private final String classpath;
    private final String serializedParams;

    public UdfDef(String name, String classpath) {
        this(name, classpath, null);
    }

    public UdfDef(String name, String classpath, String serializedParams) {
        this.name = name;
        this.classpath = classpath;
        this.serializedParams = serializedParams;
    }

    public String getName() {
        return name;
    }

    public String getClasspath() {
        return classpath;
    }

    public String getSerializedParams() {
        return serializedParams;
    }

    public Tuple2<String, String> toTuple2() {
        if (MODEL_UDF_CLASSPATH.equals(classpath) && serializedParams != null) {
            return Tuple2.of(encodeNameWithParams(name, serializedParams), classpath);
        } else {
            return Tuple2.of(name, classpath);
        }
    }

    private String encodeNameWithParams(String name, String params) {
        return name + PARAM_SEPARATOR + params;
    }

    public static UdfDef fromEncodedName(String encodedName, String classpath) {
        String[] parts = encodedName.split(PARAM_SEPARATOR, 2);
        if (parts.length > 1) {
            return new UdfDef(parts[0], classpath, parts[1]);
        } else {
            return new UdfDef(encodedName, classpath);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UdfDef udfDef = (UdfDef) o;
        return Objects.equals(name, udfDef.name)
                && Objects.equals(classpath, udfDef.classpath)
                && Objects.equals(serializedParams, udfDef.serializedParams);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, classpath, serializedParams);
    }

    @Override
    public String toString() {
        return "UdfDef{"
                + "name='"
                + name
                + '\''
                + ", classpath='"
                + classpath
                + '\''
                + ", serializedParams='"
                + serializedParams
                + '\''
                + '}';
    }
}
