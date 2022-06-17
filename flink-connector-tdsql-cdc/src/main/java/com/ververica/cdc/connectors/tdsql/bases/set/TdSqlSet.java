package com.ververica.cdc.connectors.tdsql.bases.set;

import java.io.Serializable;
import java.util.Objects;

/** tdsql set info. */
public class TdSqlSet implements Serializable {
    private static final long serialVersionUID = -3395810101971522360L;
    private String setKey;

    private String host;

    private int port;

    public TdSqlSet() {}

    public TdSqlSet(String setId, String host, int port) {
        this.setKey = setId;
        this.host = host;
        this.port = port;
    }

    public String getSetKey() {
        return setKey;
    }

    public void setSetKey(String setKey) {
        this.setKey = setKey;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TdSqlSet)) {
            return false;
        }
        TdSqlSet tdSqlSet = (TdSqlSet) o;
        return port == tdSqlSet.port
                && setKey.equals(tdSqlSet.setKey)
                && host.equals(tdSqlSet.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(setKey, host, port);
    }

    @Override
    public String toString() {
        return "TdSqlSet{"
                + "setKey='"
                + setKey
                + '\''
                + ", host='"
                + host
                + '\''
                + ", port="
                + port
                + '}';
    }
}
