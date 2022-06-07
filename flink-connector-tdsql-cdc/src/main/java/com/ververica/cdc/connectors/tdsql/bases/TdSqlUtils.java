package com.ververica.cdc.connectors.tdsql.bases;

import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;
import io.debezium.jdbc.JdbcConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Utilities related to TdSql. */
public class TdSqlUtils {
    private static final Logger LOG = LoggerFactory.getLogger(TdSqlUtils.class);

    private static final String SET_KEY = "set";

    public static List<TdSqlSet> discoverSets(JdbcConnection jdbc) {
        final String showStatusStmts = "/*proxy*/SHOW STATUS";

        try {
            return jdbc.queryAndMap(
                    showStatusStmts,
                    rs -> {
                        Map<String, String> tdsqlStatus = new HashMap<>(rs.getFetchSize());
                        while (rs.next()) {
                            tdsqlStatus.put(rs.getString(1), rs.getString(2));
                        }
                        Optional<String[]> sets =
                                Optional.ofNullable(tdsqlStatus.get(SET_KEY))
                                        .map(v -> v.split(","));
                        if (!sets.isPresent()) {
                            throw new FlinkRuntimeException(
                                    "Cannot found set infos via '"
                                            + showStatusStmts
                                            + "', Make sure your server is correctly configured");
                        }

                        String[] setKeys = sets.get();
                        List<TdSqlSet> res = new ArrayList<>(setKeys.length);

                        for (String setKey : setKeys) {
                            String value = tdsqlStatus.get(setKey);
                            int setMasterIPSplitIndex = value.indexOf(";");
                            int setHostIndex = value.indexOf(":");

                            if (setMasterIPSplitIndex == -1 || setHostIndex == -1) {
                                throw new FlinkRuntimeException(
                                        "Cannot found set '"
                                                + setKey
                                                + "' info. all status is: "
                                                + asString(tdsqlStatus)
                                                + ", please check.");
                            }

                            String host = value.substring(0, setHostIndex);
                            int port =
                                    Integer.parseInt(
                                            value.substring(
                                                    setHostIndex + 1, setMasterIPSplitIndex));

                            res.add(new TdSqlSet(setKey, host, port));
                        }
                        return res;
                    });
        } catch (SQLException e) {
            throw new RuntimeException(
                    "Cannot found set infos via '"
                            + showStatusStmts
                            + "', Make sure your server is correctly configured",
                    e);
        }
    }

    private static <K, V> String asString(Map<K, V> map) {
        return map.entrySet().stream()
                .map(kv -> kv.getKey() + "=" + kv.getValue())
                .collect(Collectors.joining(","));
    }
}
