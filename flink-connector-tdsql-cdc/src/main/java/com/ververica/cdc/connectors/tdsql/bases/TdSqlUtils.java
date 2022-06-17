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
    private static final String IP_KEY_SUFFIX = ":ip";

    public static List<TdSqlSet> discoverSets(JdbcConnection jdbc) {
        final String showStatusStmts = "/*proxy*/show status";

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

                        LOG.trace("configs {}", asString(tdsqlStatus));

                        String[] setKeys = sets.get();
                        List<TdSqlSet> res = new ArrayList<>(setKeys.length);

                        for (String setKey : setKeys) {
                            String setIpKey = setKey.trim() + IP_KEY_SUFFIX;
                            String value = tdsqlStatus.get(setIpKey);
                            LOG.trace("get set ip {} by key {}", value, setIpKey);

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

                            TdSqlSet set = new TdSqlSet(setKey.trim(), host, port);
                            LOG.info("add set: {}", set);
                            res.add(set);
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
