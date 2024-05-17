package org.apache.flink.cdc.connectors.oceanbase.catalog;

import com.google.common.collect.Lists;
import com.oceanbase.connector.flink.OceanBaseConnectorOptions;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.junit.Ignore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Tests for {@link OceanBaseMySQLCatalogTest}. */
@Ignore
public class OceanBaseMySQLCatalogTest {

    public static final ImmutableMap<String, String> configMap = ImmutableMap.<String, String>builder()
            .put("url", System.getenv("url"))
            .put("username", System.getenv("username"))
            .put("password", System.getenv("password"))
            .build();

    @Test
    public void testBuildAlterAddColumnsSql() {
        OceanBaseMySQLCatalog oceanBaseCatalog = new OceanBaseMySQLCatalog(new OceanBaseConnectorOptions(configMap));

        List<OceanBaseColumn> addColumns = Lists.newArrayList();
        addColumns.add(new OceanBaseColumn.Builder()
                .setColumnName("age")
                .setOrdinalPosition(-1)
                .setColumnComment("age")
                .setDataType("varchar(10)")
                .build());
        String columnsSql = oceanBaseCatalog.buildAlterAddColumnsSql("test", "test", addColumns);
        Assertions.assertEquals("ALTER TABLE `test`.`test` ADD COLUMN `age` VARCHAR(10) NULL COMMENT \"age\";", columnsSql);
    }
}
