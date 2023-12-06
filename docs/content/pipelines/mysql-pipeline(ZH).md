# MySQL CDC Pipeline 连接器

MySQL CDC Pipeline 连接器允许从 MySQL 数据库读取快照数据和增量数据，并提供端到端的整库数据同步能力。
本文描述了如何设置 MySQL CDC Pipeline 连接器。


如何创建 Pipeline
----------------

从 MySQL 读取数据同步到 Doris 的 Pipeline 可以定义如下：

```yaml
source:
   type: mysql
   name: MySQL Source
   hostname: 127.0.0.1
   port: 3306
   username: admin
   password: pass
   tables: adb.\.*, bdb.user_table_[0-9]+, [app|web].order_\.*
   server-id: 5401-5404

sink:
  type: doris
  name: Doris Sink
  fenodes: 127.0.0.1:8030
  username: root
  password: pass

pipeline:
   name: MySQL to Doris Pipeline
   parallelism: 4
```

Pipeline 连接器选项
----------------

<div class="highlight">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width: 10%">Option</th>
        <th class="text-left" style="width: 8%">Required</th>
        <th class="text-left" style="width: 7%">Default</th>
        <th class="text-left" style="width: 10%">Type</th>
        <th class="text-left" style="width: 65%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>hostname</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td> MySQL 数据库服务器的 IP 地址或主机名。</td>
    </tr>
    <tr>
      <td>port</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3306</td>
      <td>Integer</td>
      <td>MySQL 数据库服务器的整数端口号。</td>
    </tr>
    <tr>
      <td>username</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接到 MySQL 数据库服务器时要使用的 MySQL 用户的名称。</td>
    </tr>
    <tr>
      <td>password</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>连接 MySQL 数据库服务器时使用的密码。</td>
    </tr>
    <tr>
      <td>tables</td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>需要监视的 MySQL 数据库的表名。表名支持正则表达式，以监视满足正则表达式的多个表。<br>
          需要注意的是，点号（.）被视为数据库和表名的分隔符。 如果需要在正则表达式中使用点（.）来匹配任何字符，必须使用反斜杠对点进行转义。<br>
          例如，db0.\.*, db1.user_table_[0-9]+, db[1-2].[app|web]order_\.*</td>
    </tr>
    <tr>
      <td>schema-change.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否发送模式更改事件，下游 sink 可以响应模式变更事件实现表结构同步，默认为true。</td>
    </tr>
    <tr>
      <td>server-id</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>读取数据使用的 server id，server id 可以是个整数或者一个整数范围，比如 '5400' 或 '5400-5408', 
      建议在 'scan.incremental.snapshot.enabled' 参数为启用时，配置成整数范围。因为在当前 MySQL 集群中运行的所有 slave 节点，标记每个 salve 节点的 id 都必须是唯一的。 所以当连接器加入 MySQL 集群作为另一个 slave 节点（并且具有唯一 id 的情况下），它就可以读取 binlog。 默认情况下，连接器会在 5400 和 6400 之间生成一个随机数，但是我们建议用户明确指定 Server id。
      </td>
    </tr>
    <tr>
      <td>scan.incremental.snapshot.chunk.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">8096</td>
      <td>Integer</td>
      <td>表快照的块大小（行数），读取表的快照时，捕获的表被拆分为多个块。</td>
    </tr>
    <tr>
      <td>scan.snapshot.fetch.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">1024</td>
      <td>Integer</td>
      <td>读取表快照时每次读取数据的最大条数。</td>
    </tr>
    <tr>
      <td>scan.startup.mode</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">initial</td>
      <td>String</td>
      <td> MySQL CDC 消费者可选的启动模式，
         合法的模式为 "initial"，"earliest-offset"，"latest-offset"，"specific-offset" 和 "timestamp"。
           请查阅 <a href="#a-name-id-002-a">启动模式</a> 章节了解更多详细信息。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.file</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>在 "specific-offset" 启动模式下，启动位点的 binlog 文件名。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.pos</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>在 "specific-offset" 启动模式下，启动位点的 binlog 文件位置。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.gtid-set</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>在 "specific-offset" 启动模式下，启动位点的 GTID 集合。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.skip-events</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>在指定的启动位点后需要跳过的事件数量。</td>
    </tr>
    <tr>
      <td>scan.startup.specific-offset.skip-rows</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>在指定的启动位点后需要跳过的数据行数量。</td>
    </tr>
    <tr>
      <td>connect.timeout</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>连接器在尝试连接到 MySQL 数据库服务器后超时前应等待的最长时间。</td>
    </tr>    
    <tr>
      <td>connect.max-retries</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">3</td>
      <td>Integer</td>
      <td>连接器应重试以建立 MySQL 数据库服务器连接的最大重试次数。</td>
    </tr>
    <tr>
      <td>connection.pool.size</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>Integer</td>
      <td>连接池大小。</td>
    </tr>
    <tr>
      <td>jdbc.properties.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">20</td>
      <td>String</td>
      <td>传递自定义 JDBC URL 属性的选项。用户可以传递自定义属性，如 'jdbc.properties.useSSL' = 'false'.</td>
    </tr>
    <tr>
      <td>heartbeat.interval</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">30s</td>
      <td>Duration</td>
      <td>用于跟踪最新可用 binlog 偏移的发送心跳事件的间隔。</td>
    </tr>
    <tr>
      <td>debezium.*</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>将 Debezium 的属性传递给 Debezium 嵌入式引擎，该引擎用于从 MySQL 服务器捕获数据更改。
          例如: <code>'debezium.snapshot.mode' = 'never'</code>.
          查看更多关于 <a href="https://debezium.io/documentation/reference/1.9/connectors/mysql.html#mysql-connector-properties"> Debezium 的  MySQL 连接器属性</a></td> 
    </tr>
    <tr>
      <td>scan.incremental.close-idle-reader.enabled</td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否在快照结束后关闭空闲的 Reader。 此特性需要 flink 版本大于等于 1.14 并且 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 需要设置为 true。<br>
          若 flink 版本大于等于 1.15，'execution.checkpointing.checkpoints-after-tasks-finish.enabled' 默认值变更为 true，可以不用显式配置 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = true。</td>
    </tr>
    </tbody>
</table>
</div>


启动模式
--------

配置选项```scan.startup.mode```指定 MySQL CDC 使用者的启动模式。有效枚举包括：

- `initial` （默认）：在第一次启动时对受监视的数据库表执行初始快照，并继续读取最新的 binlog。
- `earliest-offset`：跳过快照阶段，从可读取的最早 binlog 位点开始读取
- `latest-offset`：首次启动时，从不对受监视的数据库表执行快照， 连接器仅从 binlog 的结尾处开始读取，这意味着连接器只能读取在连接器启动之后的数据更改。
- `specific-offset`：跳过快照阶段，从指定的 binlog 位点开始读取。位点可通过 binlog 文件名和位置指定，或者在 GTID 在集群上启用时通过 GTID 集合指定。
- `timestamp`：跳过快照阶段，从指定的时间戳开始读取 binlog 事件。

数据类型映射
----------------

<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left" style="width:30%;">MySQL type<a href="https://dev.mysql.com/doc/man/8.0/en/data-types.html"></a></th>
        <th class="text-left" style="width:10%;">CDC type</th>
        <th class="text-left" style="width:60%;">NOTE</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>TINYINT(n)</td>
      <td>TINYINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        SMALLINT<br>
        TINYINT UNSIGNED<br>
        TINYINT UNSIGNED ZEROFILL
      </td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        INT<br>
        YEAR<br>
        MEDIUMINT<br>
        MEDIUMINT UNSIGNED<br>
        MEDIUMINT UNSIGNED ZEROFILL<br>
        SMALLINT UNSIGNED<br>
        SMALLINT UNSIGNED ZEROFILL
      </td>
      <td>INT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BIGINT<br>
        INT UNSIGNED<br>
        INT UNSIGNED ZEROFILL
      </td>
      <td>BIGINT</td>
      <td></td>
    </tr>
   <tr>
      <td>
        BIGINT UNSIGNED<br>
        BIGINT UNSIGNED ZEROFILL<br>
        SERIAL
      </td>
      <td>DECIMAL(20, 0)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        FLOAT<br>
        FLOAT UNSIGNED<br>
        FLOAT UNSIGNED ZEROFILL
        </td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>
        REAL<br>
        REAL UNSIGNED<br>
        REAL UNSIGNED ZEROFILL<br>
        DOUBLE<br>
        DOUBLE UNSIGNED<br>
        DOUBLE UNSIGNED ZEROFILL<br>
        DOUBLE PRECISION<br>
        DOUBLE PRECISION UNSIGNED<br>
        DOUBLE PRECISION UNSIGNED ZEROFILL
      </td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        NUMERIC(p, s) UNSIGNED<br>
        NUMERIC(p, s) UNSIGNED ZEROFILL<br>
        DECIMAL(p, s)<br>
        DECIMAL(p, s) UNSIGNED<br>
        DECIMAL(p, s) UNSIGNED ZEROFILL<br>
        FIXED(p, s)<br>
        FIXED(p, s) UNSIGNED<br>
        FIXED(p, s) UNSIGNED ZEROFILL<br>
        where p <= 38<br>
      </td>
      <td>DECIMAL(p, s)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        NUMERIC(p, s)<br>
        NUMERIC(p, s) UNSIGNED<br>
        NUMERIC(p, s) UNSIGNED ZEROFILL<br>
        DECIMAL(p, s)<br>
        DECIMAL(p, s) UNSIGNED<br>
        DECIMAL(p, s) UNSIGNED ZEROFILL<br>
        FIXED(p, s)<br>
        FIXED(p, s) UNSIGNED<br>
        FIXED(p, s) UNSIGNED ZEROFILL<br>
        where 38 < p <= 65<br>
      </td>
      <td>STRING</td>
      <td>在 MySQL 中，十进制数据类型的精度高达 65，但在 Flink 中，十进制数据类型的精度仅限于 38。所以，如果定义精度大于 38 的十进制列，则应将其映射到字符串以避免精度损失。</td>
    </tr>
    <tr>
      <td>
        BOOLEAN<br>
        TINYINT(1)<br>
        BIT(1)
        </td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TIME [(p)]</td>
      <td>TIME [(p)]</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP [(p)]
      </td>
      <td>TIMESTAMP_LTZ [(p)]
      </td>
      <td></td>
    </tr>
    <tr>
      <td>DATETIME [(p)]
      </td>
      <td>TIMESTAMP [(p)]
      </td>
      <td></td>
    </tr>
    <tr>
      <td>
        CHAR(n)
      </td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARCHAR(n)
      </td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BIT(n)
      </td>
      <td>BINARY(⌈(n + 7) / 8⌉)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        BINARY(n)
      </td>
      <td>BINARY(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        VARBINARY(N)
      </td>
      <td>VARBINARY(N)</td>
      <td></td>
    </tr>
    <tr>
      <td>
        TINYTEXT<br>
        TEXT<br>
        MEDIUMTEXT<br>
        LONGTEXT<br>
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        TINYBLOB<br>
        BLOB<br>
        MEDIUMBLOB<br>
        LONGBLOB<br>
      </td>
      <td>BYTES</td>
      <td>目前，对于 MySQL 中的 BLOB 数据类型，仅支持长度不大于 2147483647（2**31-1）的 blob。 </td>
    </tr>
    <tr>
      <td>
        ENUM
      </td>
      <td>STRING</td>
      <td></td>
    </tr>
    <tr>
      <td>
        JSON
      </td>
      <td>STRING</td>
      <td> JSON 数据类型将在 Flink 中转换为 JSON 格式的字符串。</td>
    </tr>
    <tr>
      <td>
        SET
      </td>
      <td>-</td>
      <td> 暂不支持 </td>
    </tr>
    <tr>
      <td>
       GEOMETRY<br>
       POINT<br>
       LINESTRING<br>
       POLYGON<br>
       MULTIPOINT<br>
       MULTILINESTRING<br>
       MULTIPOLYGON<br>
       GEOMETRYCOLLECTION<br>
      </td>
      <td>
        STRING
      </td>
      <td>
      MySQL 中的空间数据类型将转换为具有固定 Json 格式的字符串。
      请参考 MySQL <a href="#a-name-id-003-a">空间数据类型映射</a> 章节了解更多详细信息。
      </td>
    </tr>
    </tbody>
</table>
</div>

### 空间数据类型映射<a name="空间数据类型映射" id="003"></a>

MySQL中除`GEOMETRYCOLLECTION`之外的空间数据类型都会转换为 Json 字符串，格式固定，如：<br>
```json
{"srid": 0 , "type": "xxx", "coordinates": [0, 0]}
```
字段`srid`标识定义几何体的 SRS，如果未指定 SRID，则 SRID 0 是新几何体值的默认值。
由于 MySQL 8+ 在定义空间数据类型时只支持特定的 SRID，因此在版本较低的MySQL中，字段`srid`将始终为 0。

字段`type`标识空间数据类型，例如`POINT`/`LINESTRING`/`POLYGON`。

字段`coordinates`表示空间数据的`坐标`。

对于`GEOMETRYCOLLECTION`，它将转换为 Json 字符串，格式固定，如：<br>
```json
{"srid": 0 , "type": "GeometryCollection", "geometries": [{"type":"Point","coordinates":[10,10]}]}
```

`Geometrics`字段是一个包含所有空间数据的数组。

不同空间数据类型映射的示例如下：
<div class="wy-table-responsive">
<table class="colwidths-auto docutils">
    <thead>
      <tr>
        <th class="text-left">Spatial data in MySQL</th>
        <th class="text-left">Json String converted in Flink</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>POINT(1 1)</td>
        <td>{"coordinates":[1,1],"type":"Point","srid":0}</td>
      </tr>
      <tr>
        <td>LINESTRING(3 0, 3 3, 3 5)</td>
        <td>{"coordinates":[[3,0],[3,3],[3,5]],"type":"LineString","srid":0}</td>
      </tr>
      <tr>
        <td>POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))</td>
        <td>{"coordinates":[[[1,1],[2,1],[2,2],[1,2],[1,1]]],"type":"Polygon","srid":0}</td>
      </tr>
      <tr>
        <td>MULTIPOINT((1 1),(2 2))</td>
        <td>{"coordinates":[[1,1],[2,2]],"type":"MultiPoint","srid":0}</td>
      </tr>
      <tr>
        <td>MultiLineString((1 1,2 2,3 3),(4 4,5 5))</td>
        <td>{"coordinates":[[[1,1],[2,2],[3,3]],[[4,4],[5,5]]],"type":"MultiLineString","srid":0}</td>
      </tr>
      <tr>
        <td>MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))</td>
        <td>{"coordinates":[[[[0,0],[10,0],[10,10],[0,10],[0,0]]],[[[5,5],[7,5],[7,7],[5,7],[5,5]]]],"type":"MultiPolygon","srid":0}</td>
      </tr>
      <tr>
        <td>GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))</td>
        <td>{"geometries":[{"type":"Point","coordinates":[10,10]},{"type":"Point","coordinates":[30,30]},{"type":"LineString","coordinates":[[15,15],[20,20]]}],"type":"GeometryCollection","srid":0}</td>
      </tr>
    </tbody>
</table>
</div>

常见问题
--------
* [FAQ(English)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ)
* [FAQ(中文)](https://github.com/ververica/flink-cdc-connectors/wiki/FAQ(ZH))
