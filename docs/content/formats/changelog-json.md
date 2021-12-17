# Changelog JSON Format

Flink supports to emit changelogs in JSON format and interpret the output back again.

Dependencies
------------

In order to setup the Changelog JSON format, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

### Maven dependency

```
<dependency>
  <groupId>com.ververica</groupId>
  <artifactId>flink-format-changelog-json</artifactId>
  <!-- the dependency is available only for stable releases. -->
  <version>2.1.1</version>
</dependency>
```

### SQL Client JAR

```Download link is available only for stable releases.```

Download [flink-format-changelog-json-2.1.1.jar](https://repo1.maven.org/maven2/com/ververica/flink-format-changelog-json/2.1.1/flink-format-changelog-json-2.1.1.jar) and put it under `<FLINK_HOME>/lib/`.


How to use Changelog JSON format
----------------

```sql
-- assuming we have a user_behavior logs
CREATE TABLE user_behavior (
    user_id BIGINT,
    item_id BIGINT,
    category_id BIGINT,
    behavior STRING,
    ts TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',  -- using kafka connector
    'topic' = 'user_behavior',  -- kafka topic
    'scan.startup.mode' = 'earliest-offset',  -- reading from the beginning
    'properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker address
    'format' = 'json'  -- the data format is json
);

-- we want to store the the UV aggregation result in kafka using changelog-json format
create table day_uv (
    day_str STRING,
    uv BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'day_uv',
    'scan.startup.mode' = 'earliest-offset',  -- reading from the beginning
    'properties.bootstrap.servers' = 'localhost:9092',  -- kafka broker address
    'format' = 'changelog-json'  -- the data format is changelog-json
);

-- write the UV results into kafka using changelog-json format
INSERT INTO day_uv
SELECT DATE_FORMAT(ts, 'yyyy-MM-dd') as date_str, count(distinct user_id) as uv
FROM user_behavior
GROUP BY DATE_FORMAT(ts, 'yyyy-MM-dd');

-- reading the changelog back again
SELECT * FROM day_uv;
```

Format Options
----------------

<div class="highlight">
<table class="colwidths-auto docutils">
    <thead>
         <tr>
           <th class="text-left" style="width: 25%">Option</th>
           <th class="text-center" style="width: 8%">Required</th>
           <th class="text-center" style="width: 7%">Default</th>
           <th class="text-center" style="width: 10%">Type</th>
           <th class="text-center" style="width: 50%">Description</th>
         </tr>
       </thead>
       <tbody>
       <tr>
         <td>format</td>
         <td>required</td>
         <td style="word-wrap: break-word;">(none)</td>
         <td>String</td>
         <td>Specify what format to use, here should be 'changelog-json'.</td>
       </tr>
       <tr>
         <td>changelog-json.ignore-parse-errors</td>
         <td>optional</td>
         <td style="word-wrap: break-word;">false</td>
         <td>Boolean</td>
         <td>Skip fields and rows with parse errors instead of failing.
         Fields are set to null in case of errors.</td>
       </tr>
       <tr>
         <td>changelog-json.timestamp-format.standard</td>
         <td>optional</td>
         <td style="word-wrap: break-word;">'SQL'</td>
         <td>String</td>
         <td>Specify the input and output timestamp format. Currently supported values are 'SQL' and 'ISO-8601':
         <ul>
           <li>Option 'SQL' will parse input timestamp in "yyyy-MM-dd HH:mm:ss.s{precision}" format, e.g '2020-12-30 12:13:14.123' and output timestamp in the same format.</li>
           <li>Option 'ISO-8601'will parse input timestamp in "yyyy-MM-ddTHH:mm:ss.s{precision}" format, e.g '2020-12-30T12:13:14.123' and output timestamp in the same format.</li>
         </ul>
         </td>
         </tr>
       </tbody>
</table>
</div>

Data Type Mapping
----------------

Currently, the Canal format uses JSON format for deserialization. Please refer to [JSON format documentation](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/formats/json.html#data-type-mapping) for more details about the data type mapping.

