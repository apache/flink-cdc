/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.utils.LegacyRowResource;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.ververica.cdc.connectors.mongodb.MongoDBTestBase;
import org.bson.BsonDateTime;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.ververica.cdc.connectors.mongodb.utils.MongoDBTestUtils.waitForSinkSize;
import static com.ververica.cdc.connectors.mongodb.utils.MongoDBTestUtils.waitForSnapshotStarted;
import static org.junit.Assert.assertEquals;

/** Integration tests to check mongodb-cdc works well under RawType. */
public class MongoDBRawTypeITCase extends MongoDBTestBase {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env,
                    EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
    @ClassRule public static LegacyRowResource usesLegacyRows = LegacyRowResource.INSTANCE;

    private final RawType<BsonValue> rawType =
            new RawType<>(BsonValue.class, new BsonValueSerializer());

    @Before
    public void before() {
        TestValuesTableFactory.clearAllData();
        env.setParallelism(1);
    }

    @Test
    public void testRawTypes() throws Exception {
        String database = executeCommandFileInSeparateDatabase("column_type_test");

        String sourceDDL =
                String.format(
                        "CREATE TABLE full_types (\n"
                                + "    _id STRING,\n"
                                + rawTypeOf("stringField")
                                + rawTypeOf("uuidField")
                                + rawTypeOf("md5Field")
                                + rawTypeOf("timeField")
                                + rawTypeOf("dateField")
                                + rawTypeOf("dateBefore1970")
                                + rawTypeOf("dateToTimestampField")
                                + rawTypeOf("dateToLocalTimestampField")
                                + rawTypeOf("timestampField")
                                + rawTypeOf("timestampToLocalTimestampField")
                                + rawTypeOf("booleanField")
                                + rawTypeOf("decimal128Field")
                                + rawTypeOf("doubleField")
                                + rawTypeOf("int32field")
                                + rawTypeOf("int64Field")
                                + rawTypeOf("documentField")
                                + rawTypeOf("mapField")
                                + rawTypeOf("arrayField")
                                + rawTypeOf("doubleArrayField")
                                + rawTypeOf("documentArrayField")
                                + rawTypeOf("minKeyField")
                                + rawTypeOf("maxKeyField")
                                + rawTypeOf("regexField")
                                + rawTypeOf("undefinedField")
                                + rawTypeOf("nullField")
                                + rawTypeOf("binaryField")
                                + rawTypeOf("javascriptField")
                                + rawTypeOf("dbReferenceField")
                                + "    PRIMARY KEY (_id) NOT ENFORCED"
                                + ") WITH ("
                                + " 'connector' = 'mongodb-cdc',"
                                + " 'hosts' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database' = '%s',"
                                + " 'collection' = '%s'"
                                + ")",
                        MONGODB_CONTAINER.getHostAndPort(),
                        FLINK_USER,
                        FLINK_USER_PASSWORD,
                        database,
                        "full_types");

        String sinkDDL =
                "CREATE TABLE sink (\n"
                        + "_id STRING,\n"
                        + "stringField STRING,\n"
                        + rawTypeOf("uuidField")
                        + rawTypeOf("md5Field")
                        + rawTypeOf("timeField")
                        + rawTypeOf("dateField")
                        + rawTypeOf("dateBefore1970")
                        + rawTypeOf("dateToTimestampField")
                        + rawTypeOf("dateToLocalTimestampField")
                        + rawTypeOf("timestampField")
                        + rawTypeOf("timestampToLocalTimestampField")
                        + rawTypeOf("booleanField")
                        + rawTypeOf("decimal128Field")
                        + rawTypeOf("doubleField")
                        + rawTypeOf("int32field")
                        + "int64Field BIGINT,\n"
                        + "documentField STRING,\n"
                        + rawTypeOf("mapField")
                        + rawTypeOf("arrayField")
                        + rawTypeOf("doubleArrayField")
                        + rawTypeOf("documentArrayField")
                        + rawTypeOf("minKeyField")
                        + rawTypeOf("maxKeyField")
                        + rawTypeOf("regexField")
                        + rawTypeOf("undefinedField")
                        + rawTypeOf("nullField")
                        + rawTypeOf("binaryField")
                        + rawTypeOf("javascriptField")
                        + rawTypeOf("dbReferenceField", true)
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false'"
                        + ")";
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        tEnv.createTemporaryFunction("convertBsonValue", new BsonConversionFunction());

        // async submit job
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT _id,\n"
                                + "convertBsonValue(stringField, 'STRING'),\n"
                                + "uuidField,\n"
                                + "md5Field,\n"
                                + "timeField,\n"
                                + "dateField,\n"
                                + "dateBefore1970,\n"
                                + "dateToTimestampField,\n"
                                + "dateToLocalTimestampField,\n"
                                + "timestampField,\n"
                                + "timestampToLocalTimestampField,\n"
                                + "booleanField,\n"
                                + "decimal128Field,\n"
                                + "doubleField,\n"
                                + "int32field,\n"
                                + "convertBsonValue(int64Field, 'BIGINT'),\n"
                                + "convertBsonValue(documentField, 'JSON'),\n"
                                + "mapField,\n"
                                + "arrayField,\n"
                                + "doubleArrayField,\n"
                                + "documentArrayField,\n"
                                + "minKeyField,\n"
                                + "maxKeyField,\n"
                                + "regexField,\n"
                                + "undefinedField,\n"
                                + "nullField,\n"
                                + "binaryField,\n"
                                + "javascriptField,\n"
                                + "dbReferenceField\n"
                                + "FROM full_types");

        waitForSnapshotStarted("sink");

        MongoCollection<Document> fullTypes =
                getMongoDatabase(database).getCollection("full_types");

        fullTypes.updateOne(
                Filters.eq("_id", new ObjectId("5d505646cf6d4fe581014ab2")),
                Updates.set("int64Field", 510L));

        // 2021-09-03T18:36:04.123Z
        BsonDateTime updatedDateTime = new BsonDateTime(1630694164123L);
        // 2021-09-03T18:36:04Z
        BsonTimestamp updatedTimestamp = new BsonTimestamp(1630694164, 0);
        fullTypes.updateOne(
                Filters.eq("_id", new ObjectId("5d505646cf6d4fe581014ab2")),
                Updates.combine(
                        Updates.set("timeField", updatedDateTime),
                        Updates.set("dateField", updatedDateTime),
                        Updates.set("dateToTimestampField", updatedDateTime),
                        Updates.set("dateToLocalTimestampField", updatedDateTime),
                        Updates.set("timestampField", updatedTimestamp),
                        Updates.set("timestampToLocalTimestampField", updatedTimestamp)));

        waitForSinkSize("sink", 5);

        List<String> expected =
                Arrays.asList(
                        "+I(5d505646cf6d4fe581014ab2,hello,BsonBinary{type=4, data=[11, -47, -30, 126, 40, 41, 75, 71, -114, 33, -33, -17, -109, -38, 68, -31]},BsonBinary{type=5, data=[32, 120, 105, 63, 76, 97, -50, 48, 115, -80, 27, -26, -102, -73, 100, 40]},BsonDateTime{value=1565546054692},BsonDateTime{value=1565546054692},BsonDateTime{value=-296287545308},BsonDateTime{value=1565546054692},BsonDateTime{value=1565546054692},Timestamp{value=6723967427274604545, seconds=1565545664, inc=1},Timestamp{value=6723967427274604545, seconds=1565545664, inc=1},BsonBoolean{value=true},BsonDecimal128{value=10.99},BsonDouble{value=10.5},BsonInt32{value=10},50,{\"a\": \"hello\", \"b\": 50},{\"inner_map\": {\"key\": 234}},BsonArray{values=[BsonString{value='hello'}, BsonString{value='world'}]},BsonArray{values=[BsonDouble{value=1.0}, BsonDouble{value=1.1}, BsonNull]},BsonArray{values=[{\"a\": \"hello0\", \"b\": 51}, {\"a\": \"hello1\", \"b\": 53}]},BsonMinKey,BsonMaxKey,BsonRegularExpression{pattern='^H', options='i'},null,null,BsonBinary{type=0, data=[1, 2, 3]},BsonJavaScript{code='function() { x++; }'},{\"$ref\": \"ref_doc\", \"$id\": {\"$oid\": \"5d505646cf6d4fe581014ab3\"}})",
                        "-U(5d505646cf6d4fe581014ab2,hello,BsonBinary{type=4, data=[11, -47, -30, 126, 40, 41, 75, 71, -114, 33, -33, -17, -109, -38, 68, -31]},BsonBinary{type=5, data=[32, 120, 105, 63, 76, 97, -50, 48, 115, -80, 27, -26, -102, -73, 100, 40]},BsonDateTime{value=1565546054692},BsonDateTime{value=1565546054692},BsonDateTime{value=-296287545308},BsonDateTime{value=1565546054692},BsonDateTime{value=1565546054692},Timestamp{value=6723967427274604545, seconds=1565545664, inc=1},Timestamp{value=6723967427274604545, seconds=1565545664, inc=1},BsonBoolean{value=true},BsonDecimal128{value=10.99},BsonDouble{value=10.5},BsonInt32{value=10},50,{\"a\": \"hello\", \"b\": 50},{\"inner_map\": {\"key\": 234}},BsonArray{values=[BsonString{value='hello'}, BsonString{value='world'}]},BsonArray{values=[BsonDouble{value=1.0}, BsonDouble{value=1.1}, BsonNull]},BsonArray{values=[{\"a\": \"hello0\", \"b\": 51}, {\"a\": \"hello1\", \"b\": 53}]},BsonMinKey,BsonMaxKey,BsonRegularExpression{pattern='^H', options='i'},null,null,BsonBinary{type=0, data=[1, 2, 3]},BsonJavaScript{code='function() { x++; }'},{\"$ref\": \"ref_doc\", \"$id\": {\"$oid\": \"5d505646cf6d4fe581014ab3\"}})",
                        "+U(5d505646cf6d4fe581014ab2,hello,BsonBinary{type=4, data=[11, -47, -30, 126, 40, 41, 75, 71, -114, 33, -33, -17, -109, -38, 68, -31]},BsonBinary{type=5, data=[32, 120, 105, 63, 76, 97, -50, 48, 115, -80, 27, -26, -102, -73, 100, 40]},BsonDateTime{value=1565546054692},BsonDateTime{value=1565546054692},BsonDateTime{value=-296287545308},BsonDateTime{value=1565546054692},BsonDateTime{value=1565546054692},Timestamp{value=6723967427274604545, seconds=1565545664, inc=1},Timestamp{value=6723967427274604545, seconds=1565545664, inc=1},BsonBoolean{value=true},BsonDecimal128{value=10.99},BsonDouble{value=10.5},BsonInt32{value=10},510,{\"a\": \"hello\", \"b\": 50},{\"inner_map\": {\"key\": 234}},BsonArray{values=[BsonString{value='hello'}, BsonString{value='world'}]},BsonArray{values=[BsonDouble{value=1.0}, BsonDouble{value=1.1}, BsonNull]},BsonArray{values=[{\"a\": \"hello0\", \"b\": 51}, {\"a\": \"hello1\", \"b\": 53}]},BsonMinKey,BsonMaxKey,BsonRegularExpression{pattern='^H', options='i'},null,null,BsonBinary{type=0, data=[1, 2, 3]},BsonJavaScript{code='function() { x++; }'},{\"$ref\": \"ref_doc\", \"$id\": {\"$oid\": \"5d505646cf6d4fe581014ab3\"}})",
                        "-U(5d505646cf6d4fe581014ab2,hello,BsonBinary{type=4, data=[11, -47, -30, 126, 40, 41, 75, 71, -114, 33, -33, -17, -109, -38, 68, -31]},BsonBinary{type=5, data=[32, 120, 105, 63, 76, 97, -50, 48, 115, -80, 27, -26, -102, -73, 100, 40]},BsonDateTime{value=1565546054692},BsonDateTime{value=1565546054692},BsonDateTime{value=-296287545308},BsonDateTime{value=1565546054692},BsonDateTime{value=1565546054692},Timestamp{value=6723967427274604545, seconds=1565545664, inc=1},Timestamp{value=6723967427274604545, seconds=1565545664, inc=1},BsonBoolean{value=true},BsonDecimal128{value=10.99},BsonDouble{value=10.5},BsonInt32{value=10},510,{\"a\": \"hello\", \"b\": 50},{\"inner_map\": {\"key\": 234}},BsonArray{values=[BsonString{value='hello'}, BsonString{value='world'}]},BsonArray{values=[BsonDouble{value=1.0}, BsonDouble{value=1.1}, BsonNull]},BsonArray{values=[{\"a\": \"hello0\", \"b\": 51}, {\"a\": \"hello1\", \"b\": 53}]},BsonMinKey,BsonMaxKey,BsonRegularExpression{pattern='^H', options='i'},null,null,BsonBinary{type=0, data=[1, 2, 3]},BsonJavaScript{code='function() { x++; }'},{\"$ref\": \"ref_doc\", \"$id\": {\"$oid\": \"5d505646cf6d4fe581014ab3\"}})",
                        "+U(5d505646cf6d4fe581014ab2,hello,BsonBinary{type=4, data=[11, -47, -30, 126, 40, 41, 75, 71, -114, 33, -33, -17, -109, -38, 68, -31]},BsonBinary{type=5, data=[32, 120, 105, 63, 76, 97, -50, 48, 115, -80, 27, -26, -102, -73, 100, 40]},BsonDateTime{value=1630694164123},BsonDateTime{value=1630694164123},BsonDateTime{value=-296287545308},BsonDateTime{value=1630694164123},BsonDateTime{value=1630694164123},Timestamp{value=7003778104158060544, seconds=1630694164, inc=0},Timestamp{value=7003778104158060544, seconds=1630694164, inc=0},BsonBoolean{value=true},BsonDecimal128{value=10.99},BsonDouble{value=10.5},BsonInt32{value=10},510,{\"a\": \"hello\", \"b\": 50},{\"inner_map\": {\"key\": 234}},BsonArray{values=[BsonString{value='hello'}, BsonString{value='world'}]},BsonArray{values=[BsonDouble{value=1.0}, BsonDouble{value=1.1}, BsonNull]},BsonArray{values=[{\"a\": \"hello0\", \"b\": 51}, {\"a\": \"hello1\", \"b\": 53}]},BsonMinKey,BsonMaxKey,BsonRegularExpression{pattern='^H', options='i'},null,null,BsonBinary{type=0, data=[1, 2, 3]},BsonJavaScript{code='function() { x++; }'},{\"$ref\": \"ref_doc\", \"$id\": {\"$oid\": \"5d505646cf6d4fe581014ab3\"}})");
        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertEquals(expected, actual);

        result.getJobClient().get().cancel().get();
    }

    private String rawTypeOf(String fieldName) {
        return rawTypeOf(fieldName, false);
    }

    private String rawTypeOf(String fieldName, boolean isLast) {
        // RAW('org.bson.BsonValue',
        // 'AFpjb20udmVydmVyaWNhLmNkYy5jb25uZWN0b3JzLm1vbmdvZGIudGFibGUuQnNvblZhbHVlU2VyaWFsaXplciRCc29uVmFsdWVTZXJpYWxpemVyU25hcHNob3QAAAAD')
        String rawTypeSQL = rawType.asSerializableString();

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(fieldName).append(" ").append(rawTypeSQL);
        if (!isLast) {
            stringBuilder.append(",\n");
        } else {
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }

    /** Test Bson conversion UDF. */
    public static class BsonConversionFunction extends ScalarFunction {

        public Object eval(BsonValue value, String outputType) {
            switch (outputType) {
                case "STRING":
                    return value.asString().getValue();
                case "BIGINT":
                    if (value.isInt32()) {
                        return value.asInt32().longValue();
                    }
                    return value.asInt64().getValue();
                case "JSON":
                    return Document.parse(value.asDocument().toJson()).toJson();
            }
            return null;
        }

        @Override
        public TypeInference getTypeInference(DataTypeFactory typeFactory) {
            return TypeInference.newBuilder()
                    .typedArguments(
                            DataTypes.RAW(BsonValue.class, BsonValueSerializer.INSTANCE),
                            DataTypes.STRING())
                    .outputTypeStrategy(
                            callContext -> {
                                if (!callContext.isArgumentLiteral(1)
                                        || callContext.isArgumentNull(1)) {
                                    throw callContext.newValidationError(
                                            "Literal expected for second argument.");
                                }
                                final String literal =
                                        callContext
                                                .getArgumentValue(1, String.class)
                                                .orElse("STRING");
                                switch (literal) {
                                    case "BIGINT":
                                        return Optional.of(DataTypes.BIGINT().notNull());
                                    case "STRING":
                                    case "JSON":
                                    default:
                                        return Optional.of(DataTypes.STRING());
                                }
                            })
                    .build();
        }
    }
}
