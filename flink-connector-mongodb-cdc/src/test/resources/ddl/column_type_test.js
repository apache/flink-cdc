// Copyright 2022 Ververica Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

db.getCollection('full_types').insertOne({
    "_id": ObjectId("5d505646cf6d4fe581014ab2"),
    "stringField": "hello",
    "uuidField": UUID("0bd1e27e-2829-4b47-8e21-dfef93da44e1"),
    "md5Field": MD5("2078693f4c61ce3073b01be69ab76428"),
    "timeField": ISODate("2019-08-11T17:54:14.692Z"),
    "dateField": ISODate("2019-08-11T17:54:14.692Z"),
    "dateBefore1970": ISODate("1960-08-11T17:54:14.692Z"),
    "dateToTimestampField": ISODate("2019-08-11T17:54:14.692Z"),
    "dateToLocalTimestampField": ISODate("2019-08-11T17:54:14.692Z"),
    "timestampField": Timestamp(1565545664,1),
    "timestampToLocalTimestampField": Timestamp(1565545664,1),
    "booleanField": true,
    "decimal128Field": NumberDecimal("10.99"),
    "doubleField": 10.5,
    "int32field": NumberInt("10"),
    "int64Field": NumberLong("50"),
    "documentField": {"a":"hello", "b": NumberLong("50")},
    "mapField": {
        "inner_map": {
            "key": NumberLong("234")
        }
    },
    "arrayField": ["hello","world"],
    "doubleArrayField": [1.0, 1.1, null],
    "documentArrayField": [{"a":"hello0", "b": NumberLong("51")}, {"a":"hello1", "b": NumberLong("53")}],
    "minKeyField": MinKey(),
    "maxKeyField": MaxKey(),
    "regexField": /^H/i,
    "undefinedField": undefined,
    "nullField": null,
    "binaryField": BinData(0,"AQID"),
    "javascriptField": (function() { x++; }),
    "dbReferenceField": DBRef("ref_doc", ObjectId("5d505646cf6d4fe581014ab3"))
});