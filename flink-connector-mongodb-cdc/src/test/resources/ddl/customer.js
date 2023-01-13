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

db.getCollection('customers').insertMany([
    { "cid": NumberLong("101"), "name": "user_1",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("102"), "name": "user_2",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("103"), "name": "user_3",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("109"), "name": "user_4",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("110"), "name": "user_5",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("111"), "name": "user_6",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("118"), "name": "user_7",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("121"), "name": "user_8",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("123"), "name": "user_9",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1009"),"name": "user_10", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1010"),"name": "user_11", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1011"),"name": "user_12", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1012"),"name": "user_13", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1013"),"name": "user_14", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1014"),"name": "user_15", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1015"),"name": "user_16", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1016"),"name": "user_17", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1017"),"name": "user_18", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1018"),"name": "user_19", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1019"),"name": "user_20", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("2000"),"name": "user_21", "address": "Shanghai", "phone_number": "123567891234" }
]);

db.getCollection('customers_1').insertMany([
    { "cid": NumberLong("101"),  "name": "user_1",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("102"),  "name": "user_2",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("103"),  "name": "user_3",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("109"),  "name": "user_4",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("110"),  "name": "user_5",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("111"),  "name": "user_6",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("118"),  "name": "user_7",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("121"),  "name": "user_8",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("123"),  "name": "user_9",  "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1009"), "name": "user_10", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1010"), "name": "user_11", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1011"), "name": "user_12", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1012"), "name": "user_13", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1013"), "name": "user_14", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1014"), "name": "user_15", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1015"), "name": "user_16", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1016"), "name": "user_17", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1017"), "name": "user_18", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1018"), "name": "user_19", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("1019"), "name": "user_20", "address": "Shanghai", "phone_number": "123567891234" },
    { "cid": NumberLong("2000"), "name": "user_21", "address": "Shanghai", "phone_number": "123567891234" }
]);

