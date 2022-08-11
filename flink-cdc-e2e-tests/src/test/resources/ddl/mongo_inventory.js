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

db.getCollection('products').insertMany([
    {
        "_id": ObjectId("100000000000000000000101"),
        "name": "scooter",
        "description": "Small 2-wheel scooter",
        "weight": 3.14
    },
    {
        "_id": ObjectId("100000000000000000000102"),
        "name": "car battery",
        "description": "12V car battery",
        "weight": 8.1
    },
    {
        "_id": ObjectId("100000000000000000000103"),
        "name": "12-pack drill bits",
        "description": "12-pack of drill bits with sizes ranging from #40 to #3",
        "weight": 0.8
    },
    {
        "_id": ObjectId("100000000000000000000104"),
        "name": "hammer",
        "description": "12oz carpenter's hammer",
        "weight": 0.75
    },
    {
        "_id": ObjectId("100000000000000000000105"),
        "name": "hammer",
        "description": "14oz carpenter's hammer",
        "weight": 0.875
    },
    {
        "_id": ObjectId("100000000000000000000106"),
        "name": "hammer",
        "description": "12oz carpenter's hammer",
        "weight": 1.0
    },
    {
        "_id": ObjectId("100000000000000000000107"),
        "name": "rocks",
        "description": "box of assorted rocks",
        "weight": 5.3
    },
    {
        "_id": ObjectId("100000000000000000000108"),
        "name": "jacket",
        "description": "water resistent black wind breaker",
        "weight": 0.1
    },
    {
        "_id": ObjectId("100000000000000000000109"),
        "name": "spare tire",
        "description": "24 inch spare tire",
        "weight": 22.2
    }
]);
