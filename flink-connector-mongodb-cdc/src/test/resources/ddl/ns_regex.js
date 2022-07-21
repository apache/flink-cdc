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

db.getCollection('coll_a1').insertOne({"seq": "A101"});
db.getCollection('coll_a2').insertOne({"seq": "A201"});
db.getCollection('coll_b1').insertOne({"seq": "B101"});
db.getCollection('coll_b2').insertOne({"seq": "B201"});