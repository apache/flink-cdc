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

sh.enableSharding(db.getName());

db.getCollection('shopping_cart').createIndex({ "user_id": 1, "product_no": 1, "product_kind": 1 }, { "unique": true });
sh.shardCollection(db.getName() + ".shopping_cart", { "user_id": 1, "product_no": 1, "product_kind": 1});

var shoppingCarts = [];
for (var i = 1; i <= 20480; i++) {
    shoppingCarts.push({
        "product_no": NumberLong(i),
        "product_kind": 'KIND_' + i,
        "user_id": 'user_' + i,
        "description": 'my shopping cart ' + i
    });
    if (i % 1024 == 0) {
        db.getCollection('shopping_cart').insertMany(shoppingCarts);
        shoppingCarts = [];
    }
}
