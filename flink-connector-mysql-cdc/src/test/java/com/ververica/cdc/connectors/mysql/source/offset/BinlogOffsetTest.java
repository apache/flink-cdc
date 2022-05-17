/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.offset;

import org.junit.Assert;
import org.junit.Test;

/** Unit tests for {@link BinlogOffset}. */
public class BinlogOffsetTest {
    @Test
    public void isAtOrAfter() {
        BinlogOffset low =
                new BinlogOffset(
                        "mysql-bin.000480",
                        305604659L,
                        0L,
                        0L,
                        0L,
                        "0733254d-c557-11ec-9d5d-b8599f27bf84:3-5,"
                                + "10ed0ca2-e6ba-11ea-82d6-b8599f3ebeda:1-7455798,"
                                + "11b510d4-e6ba-11ea-a3dd-b8599f3739e4:1-216,"
                                + "1e05bbdd-dad4-11eb-9024-6c92bf678de0:1-128580475,"
                                + "25f24207-b250-11eb-b10c-b8599f52b444:1-13,"
                                + "34cbf58d-b32a-11eb-903b-b8599f27b020:1-3,"
                                + "41a9e43d-2d55-11eb-bb51-6c92bf678de0:1,"
                                + "424c2aff-2d57-11eb-a541-b8599f3eddde:1-3,"
                                + "59db6c01-9066-11ec-a66c-b8599f373e4c:1-119747795,"
                                + "5a4a3eba-2c15-11ec-9f86-7cd30adfbd66:1-41,"
                                + "613c840a-d987-11eb-9296-6c92bf679762:1-18302,"
                                + "72369545-f247-11ea-b238-506b4bc22622:1-194248623,"
                                + "733b6aa5-0a44-11ec-8fb7-98039b7cccf6:1-1232,"
                                + "89bdeee1-29d0-11ec-b9bf-506b4bff102c:1-1418,"
                                + "8bbb2c57-c1ec-11eb-b62c-98039b159b1e:1-10,"
                                + "8e251398-b708-11eb-a9b4-b8599f37c6a4:1-22,"
                                + "9828dd6d-f247-11ea-8f8e-b8599f3eddb6:1-206681484,"
                                + "99232b93-29ca-11ec-b1f2-7cd30adfbd66:1-1438,"
                                + "99c907a3-dfd0-11eb-bc90-b8599f3edfa6:1-7,"
                                + "b162ecc1-d826-11eb-8a3a-7cd30adbd442:1-21,"
                                + "b1d0f7e1-d97f-11eb-9a5c-7cd30adbd442:1-16561,"
                                + "b3d48892-3b9a-11eb-b0c4-b8599f3edfa6:1-6,"
                                + "b40b3d22-de4f-11eb-a974-506b4b4b24f6:1-4,"
                                + "b87f17d7-d94a-11eb-af8b-7cd30adbd442:1-54949,"
                                + "cf9537c1-d9a7-11eb-a1b1-7cd30adbd442:1-18812,"
                                + "d045071b-3d1f-11ec-abe4-b8599f52ac34:1-221,"
                                + "d565e1ea-7c6d-11eb-9c40-b8599f3ee056:1-12563615,"
                                + "e3fbf933-0a40-11ec-8fd3-b8599f4fd990:1-1096,"
                                + "eac79140-0d28-11ec-a8d4-b8599f3f2336:1-655,"
                                + "f85d4d24-dad3-11eb-9cf3-b8599f52b444:1-753905687,"
                                + "f9658a0e-2b71-11ec-8e67-7cd30adfbd66:1-721",
                        null);
        BinlogOffset high =
                new BinlogOffset(
                        "mysql-bin.000480",
                        305606564L,
                        0L,
                        0L,
                        0L,
                        "0733254d-c557-11ec-9d5d-b8599f27bf84:3-5,"
                                + "10ed0ca2-e6ba-11ea-82d6-b8599f3ebeda:1-7455798,"
                                + "11b510d4-e6ba-11ea-a3dd-b8599f3739e4:1-216,"
                                + "1e05bbdd-dad4-11eb-9024-6c92bf678de0:1-128580475,"
                                + "25f24207-b250-11eb-b10c-b8599f52b444:1-13,"
                                + "34cbf58d-b32a-11eb-903b-b8599f27b020:1-3,"
                                + "41a9e43d-2d55-11eb-bb51-6c92bf678de0:1,"
                                + "424c2aff-2d57-11eb-a541-b8599f3eddde:1-3,"
                                + "59db6c01-9066-11ec-a66c-b8599f373e4c:1-119747798,"
                                + "5a4a3eba-2c15-11ec-9f86-7cd30adfbd66:1-41,"
                                + "613c840a-d987-11eb-9296-6c92bf679762:1-18302,"
                                + "72369545-f247-11ea-b238-506b4bc22622:1-194248623,"
                                + "733b6aa5-0a44-11ec-8fb7-98039b7cccf6:1-1232,"
                                + "89bdeee1-29d0-11ec-b9bf-506b4bff102c:1-1418,"
                                + "8bbb2c57-c1ec-11eb-b62c-98039b159b1e:1-10,"
                                + "8e251398-b708-11eb-a9b4-b8599f37c6a4:1-22,"
                                + "9828dd6d-f247-11ea-8f8e-b8599f3eddb6:1-206681484,"
                                + "99232b93-29ca-11ec-b1f2-7cd30adfbd66:1-1438,"
                                + "99c907a3-dfd0-11eb-bc90-b8599f3edfa6:1-7,"
                                + "b162ecc1-d826-11eb-8a3a-7cd30adbd442:1-21,"
                                + "b1d0f7e1-d97f-11eb-9a5c-7cd30adbd442:1-16561,"
                                + "b3d48892-3b9a-11eb-b0c4-b8599f3edfa6:1-6,"
                                + "b40b3d22-de4f-11eb-a974-506b4b4b24f6:1-4,"
                                + "b87f17d7-d94a-11eb-af8b-7cd30adbd442:1-54949,"
                                + "cf9537c1-d9a7-11eb-a1b1-7cd30adbd442:1-18812,"
                                + "d045071b-3d1f-11ec-abe4-b8599f52ac34:1-221,"
                                + "d565e1ea-7c6d-11eb-9c40-b8599f3ee056:1-12563615,"
                                + "e3fbf933-0a40-11ec-8fd3-b8599f4fd990:1-1096,"
                                + "eac79140-0d28-11ec-a8d4-b8599f3f2336:1-655,"
                                + "f85d4d24-dad3-11eb-9cf3-b8599f52b444:1-753905687,"
                                + "f9658a0e-2b71-11ec-8e67-7cd30adfbd66:1-721",
                        null);
        Assert.assertTrue(high.isAtOrAfter(low));
    }
}
