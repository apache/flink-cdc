/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.source.utils;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Unit test for {@link ResumeTokenUtils}. */
public class ResumeTokenUtilsTest {

    @Test
    public void testDecodeBinDataFormat() {
        BsonDocument resumeToken =
                BsonDocument.parse(
                        "{\"_data\": {\"$binary\": {\"base64\": \"gmNXqzwAAAABRmRfaWQAZGNXqj41xq4H4ebHNwBaEATmzwG2DzpOl4tpOyYEG9zABA==\", \"subType\": \"00\"}}}");
        BsonTimestamp expected = new BsonTimestamp(1666689852, 1);
        BsonTimestamp actual = ResumeTokenUtils.decodeTimestamp(resumeToken);
        assertEquals(expected, actual);
    }

    @Test
    public void testDecodeHexFormatV0() {
        BsonDocument resumeToken =
                BsonDocument.parse(
                        " {\"_data\": \"826357B0840000000129295A1004461ECCED47A6420D9713A5135650360746645F696400646357B05F35C6AE07E1E6C7390004\"}");
        BsonTimestamp expected = new BsonTimestamp(1666691204, 1);
        BsonTimestamp actual = ResumeTokenUtils.decodeTimestamp(resumeToken);
        assertEquals(expected, actual);
    }

    @Test
    public void testDecodeHexFormatV1() {
        BsonDocument resumeToken =
                BsonDocument.parse(
                        "{\"_data\": \"82612E8513000000012B022C0100296E5A1004A5093ABB38FE4B9EA67F01BB1A96D812463C5F6964003C5F5F5F78000004\"}");
        BsonTimestamp expected = new BsonTimestamp(1630438675, 1);
        BsonTimestamp actual = ResumeTokenUtils.decodeTimestamp(resumeToken);
        assertEquals(expected, actual);
    }
}
