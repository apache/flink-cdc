package org.apache.flink.cdc.connectors.mongodb.internal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MongoDBEnvelopeTest {

    @Test
    void shouldEncodeSpacesAsPercent20() {
        assertEquals(
                "my%20pass",
                MongoDBEnvelope.encodeValue("my pass"));
    }
}