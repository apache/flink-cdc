package org.apache.flink.cdc.connectors.elasticsearch.v2;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/** OperationSerializer is responsible for serialization and deserialization of an Operation. */
public class OperationSerializer {
    private final Kryo kryo = new Kryo();

    public OperationSerializer() {
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
    }

    public void serialize(Operation request, DataOutputStream out) {
        try (Output output = new Output(out)) {
            kryo.writeObjectOrNull(output, request, Operation.class);
            output.flush();
        }
    }

    public Operation deserialize(long requestSize, DataInputStream in) {
        try (Input input = new Input(in, (int) requestSize)) {
            if (input.available() > 0) {
                return kryo.readObject(input, Operation.class);
            } else {
                return null; // Skip if input stream is empty
            }
        } catch (Exception e) {
            // Handle the exception as needed, e.g., log the error
            System.err.println("Failed to deserialize Operation: " + e.getMessage());
            return null;
        }
    }

    public int size(Operation operation) {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (Output output = new Output(byteArrayOutputStream)) {
            kryo.writeObjectOrNull(output, operation, Operation.class);
            output.flush();
            return byteArrayOutputStream.size();
        }
    }
}
