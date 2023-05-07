package org.apache.kafka.streams.serdes;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

import static org.apache.kafka.streams.serdes.JsonSerialization.OBJECTMAPPER;

/**
 * @author arthur
 */
public class JsonDeserialization<T> implements Deserializer<T> {
    private final Class<T> clazz;

    public JsonDeserialization(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return OBJECTMAPPER.readValue(data, clazz);
        } catch (IOException e) {
            //FIXME: log error
            return null;
        }
    }
}
