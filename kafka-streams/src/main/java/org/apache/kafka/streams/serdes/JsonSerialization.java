package org.apache.kafka.streams.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;

/**
 * @author arthur
 */
class JsonSerialization<T> implements Serializer<T> {
    public final static ObjectMapper OBJECTMAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return OBJECTMAPPER.writeValueAsBytes(data);
        }catch (JsonProcessingException e){
            //FIXME: log error
            return new byte[0];
        }
    }
}
