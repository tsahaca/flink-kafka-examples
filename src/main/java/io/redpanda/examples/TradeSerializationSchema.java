package io.redpanda.examples;

import org.apache.flink.api.common.serialization.SerializationSchema;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TradeSerializationSchema implements SerializationSchema<TradeVO> {

    static ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    Logger logger = LoggerFactory.getLogger(TradeSerializationSchema.class);

    @Override
    public byte[] serialize(TradeVO trade) {
        if (objectMapper == null) {
            objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
            objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        }
        try {
            String json = objectMapper.writeValueAsString(trade);
            return json.getBytes();
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            logger.error("Failed to parse JSON", e);
        }
        return new byte[0];
    }
}
