package org.apache.kafka.streams.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.model.*;

/**
 * @author arthur
 */
public class JsonSerdes {

    public static TransactionPatternWrapperSerde transactionPattern() {
        return new TransactionPatternWrapperSerde(new JsonSerialization<>(), new JsonDeserialization<>(TransactionPattern.class));
    }

    public final static class TransactionPatternWrapperSerde extends WrapperSerde<TransactionPattern> {
        public TransactionPatternWrapperSerde(Serializer<TransactionPattern> serializer, Deserializer<TransactionPattern> deserializer) {
            super(serializer, deserializer);
        }
    }

    public static TransactionRewardWrapperSerde transactionReward() {
        return new TransactionRewardWrapperSerde(new JsonSerialization<>(), new JsonDeserialization<>(TransactionReward.class));
    }

    public final static class TransactionRewardWrapperSerde extends WrapperSerde<TransactionReward> {
        public TransactionRewardWrapperSerde(Serializer<TransactionReward> serializer, Deserializer<TransactionReward> deserializer) {
            super(serializer, deserializer);
        }
    }

    public static TransactionKeyWrapperSerde transactionKey() {
        return new TransactionKeyWrapperSerde(new JsonSerialization<>(), new JsonDeserialization<>(TransactionKey.class));
    }

    public final static class TransactionKeyWrapperSerde extends WrapperSerde<TransactionKey> {
        public TransactionKeyWrapperSerde(Serializer<TransactionKey> serializer, Deserializer<TransactionKey> deserializer) {
            super(serializer, deserializer);
        }
    }

    public static TransactionWrapperSerde transaction() {
        return new TransactionWrapperSerde(new JsonSerialization<>(), new JsonDeserialization<>(Transaction.class));
    }

    public final static class TransactionWrapperSerde extends WrapperSerde<Transaction> {
        public TransactionWrapperSerde(Serializer<Transaction> serializer, Deserializer<Transaction> deserializer) {
            super(serializer, deserializer);
        }
    }

    public static SaleWrapperSerde sale() {
        return new SaleWrapperSerde(new JsonSerialization<>(), new JsonDeserialization<>(Sale.class));
    }

    public final static class SaleWrapperSerde extends WrapperSerde<Sale> {
        public SaleWrapperSerde(Serializer<Sale> serializer, Deserializer<Sale> deserializer) {
            super(serializer, deserializer);
        }
    }

    public static SaleStateWrapperSerde saleState() {
        return new SaleStateWrapperSerde(new JsonSerialization<>(), new JsonDeserialization<>(SaleState.class));
    }

    public final static class SaleStateWrapperSerde extends WrapperSerde<SaleState> {
        public SaleStateWrapperSerde(Serializer<SaleState> serializer, Deserializer<SaleState> deserializer) {
            super(serializer, deserializer);
        }
    }

    private static class WrapperSerde<T> implements Serde<T> {
        private final Serializer<T> tSerializer;
        private final Deserializer<T> deserializer;

        private WrapperSerde(Serializer<T> serialization, Deserializer<T> deserialization) {
            this.tSerializer = serialization;
            this.deserializer = deserialization;
        }

        @Override
        public Serializer<T> serializer() {
            return tSerializer;
        }

        @Override
        public Deserializer<T> deserializer() {
            return deserializer;
        }
    }
}
