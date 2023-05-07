package org.apache.kafka.streams.stateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * @author arthur
 */
public class StatefulTransformOperation {
    //0. define variable
    private final static Logger LOG = LoggerFactory.getLogger(StatefulTransformOperation.class);
    private final static String APP_ID = "stateful_transform_operation";
    private final static String BOOTSTRAP_SERVER = "192.168.103.101:9092";
    private final static String SOURCE_TOPIC = "input.words";
    private final static String TARGET_TOPIC = "out.words";
    private final static String STATE_STORE_NAME = "stateful_transform_operation";

    public static void main(String[] args) throws InterruptedException {
        //1. create configuration
        final Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\Users\\Administrator\\Desktop\\kafka-streams\\stateful-transform-operation");
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);

        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("my-store"),
                Serdes.String(), Serdes.Integer());

        //2. create StreamBuilder
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> ks0 = builder.stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));

        ks0.flatMap((k, v) -> Arrays.stream(v.split("\\s+"))
                                .map(e -> KeyValue.pair(e, e)).collect(Collectors.toList())
                        , Named.as("flatmap-processor"))
                .repartition(Repartitioned.with(Serdes.String(), Serdes.String()))
                .transform(() -> new Transformer<String, String, KeyValue<String, Integer>>() {
                    private KeyValueStore<String, Integer> keyValueStore;

                    @Override
                    public void init(ProcessorContext context) {
                        this.keyValueStore = context.getStateStore(STATE_STORE_NAME);
                    }

                    @Override
                    public KeyValue<String, Integer> transform(String key, String value) {
                        Integer count = keyValueStore.get(key);
                        if (count == null || count == 0) {
                            count = 1;
                        } else {
                            count++;
                        }
                        keyValueStore.put(key, count);
                        return KeyValue.pair(key, count);
                    }

                    @Override
                    public void close() {

                    }
                }, Named.as("transform-processor"), STATE_STORE_NAME)
                .peek((k, v) -> LOG.info("key:{},value:{}", k, v))
                .to(TARGET_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()).withName("sink-processor"));

        builder.addStateStore(storeBuilder);

        //3. create topology
        Topology topology = builder.build();
        //4. create kafka streams
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaStreams.close();
            countDownLatch.countDown();
            LOG.info("the kafka stream application is graceful closed.");
        }));
        //5. start
        kafkaStreams.start();
        LOG.info("the kafka stream application is starting....");
        //6. stop(graceful)
        countDownLatch.await();
    }
}
