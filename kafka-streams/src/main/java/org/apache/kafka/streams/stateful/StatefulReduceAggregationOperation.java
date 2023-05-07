package org.apache.kafka.streams.stateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * @author arthur
 */
public class StatefulReduceAggregationOperation {
    //0. define variable
    private final static Logger LOG = LoggerFactory.getLogger(StatefulReduceAggregationOperation.class);
    private final static String APP_ID = "stateful_reduce_aggregation_operation";
    private final static String BOOTSTRAP_SERVER = "192.168.103.101:9092";
    private final static String USER_NAME_TOPIC = "user.info";
    private final static String USER_ADDRESS_TOPIC = "user.address";
    private final static String STATE_STORE_NAME = "stateful_transform_operation";

    public static void main(String[] args) throws InterruptedException {
        //1. create configuration
        final Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\Users\\Administrator\\Desktop\\kafka-streams\\stateful-transform-operation");
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);

        //2. create StreamBuilder
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(USER_NAME_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                .flatMap((k,v)-> Arrays.stream(v.split("\\s+"))
                        .map(e->KeyValue.pair(e,1L)).collect(Collectors.toList()), Named.as("flatmap-processor"))
                .groupByKey(Grouped.with(Serdes.String(),Serdes.Long()))
                .reduce(Long::sum, Named.as("reduce-processor"), Materialized.as("reduce-state-store"))
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel("wc"));
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
