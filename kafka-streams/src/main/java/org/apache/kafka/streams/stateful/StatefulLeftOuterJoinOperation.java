package org.apache.kafka.streams.stateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author arthur
 */
public class StatefulLeftOuterJoinOperation {
    //0. define variable
    private final static Logger LOG = LoggerFactory.getLogger(StatefulLeftOuterJoinOperation.class);
    private final static String APP_ID = "stateful_left_outer_join_operation";
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
        KStream<String, String> ks0 = builder.stream(USER_NAME_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
        KStream<String, String> ks1 = builder.stream(USER_ADDRESS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor"));

        KStream<String, String> userKs = ks0.map((k, v) -> KeyValue.pair(v.split(",")[0], v), Named.as("user-info-transform"));
        KStream<String, String> addressKs = ks1.map((k, v) -> KeyValue.pair(v.split(",")[0], v), Named.as("user-address-transform"));
        userKs.outerJoin(addressKs, (left, right) -> left + "-----" + right, JoinWindows.of(Duration.ofMillis(1)),
                        StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()))
                .print(Printed.<String, String>toSysOut().withLabel("left-outer-join"));
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
