package org.apache.kafka.streams.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 *  bin/kafka-console-producer.sh --topic input.words --bootstrap-server 192.168.103.101:9092 --property parse.key=true --property key.separator=,
 * @author arthur
 */
public class StatelessMapValuesOperation {
    //0. define variable
    private final static Logger LOG= LoggerFactory.getLogger(FirstStreamsApplication.class);
    private final static String APP_ID="stateless_mapvalues_streams_app_id";
    private final static String BOOTSTRAP_SERVER="192.168.103.101:9092";
    private final static String SOURCE_TOPIC="input.words";
    private final static String TARGET_TOPIC="out.words";

    public static void main(String[] args) throws InterruptedException {
        //1. create configuration
        final Properties properties=new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,APP_ID);
        //2. create StreamBuilder
        StreamsBuilder builder=new StreamsBuilder();
        KStream<String, String> kStream = builder.stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));
        KStream<String, String> kStream1 = kStream.mapValues(v -> v.toUpperCase(), Named.as("map-values-processor"));
        KStream<String, String> kStream2 = kStream.mapValues((k, v) -> (k + "----" + v).toUpperCase(), Named.as("map-values-withKey-processor"));
        kStream1.print(Printed.<String,String>toSysOut().withLabel("mapValues"));
        kStream2.print(Printed.<String,String>toSysOut().withLabel("mapValuesWithKey"));
        //3. create topology
        Topology topology = builder.build();
        //4. create kafka streams
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
        CountDownLatch countDownLatch=new CountDownLatch(1);
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
