package org.apache.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author arthur
 */
public class FirstStreamsApplication {
    //0. define variable
    private final static Logger LOG= LoggerFactory.getLogger(FirstStreamsApplication.class);
    private final static String APP_ID="first_streams_app_id";
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
        builder.stream(SOURCE_TOPIC, Consumed.with(Serdes.String(),Serdes.String()).withName("source-processor")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                .peek((k,v)->LOG.info("[source] value {}",v),Named.as("pre-transform-peek"))
                .filter((k,v)->v!=null&&v.length()>5,Named.as("filter-processor"))
                .mapValues(v->v.toUpperCase(),Named.as("map-processor"))
                .peek((k,v)->LOG.info("[source] value {}",v),Named.as("post-transform-peek"))
                .to(TARGET_TOPIC, Produced.with(Serdes.String(),Serdes.String()).withName("sink-processor"));
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
