package org.apache.kafka.streams.stateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.model.Sale;
import org.apache.kafka.streams.serdes.JsonSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * @author arthur
 */
public class StatefulReduceSalesChampionOperation {
    //0. define variable
    private final static Logger LOG = LoggerFactory.getLogger(StatefulReduceSalesChampionOperation.class);
    private final static String APP_ID = "stateful_reduce_sales_operation";
    private final static String BOOTSTRAP_SERVER = "192.168.103.101:9092";
    private final static String SOURCE_TOPIC = "sales";
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
        builder.stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.sale())
                .withName("source-processor")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                .mapValues(StatefulReduceSalesChampionOperation::populateTotalAmount,Named.as("populate-total-amount-processor"))
                .groupBy((k,v)->v.getUserName(),Grouped.with(Serdes.String(),JsonSerdes.sale()))
                .reduce((aggValue,currValue)-> Sale.newBuilder(currValue).accumulateSalesAmount(aggValue.getTotalSalesAmount()).build(),
                        Named.as("accumulate-sales-processor"), Materialized.as("accumulate-sales"))
                .toStream()
                .groupBy((k,v)->v.getDepartment(),Grouped.with(Serdes.String(),JsonSerdes.sale()))
                .reduce((aggValue,currValue)->currValue.getTotalSalesAmount()>aggValue.getTotalSalesAmount()?currValue:aggValue,
                        Named.as("champion-sales"),Materialized.as("champion-sales"))
                .toStream()
                .print(Printed.<String,Sale>toSysOut().withLabel("sales-champion"));
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

    private static Sale populateTotalAmount(Sale sale) {
        if (sale.getSaleAmount() !=sale.getTotalSalesAmount()) {
            sale.setTotalSalesAmount(sale.getSaleAmount());
        }
        return sale;
    }
}
