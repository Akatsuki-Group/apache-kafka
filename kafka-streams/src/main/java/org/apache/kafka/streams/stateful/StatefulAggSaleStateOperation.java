package org.apache.kafka.streams.stateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.model.Sale;
import org.apache.kafka.streams.model.SaleState;
import org.apache.kafka.streams.serdes.JsonSerdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.statestore.QueryableStateStoreServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author arthur
 */
public class StatefulAggSaleStateOperation {
    //0. define variable
    private final static Logger LOG = LoggerFactory.getLogger(StatefulAggSaleStateOperation.class);
    private final static String APP_ID = "stateful_aggregate_sales_state_operation";
    private final static String BOOTSTRAP_SERVER = "192.168.103.101:9092";
    private final static String SOURCE_TOPIC = "sales";

    public static void main(String[] args) throws InterruptedException {
        //1. create configuration
        final Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        //properties.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\Users\\Administrator\\Desktop\\kafka-streams\\stateful-transform-operation");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, args[0]);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
        //properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 3);

        //2. create StreamBuilder
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.saleState())
                        .withName("source-processor")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                .selectKey((key, value) -> value.getDepartment())
                .groupByKey(Grouped.with(Serdes.String(), JsonSerdes.saleState()))
                .aggregate(SaleState::new, (dept, sales, salesStatesAggr) -> {
                            if (salesStatesAggr.getDepartment() == null) {
                                salesStatesAggr.setCount(1);
                                salesStatesAggr.setDepartment(sales.getDepartment());
                                salesStatesAggr.setAverageAmount(sales.getSaleAmount());
                                salesStatesAggr.setTotalAmount(sales.getSaleAmount());
                            } else {
                                salesStatesAggr.setCount(salesStatesAggr.getCount() + 1);
                                salesStatesAggr.setTotalAmount(salesStatesAggr.getTotalAmount() + sales.getSaleAmount());
                                salesStatesAggr.setAverageAmount(salesStatesAggr.getTotalAmount() / salesStatesAggr.getCount());
                            }
                            return salesStatesAggr;
                        }, Named.as("aggregate-processor"),
                        Materialized.<String, SaleState, KeyValueStore<Bytes, byte[]>>as("sales-state")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.saleState()))
                .toStream()
                .print(Printed.<String, SaleState>toSysOut().withLabel("sale-state"));
        //3. create topology
        Topology topology = builder.build();
        //4. create kafka streams
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        new QueryableStateStoreServer(kafkaStreams, "sales-state").start();
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
        if (sale.getSaleAmount() != sale.getTotalSalesAmount()) {
            sale.setTotalSalesAmount(sale.getSaleAmount());
        }
        return sale;
    }
}
