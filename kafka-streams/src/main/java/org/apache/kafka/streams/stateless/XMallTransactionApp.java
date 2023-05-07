package org.apache.kafka.streams.stateless;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.model.Transaction;
import org.apache.kafka.streams.model.TransactionKey;
import org.apache.kafka.streams.model.TransactionPattern;
import org.apache.kafka.streams.model.TransactionReward;
import org.apache.kafka.streams.serdes.JsonSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author arthur
 */
public class XMallTransactionApp {
    private final static Logger LOG = LoggerFactory.getLogger(XMallTransactionApp.class);
    private final static String APP_ID = "xmall-transaction-app";
    private final static String BOOTSTRAP_SERVERS = "192.168.101.100:9092";
    private final static String XMALL_TRANSACTION_SOURCE_TOPIC = "xmall.transaction";
    private final static String XMALL_TRANSACTION_PATTERN_TOPIC = "xmall.pattern.transaction";
    private final static String XMALL_TRANSACTION_REWARDS_TOPIC = "xmall.rewards.transaction";
    private final static String XMALL_TRANSACTION_PURCHASES_TOPIC = "xmall.purchases.transaction";
    private final static String XMALL_TRANSACTION_COFFEE_TOPIC = "xmall.coffee.transaction";
    private final static String XMALL_TRANSACTION_ELECT_TOPIC = "xmall.elect.transaction";
    public static void main(String[] args) throws InterruptedException {
        //1. create configuration
        final Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        //2. create StreamBuilder
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Transaction> ks0 = builder.stream(XMALL_TRANSACTION_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.transaction()).withName("transaction-source-processor")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));

        KStream<String, Transaction> ks1 = ks0.peek((k, v) -> LOG.info("pre masking key:{},value:{}", k, v), Named.as("transaction-source-peek-processor"))
                .mapValues(v -> Transaction.builder(v).maskCreditCard().build(), Named.as("transaction-masking-pii"));

        ks1.mapValues(v -> TransactionPattern.builder(v).build(), Named.as("transaction-pattern-"))
                .to(XMALL_TRANSACTION_PATTERN_TOPIC, Produced.with(Serdes.String(), JsonSerdes.transactionPattern()).withName("transaction-pattern-sink-processor"));

        ks1.mapValues(v -> TransactionReward.builder(v).build(), Named.as("transaction-reward-"))
                .to(XMALL_TRANSACTION_REWARDS_TOPIC, Produced.with(Serdes.String(), JsonSerdes.transactionReward()).withName("transaction-reward-sink-processor"));

        ks1.filter((k,v)->v.getPrice()>5.0)
                        .selectKey((k,v)->new TransactionKey(v.getDepartment(),v.getPurchaseDate()), Named.as("transaction-key-processor"))
                                .to(XMALL_TRANSACTION_PURCHASES_TOPIC, Produced.with(JsonSerdes.transactionKey(), JsonSerdes.transaction()).withName("transaction-purchases-sink-processor"));

        ks1.split(Named.as("transaction-split-processor"))
                .branch((k, v) -> v.getDepartment().equalsIgnoreCase("coffee"),
                        Branched.withConsumer(ks->ks.to(XMALL_TRANSACTION_COFFEE_TOPIC,Produced.with(Serdes.String(),JsonSerdes.transaction()))))
                .branch((k, v) -> v.getDepartment().equalsIgnoreCase("elect"),
                        Branched.withConsumer(ks->ks.to(XMALL_TRANSACTION_ELECT_TOPIC,Produced.with(Serdes.String(),JsonSerdes.transaction()))));

        ks1.foreach((k,v)->LOG.info("simulate located the transaction record(masked) to the data lake,the value:{}", v), Named.as("transaction-sink-processor"));
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
