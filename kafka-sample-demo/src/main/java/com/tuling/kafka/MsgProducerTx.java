package com.tuling.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * https://kafka.apache.org/24/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
 *
 * kafka事务消息
 * @author arthur
 */
public class MsgProducerTx {
    /**
     * kafka topic主题名称
     */
    private final static String TOPIC_NAME = "my-replicated-topic_tx";

    /**
     * kafka集群节点信息
     */
    private final static String KAFKA_BROKER = "192.168.18.100:9092,192.168.18.100:9093,192.168.18.100:9094";

    public static void main(String[] args) {
        Properties props = new Properties();
        //设置事务 id（必须），事务 id 任意起名
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
         /*
         发出消息持久化机制参数
        （1）acks=0： 表示producer不需要等待任何broker确认收到消息的回复，就可以继续发送下一条消息。性能最高，但是最容易丢消息。
        （2）acks=1： 至少要等待leader已经成功将数据写入本地log，但是不需要等待所有follower是否成功写入。就可以继续发送下一条消息。这种情况下，如果follower没有成功备份数据，而此时leader又挂掉，则消息会丢失。
        （3）acks=-1或all： 需要等待 min.insync.replicas(默认为1，推荐配置大于等于2) 这个参数配置的副本个数都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的数据保证。一般除非是金融级别，或跟钱打交道的场景才会使用这种配置。
         */
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        //发送失败会重试，默认重试间隔100ms，重试能保证消息发送的可靠性，但是也可能造成消息重复发送，比如网络抖动，所以需要在接收者那边做好消息接收的幂等性处理
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        //重试间隔设置
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 300);
        //设置发送消息的本地缓冲区，如果设置了该缓冲区，消息会先发送到本地缓冲区，可以提高消息发送性能，默认值是33554432，即32MB
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        /*
          kafka本地线程会从缓冲区取数据，批量发送到broker，
          设置批量发送消息的大小，默认值是16384，即16kb，就是说一个batch满了16kb就发送出去
        */
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        /*
        默认值是0，意思就是消息必须立即被发送，但这样会影响性能
        一般设置10毫秒左右，就是说这个消息发送完后会进入本地的一个batch，如果10毫秒内，这个batch满了16kb就会随batch一起被发送出去
        如果10毫秒内，batch没满，那么也必须把消息发送出去，不能让消息的发送延迟时间太长
        */
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        //把发送的key从字符串序列化为字节数组
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //把发送消息value从字符串序列化为字节数组
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        //初始化事务
        producer.initTransactions();
        try {
            //开启事务
            producer.beginTransaction();
            for (int i = 0; i < 100; i++){
                //发到不同的主题的不同分区
                producer.send(new ProducerRecord<>("hdfs-topic", Integer.toString(i), Integer.toString(i)));
                producer.send(new ProducerRecord<>("es-topic", Integer.toString(i), Integer.toString(i)));
                producer.send(new ProducerRecord<>("redis-topic", Integer.toString(i), Integer.toString(i)));
            }
            //提交事务
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            //回滚事务
            producer.abortTransaction();
        }
        producer.close();
    }
}
