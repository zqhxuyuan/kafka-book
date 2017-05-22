package kafka.connect.cassandra;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**

 1. 启动Cassandra, 准备Cassandra库表结构
 CREATE KEYSPACE test WITH replication = {
 'class': 'NetworkTopologyStrategy',
 'DC2': '1',
 'DC1': '1'
 };
 use test;

 CREATE TABLE kafka_ip (
 ip text,
 logs text,
 PRIMARY KEY (ip)
 );

 2. 生产数据,执行ProducerExample

 3. 启动Consumer2Cass

 4. 验证Cassandra中的数据

 cqlsh:test> select * from kafka_ip ;

 ip            | logs
 ---------------+------------------------------------------------
 192.168.2.202 | 87:1464599779759,www.example.com,192.168.2.192
 192.168.2.232 |  33:1464599779111,www.example.com,192.168.2.31

 */
public class Consumer2Cass {
    private static ConsumerConnector consumer;
    private static String topic;
    private static ExecutorService executor;

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
    }

    public static void main(String[] args) {
        args = new String[]{"localhost:2181","kafka-cassandra","test","1"};
        String zooKeeper = args[0];
        String groupId = args[1];
        String topic = args[2];
        int threads = Integer.parseInt(args[3]);

        Properties props = new Properties();
        props.put("zookeeper.connect", zooKeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(threads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        executor = Executors.newFixedThreadPool(threads);

        int threadNumber = 0;
        for (KafkaStream stream : streams) {
            executor.submit(new CassandraWriter(stream, threadNumber));
            threadNumber++;
        }
    }
}

