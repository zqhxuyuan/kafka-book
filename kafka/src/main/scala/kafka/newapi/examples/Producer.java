/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.newapi.examples;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * bin/kafka-topics.sh --zookeeper localhost:2181/kafka11 --create --topic test --partitions 2 --replication-factor 1
 *
 * 过一段时间后, 修改主题的分区数
 *
 * bin/kafka-topics.sh --zookeeper localhost:2181/kafka11 --alter --topic test --partitions 3
 *
 * 结果:
 *
 * 消息(5, m_5) 发送到分区(0), 偏移量(105) 耗时 8 ms
 消息(1, m_1) 发送到分区(0), 偏移量(106) 耗时 7 ms
 消息(9, m_9) 发送到分区(0), 偏移量(107) 耗时 8 ms
 消息(9, m_9) 发送到分区(0), 偏移量(108) 耗时 13 ms
 消息(6, m_6) 发送到分区(1), 偏移量(78) 耗时 7 ms
 消息(6, m_6) 发送到分区(1), 偏移量(79) 耗时 9 ms
 消息(3, m_3) 发送到分区(1), 偏移量(80) 耗时 9 ms
 消息(4, m_4) 发送到分区(0), 偏移量(109) 耗时 6 ms
 消息(7, m_7) 发送到分区(1), 偏移量(81) 耗时 12 ms
 消息(8, m_8) 发送到分区(0), 偏移量(110) 耗时 19 ms
 消息(10, m_10) 发送到分区(1), 偏移量(82) 耗时 12 ms
 消息(9, m_9) 发送到分区(0), 偏移量(111) 耗时 9 ms
 消息(6, m_6) 发送到分区(1), 偏移量(83) 耗时 10 ms
 消息(10, m_10) 发送到分区(1), 偏移量(84) 耗时 8 ms
 消息(2, m_2) 发送到分区(0), 偏移量(112) 耗时 28 ms
 key 8, before:0, after: 1
 消息(8, m_8) 发送到分区(1), 偏移量(85) 耗时 10 ms
 消息(1, m_1) 发送到分区(0), 偏移量(113) 耗时 13 ms
 key 5, before:0, after: 1
 消息(5, m_5) 发送到分区(1), 偏移量(86) 耗时 9 ms
 key 8, before:0, after: 1
 消息(8, m_8) 发送到分区(1), 偏移量(87) 耗时 11 ms
 消息(3, m_3) 发送到分区(1), 偏移量(88) 耗时 15 ms
 key 9, before:0, after: 2
 消息(9, m_9) 发送到分区(2), 偏移量(0) 耗时 70 ms
 key 7, before:1, after: 2
 消息(7, m_7) 发送到分区(2), 偏移量(1) 耗时 11 ms
 */
public class Producer extends Thread {
  private final KafkaProducer<Long, String> producer;
  private final String topic;
  private final Boolean isAsync;
  private Map<Long, Integer> keyPartitons = new HashMap<>();

  public Producer(String topic, Boolean isAsync) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "192.168.6.52:19093");
    props.put("client.id", "DemoProducer");
    props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("metadata.max.age.ms", "10000");
    producer = new KafkaProducer<>(props);
    this.topic = topic;
    this.isAsync = isAsync;
  }

  public void run() {
    while (true) {
      long messageNo = Math.round(Math.random() * 10);
      String messageStr = "m_" + messageNo;
      long startTime = System.currentTimeMillis();
      if (isAsync) { // Send asynchronously
        producer.send(new ProducerRecord<>(
                topic, messageNo, messageStr), new DemoCallBack(startTime, messageNo, messageStr));
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

    }
  }

  class DemoCallBack implements Callback {

    private final long startTime;
    private final long key;
    private final String message;

    public DemoCallBack(long startTime, long key, String message) {
      this.startTime = startTime;
      this.key = key;
      this.message = message;
    }

    /**
     * A callback method the user can implement to provide asynchronous handling of request completion. This method will
     * be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
     * non-null.
     *
     * @param metadata  The metadata for the record that was sent (i.e. the partition and offset). Null if an error
     *                  occurred.
     * @param exception The exception thrown during processing of this record. Null if no error occurred.
     */
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      long elapsedTime = System.currentTimeMillis() - startTime;
      if (metadata != null) {
        int partition = metadata.partition();
        if(keyPartitons.get(key) == null) {
          keyPartitons.put(key, partition);
        } else {
          if(keyPartitons.get(key) != partition) {
            System.out.println("key " + key + ", before:" + keyPartitons.get(key) + ", after: " + partition);
          }
        }
        System.out.println("消息(" + key + ", " + message + ") " +
                "发送到分区(" + metadata.partition() + "), " +
                "偏移量(" + metadata.offset() + ") " +
                "耗时 " + elapsedTime + " ms");
      } else {
        exception.printStackTrace();
      }
    }
  }
}

