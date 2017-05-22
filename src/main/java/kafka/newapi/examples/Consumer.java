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

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class Consumer extends ShutdownableThread {
    private final KafkaConsumer<Integer, String> consumer;
    private String topic;
    private String name;

    public Consumer(String name, String topic) {
        super("KafkaConsumerExample", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.name = name;
        this.topic = topic;
    }

    // 没有放在doWork中,因为doWork方法会被循环调用,而订阅方法只需要执行一次
    public void subscribe(String topic){
        this.topic = topic;
        consumer.subscribe(Collections.singletonList(this.topic), new RebalanceNotifyListener(this.name));
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic), new RebalanceNotifyListener(this.name));

        ConsumerRecords<Integer, String> records = consumer.poll(1000);
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println(this.name +
                    " 接收消息:("+record.key()+","+record.value()+"), " +
                    " 分区:" + record.partition() + ", " +
                    " 偏移量:" + record.offset());
        }
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }
}

class RebalanceNotifyListener implements ConsumerRebalanceListener {
    private String consumerName;

    public RebalanceNotifyListener(String name) {
        this.consumerName = name;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        System.out.println(this.consumerName + " onPartitionsRevoked:"+collection);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        System.out.println(this.consumerName + " onPartitionsAssigned:"+collection);
    }
}
