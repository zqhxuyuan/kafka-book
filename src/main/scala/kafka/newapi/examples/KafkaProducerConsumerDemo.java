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

public class KafkaProducerConsumerDemo {
    public static void main(String[] args) throws Exception{
        //String topic = KafkaProperties.TOPIC;
        String topic = "topic";
        boolean isAsync = args.length == 0 || !args[0].trim().toLowerCase().equals("sync");
        Producer producerThread = new Producer(topic, isAsync);
        producerThread.start();

        Consumer consumerThread = new Consumer("C1", topic);
        consumerThread.start();

        Thread.sleep(1000);

        Consumer consumerThread2 = new Consumer("C2", topic);
        consumerThread2.start();

//        Thread.sleep(1000);
        Thread.sleep(3000);

        Consumer consumerThread3 = new Consumer("C3", topic);
        //consumerThread3.subscribe(topic);
        consumerThread3.start();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                System.out.println("KafkaProducerConsumerDemo shutdown hook executed...");
            }
        });
    }
}
