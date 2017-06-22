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

package kafka.streams.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

/**
 * Demonstrates, using the high-level KStream DSL, how to implement the WordCount program
 * that computes a simple word occurrence histogram from an input text.
 *
 * In this example, the input stream reads from a topic named "streams-file-input", where the values of messages
 * represent lines of text; and the histogram output is written to topic "streams-wordcount-output" where each record
 * is an updated count of a single word.
 *
 * Before running this example you must create the source topic (e.g. via bin/kafka-topics.sh --create ...)
 * and write some data to it (e.g. via bin-kafka-console-producer.sh). Otherwise you won't see any data arriving in the output topic.
 */
public class WordCountStreamAndTableDemo {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dsl-wc1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        //props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> source = builder.stream("dsl-input1");
        KTable<String, Long> countTable = source
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .map((key,value) -> KeyValue.pair(value, value))
                .groupByKey().count("Counts1");

//        [KSTREAM-AGGREGATE-0000000003]: hello , (1<-null)
//        [KSTREAM-AGGREGATE-0000000003]: kafka , (1<-null)
//        [KSTREAM-AGGREGATE-0000000003]: hello , (2<-null)
//        [KSTREAM-AGGREGATE-0000000003]: kafka , (2<-null)
//        [KSTREAM-AGGREGATE-0000000003]: streams , (1<-null)
//        countTable.print();  // print before to

        countTable.to(Serdes.String(), Serdes.Long(), "ktable-output1");

//        countTable.print();  // print after to, output is the same as print before to

        KStream<String, Long> countStream = countTable.toStream();
        countStream.to(Serdes.String(), Serdes.Long(), "kstream-output1");

//        KTable<String, Long> table1 = countTable.through(Serdes.String(), Serdes.Long(), "ktable-output1", "Counts_1");
//        table1.print();

        //------------------------

        // Stream Table Durability
//        [KTABLE-SOURCE-0000000010]: hello , (1<-null)
//        [KTABLE-SOURCE-0000000010]: kafka , (1<-null)
//        [KTABLE-SOURCE-0000000010]: hello , (2<-null)
//        [KTABLE-SOURCE-0000000010]: kafka , (2<-null)
//        [KTABLE-SOURCE-0000000010]: streams , (1<-null)
//        KTable<String, Long> table1 = builder.table(Serdes.String(), Serdes.Long(), "ktable-output1", "Counts_1");
//        table1.print();

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        Thread.sleep(60000L);

        streams.close();
    }
}
