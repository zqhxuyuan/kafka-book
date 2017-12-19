/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams

import java.util.Properties

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{KStream, Produced}

/**
  * Demonstrates how to perform simple, state-less transformations via map functions.
  * Same as [[MapFunctionLambdaExample]] but in Scala.
  *
  * Use cases include e.g. basic data sanitization, data anonymization by obfuscating sensitive data
  * fields (such as personally identifiable information aka PII).  This specific example reads
  * incoming text lines and converts each text line to all-uppercase.
  *
  * Requires a version of Scala that supports Java 8 and SAM / Java lambda (e.g. Scala 2.11 with
  * `-Xexperimental` compiler flag, or 2.12).
  *
  * HOW TO RUN THIS EXAMPLE
  *
  * 1) Start Zookeeper and Kafka.
  * Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
  *
  * 2) Create the input and output topics used by this example.
  *
  * {{{
  * $ bin/kafka-topics --create --topic TextLinesTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1
  * $ bin/kafka-topics --create --topic UppercasedTextLinesTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1
  * $ bin/kafka-topics --create --topic OriginalAndUppercasedTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1
  * }}}
  *
  * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be
  * `bin/kafka-topics.sh ...`.
  *
  * 3) Start this example application either in your IDE or on the command line.
  *
  * If via the command line please refer to
  * <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
  * Once packaged you can then run:
  *
  * {{{
  * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.MapFunctionScalaExample
  * }}}
  *
  * 4) Write some input data to the source topics (e.g. via `kafka-console-producer`.  The already
  * running example application (step 3) will automatically process this input data and write the
  * results to the output topics.
  *
  * {{{
  * # Start the console producer.  You can then enter input data by writing some line of text,
  * # followed by ENTER:
  * #
  * #   hello kafka streams<ENTER>
  * #   all streams lead to kafka<ENTER>
  * #
  * # Every line you enter will become the value of a single Kafka message.
  * $ bin/kafka-console-producer --broker-list localhost:9092 --topic TextLinesTopic
  * }}}
  *
  * 5) Inspect the resulting data in the output topics, e.g. via `kafka-console-consumer`.
  *
  * {{{
  * $ bin/kafka-console-consumer --new-consumer --bootstrap-server localhost:9092 --topic UppercasedTextLinesTopic --from-beginning
  * $ bin/kafka-console-consumer --new-consumer --bootstrap-server localhost:9092 --topic OriginalAndUppercasedTopic --from-beginning
  * }}}
  *
  * You should see output data similar to:
  * {{{
  * HELLO KAFKA STREAMS
  * ALL STREAMS LEAD TO KAFKA
  * }}}
  *
  * 6) Once you're done with your experiments, you can stop this example via `Ctrl-C`.  If needed,
  * also stop the Kafka broker (`Ctrl-C`), and only then stop the ZooKeeper instance (`Ctrl-C`).
  */
object MapFunctionScalaExample {

  def main(args: Array[String]) {
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    val builder = new StreamsBuilder

    val streamingConfig = {
      val settings = new Properties
      settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-scala-example")
      settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      // Specify default (de)serializers for record keys and for record values.
      settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass.getName)
      settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      settings
    }

    val stringSerde: Serde[String] = Serdes.String()

    // Read the input Kafka topic into a KStream instance.
    val textLines: KStream[Array[Byte], String] = builder.stream("TextLinesTopic")

    // Variant 1: using `mapValues`
    val uppercasedWithMapValues: KStream[Array[Byte], String] = textLines.mapValues(_.toUpperCase())

    // Write (i.e. persist) the results to a new Kafka topic called "UppercasedTextLinesTopic".
    //
    // In this case we can rely on the default serializers for keys and values because their data
    // types did not change, i.e. we only need to provide the name of the output topic.
    uppercasedWithMapValues.to("UppercasedTextLinesTopic")

    // We are using implicit conversions to convert Scala's `Tuple2` into Kafka Streams' `KeyValue`.
    // This allows us to write streams transformations as, for example:
    //
    //    map((key, value) => (key, value.toUpperCase())
    //
    // instead of the more verbose
    //
    //    map((key, value) => new KeyValue(key, value.toUpperCase())
    //
    import KeyValueImplicits._

    // Variant 2: using `map`, modify value only (equivalent to variant 1)
    val uppercasedWithMap: KStream[Array[Byte], String] = textLines.map((key, value) => (key, value.toUpperCase()))

    // Variant 3: using `map`, modify both key and value
    //
    // Note: Whether, in general, you should follow this artificial example and store the original
    //       value in the key field is debatable and depends on your use case.  If in doubt, don't
    //       do it.
    val originalAndUppercased: KStream[String, String] = textLines.map((key, value) => (value, value.toUpperCase()))

    // Write the results to a new Kafka topic "OriginalAndUppercasedTopic".
    //
    // In this case we must explicitly set the correct serializers because the default serializers
    // (cf. streaming configuration) do not match the type of this particular KStream instance.
    originalAndUppercased.to("OriginalAndUppercasedTopic", Produced.`with`(stringSerde, stringSerde))

    val stream: KafkaStreams = new KafkaStreams(builder.build(), streamingConfig)
    stream.start()
  }

}
