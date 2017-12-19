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
package io.confluent.examples.streams;

import io.confluent.examples.streams.utils.PriorityQueueSerde;
import io.confluent.examples.streams.utils.WindowedSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 *
 * In this example, we count the TopN articles from a stream of page views (aka clickstreams) that
 * reads from a topic named "PageViews". We filter the PageViews stream so that we only consider
 * pages of type article, and then map the recored key to effectively nullify the user, such that
 * the we can count page views by (page, industry). The counts per (page, industry) are then
 * grouped by industry and aggregated into a PriorityQueue with descending order. Finally we
 * perform a mapValues to fetch the top 100 articles per industry.
 * <p>
 * Note: The generic Avro binding is used for serialization/deserialization.  This means the
 * appropriate Avro schema files must be provided for each of the "intermediate" Avro classes, i.e.
 * whenever new types of Avro objects (in the form of GenericRecord) are being passed between
 * processing steps.
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 * <p>
 * 2) Create the input/intermediate/output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic PageViews \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic TopNewsPerIndustry \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * }</pre>
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be {@code bin/kafka-topics.sh ...}.
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.TopArticlesLambdaExample
 * }
 * </pre>
 * 4) Write some input data to the source topics (e.g. via {@link TopArticlesExampleDriver}).
 * The already running example application (step 3) will automatically process this input data and
 * write the results to the output topic. The {@link TopArticlesExampleDriver} will print the
 * results from the output topic
 * <pre>
 * {@code
 * # Here: Write input data using the example driver.  Once the driver has stopped generating data,
 * # you can terminate it via Ctrl-C.
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar io.confluent.examples.streams.TopArticlesExampleDriver
 * }
 * </pre>
 */
public class TopArticlesLambdaExample {

  static final String TOP_NEWS_PER_INDUSTRY_TOPIC = "TopNewsPerIndustry";
  static final String PAGE_VIEWS = "PageViews";

  private static boolean isArticle(final GenericRecord record) {
    final Utf8 flags = (Utf8) record.get("flags");
    if (flags == null) {
      return false;
    }
    return flags.toString().contains("ARTICLE");
  }

  public static void main(final String[] args) throws Exception {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
    final KafkaStreams streams = buildTopArticlesStream(
        bootstrapServers,
        schemaRegistryUrl,
      "/tmp/kafka-streams");
    // Always (and unconditionally) clean local state prior to starting the processing topology.
    // We opt for this unconditional call here because this will make it easier for you to play around with the example
    // when resetting the application for doing a re-run (via the Application Reset Tool,
    // http://docs.confluent.io/current/streams/developer-guide.html#application-reset-tool).
    //
    // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
    // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
    // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
    // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
    // See `ApplicationResetExample.java` for a production-like example.
    streams.cleanUp();
    streams.start();

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  static KafkaStreams buildTopArticlesStream(final String bootstrapServers,
                                             final String schemaRegistryUrl,
                                             final String stateDir) throws IOException {
    final Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "top-articles-lambda-example");
      streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "top-articles-lambda-example-client");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Where to find the Confluent schema registry instance(s)
    streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
    // Records should be flushed every 10 seconds. This is less than the default
    // in order to keep this example interactive.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);

    // Serdes used in this example
    final Serde<String> stringSerde = Serdes.String();

    final Map<String, String> serdeConfig = Collections.singletonMap(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

    final Serde<GenericRecord> keyAvroSerde = new GenericAvroSerde();
    keyAvroSerde.configure(serdeConfig, true);

    final Serde<GenericRecord> valueAvroSerde = new GenericAvroSerde();
    valueAvroSerde.configure(serdeConfig, false);

    final Serde<Windowed<String>> windowedStringSerde = new WindowedSerde<>(stringSerde);

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<byte[], GenericRecord> views = builder.stream(PAGE_VIEWS);

    final InputStream statsSchema = TopArticlesLambdaExample.class.getClassLoader()
        .getResourceAsStream("avro/io/confluent/examples/streams/pageviewstats.avsc");
    final Schema schema = new Schema.Parser().parse(statsSchema);

    final KStream<GenericRecord, GenericRecord> articleViews = views
      // filter only article pages
      .filter((dummy, record) -> isArticle(record))
      // map <page id, industry> as key by making user the same for each record
      .map((dummy, article) -> {
        final GenericRecord clone = new GenericData.Record(article.getSchema());
        clone.put("user", "user");
        clone.put("page", article.get("page"));
        clone.put("industry", article.get("industry"));
        return new KeyValue<>(clone, clone);
      });

    final KTable<Windowed<GenericRecord>, Long> viewCounts = articleViews
      // count the clicks per hour, using tumbling windows with a size of one hour
      .groupByKey(Serialized.with(keyAvroSerde, valueAvroSerde))
      .windowedBy(TimeWindows.of(TimeUnit.MINUTES.toMillis(60)))
      .count();

    final Comparator<GenericRecord> comparator =
      (o1, o2) -> (int) ((Long) o2.get("count") - (Long) o1.get("count"));

    final KTable<Windowed<String>, PriorityQueue<GenericRecord>> allViewCounts = viewCounts
      .groupBy(
        // the selector
        (windowedArticle, count) -> {
          // project on the industry field for key
          Windowed<String> windowedIndustry =
            new Windowed<>(windowedArticle.key().get("industry").toString(),
              windowedArticle.window());
          // add the page into the value
          GenericRecord viewStats = new GenericData.Record(schema);
          viewStats.put("page", windowedArticle.key().get("page"));
          viewStats.put("user", "user");
          viewStats.put("industry", windowedArticle.key().get("industry"));
          viewStats.put("count", count);
          return new KeyValue<>(windowedIndustry, viewStats);
        },
        Serialized.with(windowedStringSerde, valueAvroSerde)
      ).aggregate(
        // the initializer
        () -> new PriorityQueue<>(comparator),

        // the "add" aggregator
        (windowedIndustry, record, queue) -> {
          queue.add(record);
          return queue;
        },

        // the "remove" aggregator
        (windowedIndustry, record, queue) -> {
          queue.remove(record);
          return queue;
        },

        Materialized.with(windowedStringSerde, new PriorityQueueSerde<>(comparator, valueAvroSerde))
        );

    final int topN = 100;
    final KTable<Windowed<String>, String> topViewCounts = allViewCounts
      .mapValues(queue -> {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < topN; i++) {
          final GenericRecord record = queue.poll();
          if (record == null) {
            break;
          }
          sb.append(record.get("page").toString());
          sb.append("\n");
        }
        return sb.toString();
      });

    topViewCounts.toStream().to(TOP_NEWS_PER_INDUSTRY_TOPIC, Produced.with(windowedStringSerde, stringSerde));
    return new KafkaStreams(builder.build(), streamsConfiguration);
  }

}
