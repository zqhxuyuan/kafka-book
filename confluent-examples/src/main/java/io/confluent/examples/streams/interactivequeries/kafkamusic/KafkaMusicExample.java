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
package io.confluent.examples.streams.interactivequeries.kafkamusic;

import io.confluent.examples.streams.avro.PlayEvent;
import io.confluent.examples.streams.avro.Song;
import io.confluent.examples.streams.avro.SongPlayCount;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;

/**
 * Demonstrates how to locate and query state stores (Interactive Queries).
 *
 * This application continuously computes the latest Top 5 music charts based on song play events
 * collected in real-time in a Kafka topic. This charts data is maintained in a continuously updated
 * state store that can be queried interactively via a REST API.
 *
 * Note: This example uses Java 8 functionality and thus works with Java 8+ only.  But of course you
 * can use the Interactive Queries feature of Kafka Streams also with Java 7.
 *
 * The topology in this example is modelled on a (very) simple streaming music service. It has 2
 * input topics: song-feed and play-events.
 *
 * The song-feed topic contains all of the songs available in the streaming service and is read
 * as a KTable with all songs being stored in the all-songs state store.
 *
 * The play-events topic is a feed of song plays. We filter the play events to only accept events
 * where the duration is >= 30 seconds. We then map the stream so that it is keyed by songId.
 *
 * Now that both streams are keyed the same we can join the play events with the songs, group by
 * the song and count them into a KTable, songPlayCounts, and a state store, song-play-count,
 * to keep track of the number of times each song has been played.
 *
 * Next, we group the songPlayCounts KTable by genre and aggregate into another KTable with the
 * state store, top-five-songs-by-genre, to track the top five songs by genre. Subsequently, we
 * group the same songPlayCounts KTable such that all song plays end up in the same partition. We
 * use this to aggregate the overall top five songs played into the state store, top-five.
 *
 * HOW TO RUN THIS EXAMPLE
 *
 * 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 *
 * 2) Create the input and output topics used by this example.
 *
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic play-events \
 *                    --zookeeper localhost:2181 --partitions 4 --replication-factor 1
 * $ bin/kafka-topics --create --topic song-feed \
 *                    --zookeeper localhost:2181 --partitions 4 --replication-factor 1
 *
 * }
 * </pre>
 *
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be
 * `bin/kafka-topics.sh ...`.
 *
 *
 * 3) Start two instances of this example application either in your IDE or on the command
 * line.
 *
 * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
 *
 * Once packaged you can then start the first instance of the application (on port 7070):
 *
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar \
 *      io.confluent.examples.streams.interactivequeries.kafkamusic.KafkaMusicExample 7070
 * }
 * </pre>
 *
 * Here, `7070` sets the port for the REST endpoint that will be used by this application instance.
 *
 * Then, in a separate terminal, run the second instance of this application (on port 7071):
 *
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-4.0.0-SNAPSHOT-standalone.jar \
 *      io.confluent.examples.streams.interactivequeries.kafkamusic.KafkaMusicExample 7071
 * }
 * </pre>
 *
 *
 * 4) Write some input data to the source topics (e.g. via {@link KafkaMusicExampleDriver}). The
 * already running example application (step 3) will automatically process this input data
 *
 *
 * 5) Use your browser to hit the REST endpoint of the app instance you started in step 3 to query
 * the state managed by this application.  Note: If you are running multiple app instances, you can
 * query them arbitrarily -- if an app instance cannot satisfy a query itself, it will fetch the
 * results from the other instances.
 *
 * For example:
 *
 * <pre>
 * {@code
 * # List all running instances of this application
 * http://localhost:7070/kafka-music/instances
 *
 * # List app instances that currently manage (parts of) state store "song-play-count"
 * http://localhost:7070/kafka-music/instances/song-play-count
 *
 * # Get the latest top five for the genre "punk"
 * http://localhost:7070/kafka-music/charts/genre/punk
 *
 * # Get the latest top five across all genres
 * http://localhost:7070/kafka-music/charts/top-five
 * }
 * </pre>
 *
 * Note: that the REST functionality is NOT part of Kafka Streams or its API. For demonstration
 * purposes of this example application, we decided to go with a simple, custom-built REST API that
 * uses the Interactive Queries API of Kafka Streams behind the scenes to expose the state stores of
 * this application via REST.
 *
 * 6) Once you're done with your experiments, you can stop this example via `Ctrl-C`.  If needed,
 * also stop the Schema Registry (`Ctrl-C`), the Kafka broker (`Ctrl-C`), and only then stop the ZooKeeper instance
 * (`Ctrl-C`).
 *
 * If you like you can run multiple instances of this example by passing in a different port. You
 * can then experiment with seeing how keys map to different instances etc.
 */

public class KafkaMusicExample {

  private static final Long MIN_CHARTABLE_DURATION = 30 * 1000L;
  private static final String SONG_PLAY_COUNT_STORE = "song-play-count";
  static final String PLAY_EVENTS = "play-events";
  static final String ALL_SONGS = "all-songs";
  static final String SONG_FEED = "song-feed";
  static final String TOP_FIVE_SONGS_BY_GENRE_STORE = "top-five-songs-by-genre";
  static final String TOP_FIVE_SONGS_STORE = "top-five-songs";
  static final String TOP_FIVE_KEY = "all";

  private static final String DEFAULT_REST_ENDPOINT_HOSTNAME = "localhost";
  private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String DEFAULT_SCHEMA_REGISTRY_URL = "http://localhost:8081";

  public static void main(String[] args) throws Exception {
    if (args.length == 0 || args.length > 4) {
      throw new IllegalArgumentException("usage: ... <portForRestEndpoint> " +
          "[<bootstrap.servers> (optional, default: " + DEFAULT_BOOTSTRAP_SERVERS + ")] " +
          "[<schema.registry.url> (optional, default: " + DEFAULT_SCHEMA_REGISTRY_URL + ")] " +
          "[<hostnameForRestEndPoint> (optional, default: " + DEFAULT_REST_ENDPOINT_HOSTNAME + ")]");
    }
    final int restEndpointPort = Integer.valueOf(args[0]);
    final String bootstrapServers = args.length > 1 ? args[1] : "localhost:9092";
    final String schemaRegistryUrl = args.length > 2 ? args[2] : "http://localhost:8081";
    final String restEndpointHostname = args.length > 3 ? args[3] : DEFAULT_REST_ENDPOINT_HOSTNAME;
    final HostInfo restEndpoint = new HostInfo(restEndpointHostname, restEndpointPort);

    System.out.println("Connecting to Kafka cluster via bootstrap servers " + bootstrapServers);
    System.out.println("Connecting to Confluent schema registry at " + schemaRegistryUrl);
    System.out.println("REST endpoint at http://" + restEndpointHostname + ":" + restEndpointPort);

    final KafkaStreams streams = createChartsStreams(bootstrapServers,
                                                     schemaRegistryUrl,
                                                     restEndpointPort,
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

    // Now that we have finished the definition of the processing topology we can actually run
    // it via `start()`.  The Streams application as a whole can be launched just like any
    // normal Java application that has a `main()` method.
    streams.start();

    // Start the Restful proxy for servicing remote access to state stores
    final MusicPlaysRestService restService = startRestProxy(streams, restEndpoint);

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        restService.stop();
        streams.close();
      } catch (Exception e) {
        // ignored
      }
    }));
  }

  static MusicPlaysRestService startRestProxy(final KafkaStreams streams, final HostInfo hostInfo)
      throws Exception {
    final MusicPlaysRestService
        interactiveQueriesRestService = new MusicPlaysRestService(streams, hostInfo);
    interactiveQueriesRestService.start();
    return interactiveQueriesRestService;
  }

  static KafkaStreams createChartsStreams(final String bootstrapServers,
                                          final String schemaRegistryUrl,
                                          final int applicationServerPort,
                                          final String stateDir) {
    final Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-music-charts");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Provide the details of our embedded http service that we'll use to connect to this streams
    // instance and discover locations of stores.
    streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + applicationServerPort);
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
    // Set to earliest so we don't miss any data that arrived in the topics before the process
    // started
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Set the commit interval to 500ms so that any changes are flushed frequently and the top five
    // charts are updated with low latency.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);
    // Allow the user to fine-tune the `metadata.max.age.ms` via Java system properties from the CLI.
    // Lowering this parameter from its default of 5 minutes to a few seconds is helpful in
    // situations where the input topic was not pre-created before running the application because
    // the application will discover a newly created topic faster.  In production, you would
    // typically not change this parameter from its default.
    String metadataMaxAgeMs = System.getProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG);
    if (metadataMaxAgeMs != null) {
      try {
        int value = Integer.parseInt(metadataMaxAgeMs);
        streamsConfiguration.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, value);
        System.out.println("Set consumer configuration " + ConsumerConfig.METADATA_MAX_AGE_CONFIG +
            " to " + value);
      } catch (NumberFormatException ignored) {
      }
    }

    // create and configure the SpecificAvroSerdes required in this example
    final Map<String, String> serdeConfig = Collections.singletonMap(
        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

    final SpecificAvroSerde<PlayEvent> playEventSerde = new SpecificAvroSerde<>();
    playEventSerde.configure(serdeConfig, false);

    final SpecificAvroSerde<Song> keySongSerde = new SpecificAvroSerde<>();
    keySongSerde.configure(serdeConfig, true);

    final SpecificAvroSerde<Song> valueSongSerde = new SpecificAvroSerde<>();
    valueSongSerde.configure(serdeConfig, false);

    final SpecificAvroSerde<SongPlayCount> songPlayCountSerde = new SpecificAvroSerde<>();
    songPlayCountSerde.configure(serdeConfig, false);

    final StreamsBuilder builder = new StreamsBuilder();

    // get a stream of play events
    final KStream<String, PlayEvent> playEvents = builder.stream(
        PLAY_EVENTS,
        Consumed.with(Serdes.String(), playEventSerde));

    // get table and create a state store to hold all the songs in the store
    final KTable<Long, Song>
        songTable =
        builder.table(SONG_FEED, Materialized.<Long, Song, KeyValueStore<Bytes, byte[]>>as(ALL_SONGS)
            .withKeySerde(Serdes.Long())
            .withValueSerde(valueSongSerde));

    // Accept play events that have a duration >= the minimum
    final KStream<Long, PlayEvent> playsBySongId =
        playEvents.filter((region, event) -> event.getDuration() >= MIN_CHARTABLE_DURATION)
            // repartition based on song id
            .map((key, value) -> KeyValue.pair(value.getSongId(), value));


    // join the plays with song as we will use it later for charting
    final KStream<Long, Song> songPlays = playsBySongId.leftJoin(songTable,
        (value1, song) -> song,
        Joined.with(Serdes.Long(), playEventSerde, valueSongSerde));

    // create a state store to track song play counts
    final KTable<Song, Long> songPlayCounts = songPlays.groupBy((songId, song) -> song,
                                                                Serialized.with(keySongSerde, valueSongSerde))
            .count(Materialized.<Song, Long, KeyValueStore<Bytes, byte[]>>as(SONG_PLAY_COUNT_STORE)
                           .withKeySerde(valueSongSerde)
                           .withValueSerde(Serdes.Long()));

    final TopFiveSerde topFiveSerde = new TopFiveSerde();


    // Compute the top five charts for each genre. The results of this computation will continuously update the state
    // store "top-five-songs-by-genre", and this state store can then be queried interactively via a REST API (cf.
    // MusicPlaysRestService) for the latest charts per genre.
    songPlayCounts.groupBy((song, plays) ->
            KeyValue.pair(song.getGenre().toLowerCase(),
                new SongPlayCount(song.getId(), plays)),
        Serialized.with(Serdes.String(), songPlayCountSerde))
        // aggregate into a TopFiveSongs instance that will keep track
        // of the current top five for each genre. The data will be available in the
        // top-five-songs-genre store
        .aggregate(TopFiveSongs::new,
            (aggKey, value, aggregate) -> {
              aggregate.add(value);
              return aggregate;
            },
            (aggKey, value, aggregate) -> {
              aggregate.remove(value);
              return aggregate;
            },
            Materialized.<String, TopFiveSongs, KeyValueStore<Bytes, byte[]>>as(TOP_FIVE_SONGS_BY_GENRE_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(topFiveSerde)
        );

    // Compute the top five chart. The results of this computation will continuously update the state
    // store "top-five-songs", and this state store can then be queried interactively via a REST API (cf.
    // MusicPlaysRestService) for the latest charts per genre.
    songPlayCounts.groupBy((song, plays) ->
            KeyValue.pair(TOP_FIVE_KEY,
                new SongPlayCount(song.getId(), plays)),
        Serialized.with(Serdes.String(), songPlayCountSerde))
        .aggregate(TopFiveSongs::new,
            (aggKey, value, aggregate) -> {
              aggregate.add(value);
              return aggregate;
            },
            (aggKey, value, aggregate) -> {
              aggregate.remove(value);
              return aggregate;
            },
            Materialized.<String, TopFiveSongs, KeyValueStore<Bytes, byte[]>>as(TOP_FIVE_SONGS_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(topFiveSerde)
        );

    return new KafkaStreams(builder.build(), streamsConfiguration);

  }

  /**
   * Serde for TopFiveSongs
   */
  private static class TopFiveSerde implements Serde<TopFiveSongs> {

    @Override
    public void configure(final Map<String, ?> map, final boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<TopFiveSongs> serializer() {
      return new Serializer<TopFiveSongs>() {
        @Override
        public void configure(final Map<String, ?> map, final boolean b) {
        }

        @Override
        public byte[] serialize(final String s, final TopFiveSongs topFiveSongs) {
          final ByteArrayOutputStream out = new ByteArrayOutputStream();
          final DataOutputStream
              dataOutputStream =
              new DataOutputStream(out);
          try {
            for (SongPlayCount songPlayCount : topFiveSongs) {
                dataOutputStream.writeLong(songPlayCount.getSongId());
                dataOutputStream.writeLong(songPlayCount.getPlays());
            }
            dataOutputStream.flush();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
            return out.toByteArray();
        }

        @Override
        public void close() {

        }
      };
    }

    @Override
    public Deserializer<TopFiveSongs> deserializer() {
      return new Deserializer<TopFiveSongs>() {
        @Override
        public void configure(final Map<String, ?> map, final boolean b) {

        }

        @Override
        public TopFiveSongs deserialize(final String s, final byte[] bytes) {
          if (bytes == null || bytes.length == 0) {
            return null;
          }
          final TopFiveSongs result = new TopFiveSongs();

          final DataInputStream
              dataInputStream =
              new DataInputStream(new ByteArrayInputStream(bytes));

          try {
            while(dataInputStream.available() > 0) {
              result.add(new SongPlayCount(dataInputStream.readLong(),
                                           dataInputStream.readLong()));
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return result;
        }

        @Override
        public void close() {

        }
      };
    }
  }

  /**
   * Used in aggregations to keep track of the Top five songs
   */
  static class TopFiveSongs implements Iterable<SongPlayCount> {
    private final Map<Long, SongPlayCount> currentSongs = new HashMap<>();
    private final TreeSet<SongPlayCount> topFive = new TreeSet<>((o1, o2) -> {
      final int result = o2.getPlays().compareTo(o1.getPlays());
      if (result != 0) {
        return result;
      }
      return o1.getSongId().compareTo(o2.getSongId());
    });

    public void add(final SongPlayCount songPlayCount) {
      if(currentSongs.containsKey(songPlayCount.getSongId())) {
        topFive.remove(currentSongs.remove(songPlayCount.getSongId()));
      }
      topFive.add(songPlayCount);
      currentSongs.put(songPlayCount.getSongId(), songPlayCount);
      if (topFive.size() > 5) {
        final SongPlayCount last = topFive.last();
        currentSongs.remove(last.getSongId());
        topFive.remove(last);
      }
    }

    void remove(final SongPlayCount value) {
      topFive.remove(value);
      currentSongs.remove(value.getSongId());
    }


    @Override
    public Iterator<SongPlayCount> iterator() {
      return topFive.iterator();
    }
  }


}
