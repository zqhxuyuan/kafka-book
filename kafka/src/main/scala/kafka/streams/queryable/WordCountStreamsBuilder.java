package kafka.streams.queryable;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author laclefyoshi
 */
public class WordCountStreamsBuilder {
  private final Serde<String> stringSerde = Serdes.String();
  private final Serde<Long> longSerde = Serdes.Long();

  private String brokers;
  private String zookeepers;
  private String inputStream;
  private String outputStream;
  private String storeName;

  public WordCountStreamsBuilder(final String brkrs, final String zks,
                                 final String iStream, final String oStream,
                                 final String sName) {
    brokers = brkrs;
    zookeepers = zks;
    inputStream = iStream;
    outputStream = oStream;
    storeName = sName;
  }

  public KafkaStreams build() {
    KStreamBuilder builder = new KStreamBuilder();
    KStream<String, String> textLines =
            builder.stream(stringSerde, stringSerde, inputStream);
    KStream<String, Long> wordCounts =
            textLines
                    .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                    .groupBy((key, word) -> word)
                    .count(storeName)
                    .toStream();
    wordCounts.to(stringSerde, longSerde, outputStream);
    KafkaStreams streams = new KafkaStreams(builder, makeConf());
    return streams;
  }

  private Properties makeConf() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG,
            "word-count-streams-application");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);
    props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG,
            Serdes.String().getClass().getName());
    props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG,
            Serdes.String().getClass().getName());
    return props;
  }
}
