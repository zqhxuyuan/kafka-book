package kafka.streams.serde;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by zhengqh on 17/6/20.
 */
class SensorData {

    private Long id;
    private Long value;
    private Long time;

    public void setTime(Long time) {
        this.time = time;
    }

    public Long getTime() {
        return time;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public Long getId() {
        return id;
    }

    public Long getValue() {
        return value;
    }
}

class SensorDataAccumulator {
    ArrayList list = new ArrayList<SensorData>();

    public SensorDataAccumulator add(SensorData s) {
        list.add(s);
        return this;
    }
}

public class SerdeTest {

    public static void main(String[] args) {
        Properties properties = new Properties();
        StreamsConfig streamingConfig = new StreamsConfig(properties);

        Serde<String> stringSerde = Serdes.String();

        JsonDeserializer<SensorData> sensorDataDeserializer = new JsonDeserializer<>(SensorData.class);
        JsonSerializer<SensorData> sensorDataSerializer = new JsonSerializer<>();
        Serde sensorDataSerde = Serdes.serdeFrom(sensorDataSerializer, sensorDataDeserializer);
        JsonDeserializer<SensorData> sensorDataJsonDeserializer = new JsonDeserializer<>(SensorData.class);
        Serde sensorDataJSONSerde = Serdes.serdeFrom(sensorDataSerializer, sensorDataJsonDeserializer);

        StringSerializer stringSerializer = new StringSerializer();
        StringDeserializer stringDeserializer = new StringDeserializer();
        WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(stringSerializer);
        WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(stringDeserializer);
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

        JsonSerializer<SensorDataAccumulator> accSerializer = new JsonSerializer<>();
        JsonDeserializer accDeserializer = new JsonDeserializer<>(SensorDataAccumulator.class);
        Serde<SensorDataAccumulator> accSerde = Serdes.serdeFrom(accSerializer, accDeserializer);


        KStreamBuilder kStreamBuilder = new KStreamBuilder();
        KStream<String,SensorData> initialStream =  kStreamBuilder.stream(stringSerde,sensorDataSerde,"e40_orig");

        final KStream<String, SensorData> sensorDataKStream = initialStream
                .filter((k, v) -> (v != null))
                .map((k, v) -> new KeyValue<>(v.getTime().toString(), v));

//        sensorDataKStream
//                .filter((k, v) -> (v != null))
//                .groupBy((k,v) -> k, stringSerde, sensorDataJSONSerde)
//                .aggregate(SensorDataAccumulator::new,
//                (k, v, list) -> list.add(v),
//                TimeWindows.of(10000),
//                accSerde, "acc")
//        .to(windowedSerde, accSerde, "out");

        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder,streamingConfig);
        kafkaStreams.start();
    }
}
