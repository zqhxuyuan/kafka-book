package kafka.streams.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Map;

/**
 * Serde for instances of `Window` subclasses, as long as they do not contain other instance variables that `start` and `end`.
 * It always deserializes as a `TimeWindow`. An `UnlimitedWindow` is a window with `end` = Long.MAX_VALUE.
 *
 * We do not use {@link org.apache.kafka.streams.kstream.internals.WindowedSerializer} because it does not serialize
 * the window `end`, only the window `start`.
 *
 * @param <T>
 */
public class TimeWindowedSerde<T> implements Serde<Windowed<T>> {

    private final Serde<Windowed<T>> inner;

    public TimeWindowedSerde(Serde<T> serde) {
        inner = Serdes.serdeFrom(
                new TimeWindowedSerializer<>(serde.serializer()),
                new TimeWindowedDeserializer<>(serde.deserializer()));
    }

    @Override
    public Serializer<Windowed<T>> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<Windowed<T>> deserializer() {
        return inner.deserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        inner.serializer().configure(configs, isKey);
        inner.deserializer().configure(configs, isKey);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }

}