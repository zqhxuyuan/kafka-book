package kafka.streams.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by zhengqh on 17/6/20.
 *
 * https://gist.github.com/nfo/eaf350afb5667a3516593da4d48e757a
 */
public class TimeWindowedDeserializer<T> implements Deserializer<Windowed<T>> {

    private static final int TIMESTAMP_SIZE = 8;

    private Deserializer<T> inner;

    public TimeWindowedDeserializer(Deserializer<T> inner) {
        this.inner = inner;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public Windowed<T> deserialize(String topic, byte[] data) {

        // Read the inner data
        byte[] bytes = new byte[data.length - TIMESTAMP_SIZE - TIMESTAMP_SIZE];
        System.arraycopy(data, 0, bytes, 0, bytes.length);

        // Read the start timestamp
        byte[] startBytes = new byte[TIMESTAMP_SIZE];
        System.arraycopy(data, data.length - TIMESTAMP_SIZE - TIMESTAMP_SIZE, startBytes, 0, startBytes.length);
        long start = ByteBuffer.wrap(startBytes).getLong(0);

        // Read the end timestamp
        byte[] endBytes = new byte[TIMESTAMP_SIZE];
        System.arraycopy(data, data.length - TIMESTAMP_SIZE, endBytes, 0, endBytes.length);
        long end = ByteBuffer.wrap(endBytes).getLong(0);

        // Read as a `TimeWindow`.
        // An `UnlimitedWindow` is just a window with an end equal to `Long.MAX_VALUE`.
        // And consumer code should should only use the superclass `Window`.
        return new Windowed<T>(inner.deserialize(topic, bytes), new TimeWindow(start, end));
    }


    @Override
    public void close() {
        inner.close();
    }
}