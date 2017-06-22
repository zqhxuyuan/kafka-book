package kafka.streams.serde;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.Windowed;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by zhengqh on 17/6/20.
 */
public class TimeWindowedSerializer<T> implements Serializer<Windowed<T>> {

    private static final int TIMESTAMP_SIZE = 8;

    private Serializer<T> inner;

    public TimeWindowedSerializer(Serializer<T> inner) {
        this.inner = inner;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public byte[] serialize(String topic, Windowed<T> data) {
        byte[] serializedKey = inner.serialize(topic, data.key());

        ByteBuffer buf = ByteBuffer.allocate(serializedKey.length + TIMESTAMP_SIZE + TIMESTAMP_SIZE);
        buf.put(serializedKey);
        buf.putLong(data.window().start());
        buf.putLong(data.window().end());

        return buf.array();
    }

    @Override
    public void close() {
        inner.close();
    }

    public byte[] serializeBaseKey(String topic, Windowed<T> data) {
        return inner.serialize(topic, data.key());
    }

}