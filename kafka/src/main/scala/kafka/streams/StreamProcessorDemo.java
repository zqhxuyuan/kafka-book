package kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;

/**
 * Created by zhengqh on 17/6/10.
 */
public class StreamProcessorDemo {

    static KStreamBuilder builder = new KStreamBuilder();
    KStream<String, String> sentenceStream = builder.stream(Serdes.String(), Serdes.String(), "input-topic");
    // create store
    StateStoreSupplier myStore = Stores.create("myTransformState")
            .withStringKeys().withStringValues().persistent().build();

    public void testTransform() {
        builder.addStateStore(myStore);

        TransformerSupplier transformerSupplier = new MyTransformer();

        KStream outputStream = sentenceStream.transform(transformerSupplier, "myTransformState");
    }
}

class MyTransformer implements TransformerSupplier<String,String,String> {

    @Override
    public Transformer get() {
        return new Transformer<String,String,String>() {
            private ProcessorContext context;
            private StateStore state;

            public void init(ProcessorContext context) {
                this.context = context;
                this.state = context.getStateStore("myTransformState");
                context.schedule(1000); // call #punctuate() each 1000ms
            }

            @Override
            public String transform(String key, String value) {
                // can access this.state
                // can emit as many new KeyValue pairs as required via this.context#forward()
                return key + value; // can emit a single value via return -- can also be null
            }

            public String punctuate(long timestamp) {
                // can access this.state
                // can emit as many new KeyValue pairs as required via this.context#forward()
                return null; // don't return result -- can also be "new KeyValue()"
            }

            public void close() {
                // can access this.state
                // can emit as many new KeyValue pairs as required via this.context#forward()
            }
        };
    }
}