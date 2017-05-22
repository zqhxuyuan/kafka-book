package clients.disruptor;

import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author jchugh
 */
public class EventLogProducer {

    private final RingBuffer<DisruptorEvent<String, String>> ringBuffer;
    private static final EventTranslatorOneArg<DisruptorEvent<String, String>, ConsumerRecord<String, String>> TRANSLATOR = new EventTranslatorOneArgImpl();

    public EventLogProducer(RingBuffer<DisruptorEvent<String, String>> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void onData(ConsumerRecord<String, String> consumerRecord) {
        ringBuffer.publishEvent(TRANSLATOR, consumerRecord);
    }

}
