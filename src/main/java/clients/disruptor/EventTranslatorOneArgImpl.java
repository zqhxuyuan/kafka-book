package clients.disruptor;


import com.lmax.disruptor.EventTranslatorOneArg;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author jchugh
 */
public class EventTranslatorOneArgImpl implements EventTranslatorOneArg<DisruptorEvent<String, String>, ConsumerRecord<String, String>> {

    @Override
    public void translateTo(DisruptorEvent<String, String> event, long sequence, ConsumerRecord<String, String> consumerRecord) {
        event.setConsumerRecord(consumerRecord);
    }
}
