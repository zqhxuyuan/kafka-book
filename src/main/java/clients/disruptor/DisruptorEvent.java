package clients.disruptor;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author jchugh
 */
public class DisruptorEvent <K, V> {
    private ConsumerRecord<K, V> consumerRecord;

    public void setConsumerRecord(ConsumerRecord<K, V> consumerRecord) {
        this.consumerRecord = consumerRecord;
    }

    public ConsumerRecord<K, V> getConsumerRecord() {
        return consumerRecord;
    }
}
