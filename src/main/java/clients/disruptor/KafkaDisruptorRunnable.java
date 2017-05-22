package clients.disruptor;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author jchugh
 */
public class KafkaDisruptorRunnable<K, V> implements Runnable {

    private final Disruptor<DisruptorEvent<K, V>> disruptor;
    private final RingBuffer<DisruptorEvent<K, V>> ringBuffer;
    private final KafkaConsumer<K, V> kafkaConsumer;
    private final long pollTime;
    private AtomicBoolean running;

    public KafkaDisruptorRunnable(KafkaDisruptor<K, V> kafkaDisruptor) {
        this.kafkaConsumer = kafkaDisruptor.getKafkaConsumer();
        this.disruptor = kafkaDisruptor.getDisruptor();
        this.ringBuffer = kafkaDisruptor.getDisruptor().getRingBuffer();
        this.pollTime = kafkaDisruptor.getPollTimeMs();
        this.disruptor.start();
        running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            kafkaConsumer.wakeup();
            disruptor.shutdown();
        }));
    }

    @Override
    public void run() {
        while(running.get()) {
            Iterable<ConsumerRecord<K, V>> consumerRecords_1 = kafkaConsumer.poll(pollTime);
            for (ConsumerRecord<K, V> consumerRecord : consumerRecords_1) {
                ringBuffer.publishEvent((event, sequence, record) -> event.setConsumerRecord(record), consumerRecord);
            }
        }
    }
}
