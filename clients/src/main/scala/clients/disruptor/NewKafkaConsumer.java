package clients.disruptor;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author jchugh
 */
public class NewKafkaConsumer {
    public static void main(String[] args) {


        int bufferSize = 2048;  // half the size of my l3 cache. nearest to the power of 2 (4096)
        DisruptorEventFactory<String, String> disruptorEventFactory = new DisruptorEventFactory<>();
        Executor disruptorExecutor = Executors.newCachedThreadPool();
        StringDeserializer stringDeserializer = new StringDeserializer();
        long pollTime = 100;

        // For Each Kafka Consumer, create 1 disruptor to achieve higher throughput. (Single Producer per disruptor)
        @SuppressWarnings("unchecked")
        KafkaDisruptor<String, String> kafkaDisruptor = KafkaDisruptor.BUILDER
                .setDisruptorEventEventFactory(disruptorEventFactory)
                .setDisruptorExecutor(disruptorExecutor)
                .setDisruptorProducerType(ProducerType.SINGLE)
                .setDisruptorRingBufferSize(bufferSize)
                .setDisruptorWaitStrategy(new BusySpinWaitStrategy())
                .setKafkaBootstrapServers("localhost:9092")
                .setKafkaConsumerGroupId("t91")
                .setKafkaKeyDeserializer(stringDeserializer)
                .setKafkaConsumerPollTimeMs(pollTime)
                .setKafkaValueDeserializer(stringDeserializer)
                .createKafkaDisruptor();


        kafkaDisruptor.getDisruptor().handleEventsWith(new DisruptorEventHandler());
        kafkaDisruptor.getKafkaConsumer().subscribe(Collections.singletonList("topic1"));

        KafkaDisruptorRunnable<String, String> kafkaDisruptorRunnable = new KafkaDisruptorRunnable<>(kafkaDisruptor);



        ExecutorService executorService = Executors.newFixedThreadPool(3);

        executorService.execute(kafkaDisruptorRunnable);

        while(!executorService.isShutdown());

    }
}
