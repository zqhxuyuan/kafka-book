package clients.disruptor;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * @author jchugh
 */
public class KafkaDisruptor<K, V> {

    private KafkaConsumer<K, V> kafkaConsumer;
    private Disruptor<DisruptorEvent<K, V>> disruptor;
    public static KafkaDisruptorBuilder BUILDER = new KafkaDisruptorBuilder<>();
    private long pollTimeMs;

    private KafkaDisruptor(KafkaConsumer<K, V> kafkaConsumer,
                           Disruptor<DisruptorEvent<K, V>> disruptor,
                           long pollTimeLongMs) {
        this.kafkaConsumer = kafkaConsumer;
        this.disruptor = disruptor;
        this.pollTimeMs = pollTimeLongMs;
    }

    public KafkaConsumer<K, V> getKafkaConsumer() {
        return kafkaConsumer;
    }

    public Disruptor<DisruptorEvent<K, V>> getDisruptor() {
        return disruptor;
    }

    public long getPollTimeMs() {
        return pollTimeMs;
    }

    public static class KafkaDisruptorBuilder<K, V> {

        private KafkaConsumer<K, V> kafkaConsumer;
        private Deserializer<K> kafkaKeyDeserializer;
        private Deserializer<V> kafkaValueDeserializer;
        private String kafkaConsumerGroupId;
        private String kafkaBootstrapServers;
        private EventFactory<DisruptorEvent<K, V>> disruptorEventEventFactory;
        private Executor disruptorExecutor;
        private int disruptorRingBufferSize;
        private WaitStrategy disruptorWaitStrategy;
        private ProducerType disruptorProducerType;
        private Disruptor<DisruptorEvent<K,V>> disruptor;
        private long kafkaConsumerPollTimeMs;

        private boolean isValidConfiguration() {
            return  (  (isKafkaÀlacarte()  && isDisruptorÀlacarte()  && kafkaConsumer == null && disruptor == null)
                    || (!isKafkaÀlacarte() && !isDisruptorÀlacarte() && kafkaConsumer != null && disruptor != null)
                    || (!isKafkaÀlacarte() && isDisruptorÀlacarte()  && kafkaConsumer != null && disruptor == null)
                    || (isKafkaÀlacarte()  && !isDisruptorÀlacarte() && kafkaConsumer == null && disruptor != null));
        }

        private boolean isKafkaÀlacarte() {
            return  (kafkaKeyDeserializer != null
                    && kafkaValueDeserializer != null
                    && kafkaConsumerGroupId != null
                    && kafkaBootstrapServers != null);
        }

        private boolean isDisruptorÀlacarte() {
            return (disruptorEventEventFactory != null
                    && disruptorExecutor != null
                    && disruptorRingBufferSize >= 0
                    && disruptorWaitStrategy != null
                    && disruptorProducerType != null);
        }


        public KafkaDisruptorBuilder<K, V> setKafkaConsumer(KafkaConsumer<K, V> kafkaConsumer) {
            this.kafkaConsumer = kafkaConsumer;
            return this;
        }

        public KafkaDisruptorBuilder<K, V> setKafkaKeyDeserializer(Deserializer<K> kafkaKeyDeserializer) {
            this.kafkaKeyDeserializer = kafkaKeyDeserializer;
            return this;
        }

        public KafkaDisruptorBuilder<K, V> setKafkaValueDeserializer(Deserializer<V> kafkaValueDeserializer) {
            this.kafkaValueDeserializer = kafkaValueDeserializer;
            return this;
        }

        public KafkaDisruptorBuilder<K, V> setKafkaConsumerGroupId(String kafkaConsumerGroupId) {
            this.kafkaConsumerGroupId = kafkaConsumerGroupId;
            return this;
        }

        public KafkaDisruptorBuilder<K, V> setKafkaBootstrapServers(String kafkaBootstrapServers) {
            this.kafkaBootstrapServers = kafkaBootstrapServers;
            return this;
        }

        public KafkaDisruptorBuilder<K, V> setDisruptorEventEventFactory(EventFactory<DisruptorEvent<K, V>> disruptorEventEventFactory) {
            this.disruptorEventEventFactory = disruptorEventEventFactory;
            return this;
        }

        public KafkaDisruptorBuilder<K, V> setDisruptorExecutor(Executor disruptorExecutor) {
            this.disruptorExecutor = disruptorExecutor;
            return this;
        }

        /**
         * Buffer size be smaller than the CPU's L3 Cache Size in KB and must be power of 2
         * @param disruptorRingBufferSize size of the ring buffer
         * @return KafkaConsumerRunnableBuilder
         */
        public KafkaDisruptorBuilder<K, V> setDisruptorRingBufferSize(int disruptorRingBufferSize) {
            this.disruptorRingBufferSize = disruptorRingBufferSize;
            return this;
        }

        public KafkaDisruptorBuilder<K, V> setDisruptorWaitStrategy(WaitStrategy disruptorWaitStrategy) {
            this.disruptorWaitStrategy = disruptorWaitStrategy;
            return this;
        }

        public KafkaDisruptorBuilder<K, V> setDisruptorProducerType(ProducerType disruptorProducerType) {
            this.disruptorProducerType = disruptorProducerType;
            return this;
        }


        public KafkaDisruptorBuilder<K, V> setDisruptor(Disruptor<DisruptorEvent<K, V>> disruptor) {
            this.disruptor = disruptor;
            return this;
        }

        public KafkaDisruptorBuilder<K, V> setKafkaConsumerPollTimeMs(long kafkaConsumerPollTimeMs) {
            this.kafkaConsumerPollTimeMs = kafkaConsumerPollTimeMs;
            return this;
        }

        private Properties createConsumerProperties(String a_groupId, String bootstrapServers) {
            Properties props = new Properties();
            props.put("group.id", a_groupId);
            props.put("auto.commit.interval.ms", "1000");
            props.put("bootstrap.servers", bootstrapServers);
            return props;
        }

        public KafkaDisruptor<K, V> createKafkaDisruptor() {
            if (!isValidConfiguration()) {
                throw new IllegalArgumentException("Either KafkaConsumer should be set, or its components, same goes for disruptor");
            }
            if (isKafkaÀlacarte()) {
                kafkaConsumer = new KafkaConsumer<>(createConsumerProperties(kafkaConsumerGroupId, kafkaBootstrapServers),
                        kafkaKeyDeserializer, kafkaValueDeserializer);
            }
            if (isDisruptorÀlacarte()) {
                disruptor = new Disruptor<>(disruptorEventEventFactory,
                        disruptorRingBufferSize,
                        disruptorExecutor,
                        disruptorProducerType,
                        disruptorWaitStrategy);
            }
            return new KafkaDisruptor<>(kafkaConsumer, disruptor, kafkaConsumerPollTimeMs);
        }
    }
}
