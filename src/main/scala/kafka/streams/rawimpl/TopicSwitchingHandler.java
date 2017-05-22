package kafka.streams.rawimpl;


import kafka.consumer.BaseConsumerRecord;
import kafka.message.MessageAndMetadata;
import kafka.tools.MirrorMaker;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.List;

/**
 * Using a MessageHandler in MirrorMaker
 ======================================

 * Add the jar containing the handler to the class path. For example:

 `export CLASSPATH=$CLASSPATH:/Users/gwen/workspaces/kafka-examples/MirrorMakerHandler/target/TopicSwitchingHandler-1.0-SNAPSHOT.jar`

 * Start MirrorMaker. Specify the Handler class in "--message.handler" and any arguments in "--message.handler.args". For example:

 `bin/kafka-mirror-maker.sh --consumer.config config/consumer.properties --message.handler com.shapira.examples.TopicSwitchingHandler --message.handler.args dc1 --producer.config config/producer.properties --whitelist mm1`

 * Test it with a producer on source topic and consumer on destination:

 `bin/kafka-console-producer.sh --topic mm1 --broker-list localhost:9092`

 `bin/kafka-console-consumer.sh --topic dc1.mm1 --zookeeper localhost:2181 --from-beginning`

 */
public class TopicSwitchingHandler implements MirrorMaker.MirrorMakerMessageHandler {

    private final String topicPrefix;

    public TopicSwitchingHandler(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    public List<ProducerRecord<byte[], byte[]>> handle(MessageAndMetadata<byte[], byte[]> record) {
        return Collections.singletonList(new ProducerRecord<byte[], byte[]>(topicPrefix + "." + record.topic(), record.partition(), record.key(), record.message()));
    }

    public List<ProducerRecord<byte[], byte[]>> handle(BaseConsumerRecord record) {
        return Collections.singletonList(new ProducerRecord<byte[], byte[]>(topicPrefix + "." + record.topic(), record.partition(), record.key(), record.value()));
    }
}