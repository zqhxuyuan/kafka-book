package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;


/**
 * Generate a String every 500 ms and write it into a Kafka topic
 *
 * Please pass the following arguments to run the example:
 * 	--topic test --bootstrap.servers localhost:9092
 *
 */
public class WriteIntoKafka08 {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if(parameterTool.getNumberOfParameters() < 2) {
            System.out.println("Missing parameters!");
            System.out.println("Usage: Kafka --topic <topic> --bootstrap.servers <kafka brokers>");
            return;
        }

        StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

        // very simple data generator
        DataStream<String> messageStream = env.addSource(new SourceFunction<String>() {
            private static final long serialVersionUID = 6369260445318862378L;
            public boolean running = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                long i = 0;
                while(this.running) {
                    ctx.collect("Element - " + i++);
                    Thread.sleep(500);
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        // write data into Kafka
        messageStream.addSink(new FlinkKafkaProducer08<>(parameterTool.getRequired("topic"), new SimpleStringSchema(), parameterTool.getProperties()));

        env.execute("Write into Kafka example");
    }
}