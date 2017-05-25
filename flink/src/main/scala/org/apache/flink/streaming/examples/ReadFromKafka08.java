package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;


/**
  * Read Strings from Kafka and print them to standard out.
  * Note: On a cluster, DataStream.print() will print to the TaskManager's .out file!
  *
  * Please pass the following arguments to run the example:
  * 	--topic test --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer
  *
  */
public class ReadFromKafka08 {

  public static void main(String[] args) throws Exception {
    // parse input arguments
    final ParameterTool parameterTool = ParameterTool.fromArgs(args);

    if(parameterTool.getNumberOfParameters() < 4) {
      System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> " +
        "--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id>");
      return;
    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().disableSysoutLogging();
    env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
    env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
    env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface

    DataStream<String> messageStream = env
      .addSource(new FlinkKafkaConsumer08<>(
        parameterTool.getRequired("topic"),
        new SimpleStringSchema(),
        parameterTool.getProperties()));

    // write kafka stream to standard out.
    messageStream.print();

    env.execute("Read from Kafka example");
  }
}