package org.springframework.kafka;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;

public class Consumer {
    public static void main(String[] args) {
        ApplicationContext context = new ClassPathXmlApplicationContext("/spring-consumer.xml");
        QueueChannel queueChannel = (QueueChannel) context.getBean("inputFromKafka");
        Message msg = null;
        while((msg = queueChannel.receive(-1))!=null){
            String payload = (String) msg.getPayload();
            // TODO How to get Partition, Offset from Message? By Listener offered by Spring-Kafka?
            System.out.println(payload);
        }
    }

}