package org.springframework.kafka;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;

public class Producer {

    public static void main(String[] args) {
        final ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("/spring-producer.xml", Producer.class);
        context.start();

        MessageChannel messageChannel = (MessageChannel) context.getBean("inputToKafka");
        for(int i=0;i<15;i++){
            Message<String> message = new GenericMessage<String>("test");
            boolean flag = messageChannel.send(message);
            System.out.println(flag);
        }

        try {
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        context.close();
    }

}