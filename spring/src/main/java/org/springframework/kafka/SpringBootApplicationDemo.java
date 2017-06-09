package org.springframework.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class SpringBootApplicationDemo {
    @Bean
    public MyBean myBean () {
        return new MyBean();
    }

    public static void main (String[] args) {
        ApplicationContext ctx =
                SpringApplication.run(SpringBootApplicationDemo.class, args);
        MyBean bean = ctx.getBean(MyBean.class);
        bean.doSomething();
    }

    private static class MyBean {

        public void doSomething () {
            Map<String,Integer> bag = new HashMap<>();
            bag.put("ONE", 6);
            System.out.println("Doing something in MyBean");
            System.out.println(bag);
        }
    }
}