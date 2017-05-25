package base.concurrent.queue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by zhengqh on 16/10/19.

 Produced 0
 Produced 1
 Produced 2
 Produced 3
 Consumed 0
 Produced 4
 Produced 5
 Consumed 1
 Produced 6
 Produced 7
 Consumed 2
 Produced 8

 */
public class ProducerConsumerService {

    public static void main(String[] args) {
        //Creating BlockingQueue of size 10
        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(10);
        Producer producer = new Producer(queue);
        Consumer consumer = new Consumer(queue);
        //starting producer to produce messages in queue
        new Thread(producer).start();
        //starting consumer to consume messages from queue
        new Thread(consumer).start();
        System.out.println("Producer and Consumer has been started");
    }

    public static class Consumer implements Runnable {
        private BlockingQueue<Message> queue;

        public Consumer(BlockingQueue<Message> q){
            this.queue=q;
        }

        @Override
        public void run() {
            try{
                Message msg;
                //consuming messages until exit message is received
                while((msg = queue.take()).getMsg() !="exit"){
                    Thread.sleep(10);
                    System.out.println("Consumed "+msg.getMsg());
                }
            }catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Message {

        private String msg;

        public Message(String str){
            this.msg=str;
        }

        public String getMsg() {
            return msg;
        }
    }

    public static class Producer implements Runnable {

        private BlockingQueue<Message> queue;

        public Producer(BlockingQueue<Message> q){
            this.queue=q;
        }

        @Override
        public void run() {
            //往队列中生产消息
            for(int i=0; i<100; i++){
                Message msg = new Message(""+i);
                try {
                    Thread.sleep(i);
                    queue.put(msg);
                    System.out.println("Produced "+msg.getMsg());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //adding exit message
            Message msg = new Message("exit");
            try {
                queue.put(msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
