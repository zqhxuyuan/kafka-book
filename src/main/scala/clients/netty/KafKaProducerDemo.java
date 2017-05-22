package clients.netty;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafKaProducerDemo {

    public  static void inBoundKafka(String topic,String key,String value){
        //kafka producer config
        Properties properties = new Properties ();
        properties.setProperty ("bootstrap.servers","localhost:9092") ;
        properties.setProperty ("acks","all");
        properties.setProperty ("retries","0");
        properties.setProperty ("batch.size","16384");
        properties.setProperty ("linger.ms","1");
        properties.setProperty ("buffer.memory","33554432");
        properties.setProperty ("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty ("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        Producer<String,String> producer = new KafkaProducer<String,String>(properties);
        ProducerRecord producerRecord = new ProducerRecord (topic,key,value);
        Future<RecordMetadata> future= producer.send (producerRecord);

        try {
            RecordMetadata  recordMetadata  =future.get ();
            System.out.println ("producer topic:"+recordMetadata.topic ());
            System.out.println ("producer partition:"+recordMetadata.partition ());
            System.out.println ("producer offset:"+recordMetadata.offset ());
        } catch (InterruptedException e) {
            e.printStackTrace ();
        } catch (ExecutionException e) {
            e.printStackTrace ();
        }finally {
            producer.close ();
        }
    }

}