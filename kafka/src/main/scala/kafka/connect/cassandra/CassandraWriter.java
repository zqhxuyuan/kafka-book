package kafka.connect.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;

import java.util.List;

/**
 * Created by zhengqh on 16/5/30.
 */
public class CassandraWriter implements Runnable {

    private KafkaStream m_stream;
    private int m_threadNumber;
    private int batchSize = 10;

    private CassClient client = new CassClient("test", "localhost","DC1");
    private PreparedStatement insert = null;

    public CassandraWriter(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;

        client.init();
        insert = client.getPrepareSTMT("insert into test.kafka_ip (ip,logs)values(?,?)");
        insert.setConsistencyLevel(ConsistencyLevel.ONE).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
    }

    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()){
            MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
            String key = new String(messageAndMetadata.key());
            String message = new String(messageAndMetadata.message());
            System.out.println(key + "->" + message);
            client.execute(insert, key, message);
        }
    }

    public void batchWrite(final List<String[]> list) {
        BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (String[] kv : list) {
            BoundStatement statement = insert.bind(kv[0], kv[1]);
            batch.add(statement);
        }
        try {
            client.execute(batch);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
