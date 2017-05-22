package kafka.connect.cassandra;


import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CassClient {
    private static Logger    log                        = LoggerFactory.getLogger(CassClient.class);

    private Cluster          cluster;
    private Session          session;
    private String           keyspace;
    private String[]         agentHosts;
    private String           localDc;

    //读操作的一致性级别
    private ConsistencyLevel readConsistencyLevel       = ConsistencyLevel.ONE;

    public CassClient(){}

    public CassClient(String keyspace, String agentHosts, String localDc) {
        this.keyspace = keyspace;
        this.agentHosts = agentHosts.split(",");
        this.localDc = localDc;
    }

    public void setReadConsistencyLevel(String consistency) {
        readConsistencyLevel = Enum.valueOf(ConsistencyLevel.class, consistency);
    }

    public void init() {
        Builder builder = Cluster.builder();
        for (String host : this.agentHosts) {
            builder.addContactPoints(host);
        }
        builder.withPort(9042);

        cluster = builder.build();
        session = cluster.connect(keyspace);
    }
    public void init2(){
        Builder builder = Cluster.builder();

        PoolingOptions poolingOptions = new PoolingOptions().setMaxConnectionsPerHost(HostDistance.REMOTE, 20).setCoreConnectionsPerHost(HostDistance.REMOTE, 2);
        SocketOptions socketOptions = new SocketOptions().setKeepAlive(true)
                .setReceiveBufferSize(1024 * 1024).setSendBufferSize(1024 * 1024)
                .setConnectTimeoutMillis(5 * 1000).setReadTimeoutMillis(5 * 1000); // read time out
        //TODO 这里设置fetchSize和使用SimpleStatement设置有什么区别??
        //consistencyLevel是查询的一致性级别, 写入的时候也有一个一致性级别的设置
        QueryOptions queryOptions = new QueryOptions().setFetchSize(1000).setConsistencyLevel(readConsistencyLevel);

        //DCAwareRoundRobinPolicy dCAwareRoundRobinPolicy = new DCAwareRoundRobinPolicy(localDc, 0);

        builder.withPoolingOptions(poolingOptions);
        builder.withSocketOptions(socketOptions);
        builder.withQueryOptions(queryOptions);
        builder.withRetryPolicy(DefaultRetryPolicy.INSTANCE);
        //builder.withLoadBalancingPolicy(dCAwareRoundRobinPolicy);
        builder.withCompression(Compression.LZ4);
        for (String host : this.agentHosts) {
            builder.addContactPoints(host);
        }
        builder.withPort(9042);

        cluster = builder.build();
        session = cluster.connect(keyspace);
    }

    public void close() {
        if (null != session) {
            session.close();
        }
        if (null != cluster) {
            cluster.close();
        }
        cluster = null;
    }

    public ResultSet execute(String cql) {
        return session.execute(cql);
    }

    public Session getSession() {
        return session;
    }

    //一行记录, 返回Row
    public Row getOne(String cql, Object... paramValues) {
        ResultSet rs = session.execute(cql, paramValues);
        return rs.one();
    }

    public Row getOne(BoundStatement bstmt) {
        return session.execute(bstmt).one();
    }

    public Row getOne(PreparedStatement pstmt, Object... paramValues) {
        BoundStatement bstmt = pstmt.bind(paramValues);
        return getOne(bstmt);
    }

    //所有记录, 返回List<Row>
    public List<Row> getAll(BoundStatement bstmt) {
        ResultSet rs = session.execute(bstmt);
        return rs.all();
    }

    public List<Row> getAll(PreparedStatement pstmt, Object... paramValues) {
        BoundStatement bstmt = pstmt.bind(paramValues);
        return getAll(bstmt);
    }

    public List<Row> getAllOfSize(BoundStatement bstmt, int size) {
        List<Row> resultRows = new ArrayList<Row>();
        int count = 0;
        ResultSet rs = session.execute(bstmt);
        Iterator<Row> it = rs.iterator();
        while (it.hasNext() && count < size) {
            count++;
            resultRows.add(it.next());
        }
        return resultRows;
    }

    public List<Row> getAllOfSize(PreparedStatement pstmt, int count, Object... paramValues) {
        BoundStatement bstmt = pstmt.bind(paramValues);
        return getAllOfSize(bstmt, count);
    }

    //原始接口, 返回ResultSet.
    public ResultSet execute(Statement stmt) {
        return session.execute(stmt);
    }

    public ResultSet execute(PreparedStatement pstmt, Object... paramValues) {
        BoundStatement bstmt = pstmt.bind(paramValues);
        return session.execute(bstmt);
    }

    //设置Statement的一致性级别和重试策略. Statement可以是查询语句,或者insert插入语句
    public PreparedStatement getPrepareSTMT(String cql){
        PreparedStatement statement = session.prepare(cql);
        statement.setConsistencyLevel(ConsistencyLevel.ONE).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
        return statement;
    }
    public Statement getSimpleSTMT(String cql){
        Statement statement = new SimpleStatement(cql);
        statement.setConsistencyLevel(ConsistencyLevel.ONE).setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
        return statement;
    }

    //带超时的SQL
    public ResultSet getResultSetTimeout(BoundStatement bstmt) {
        ResultSetFuture future = session.executeAsync(bstmt);
        try {
            ResultSet result = future.get(500, TimeUnit.MILLISECONDS);
            return result;
        } catch (TimeoutException e) {
            future.cancel(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    public List<Row> getRowTimeout(BoundStatement bstmt){
        ResultSet resultSet = getResultSetTimeout(bstmt);
        if(null == resultSet) return null;

        List<Row> resultRows = new ArrayList<Row>();
        Iterator<Row> it = resultSet.iterator();
        while (it.hasNext()) {
            resultRows.add(it.next());
        }
        return resultRows;
    }
}