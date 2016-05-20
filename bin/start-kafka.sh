cd Soft/kafka_2.10-0.9.0.1

# 启动ZooKeeper服务
bin/zookeeper-server-start.sh config/zookeeper.properties

# 启动Kafka服务
bin/kafka-server-start.sh config/server.properties

#