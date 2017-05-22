rm -rf /tmp/zookeeper
rm -rf /tmp/kafka-logs

# 进入kafka目录
cd Soft/kafka_2.10-0.10.0.0

# 启动ZooKeeper服务
nohup bin/zookeeper-server-start.sh config/zookeeper.properties &

# 启动Kafka服务
bin/kafka-server-start.sh config/server.properties

# topic列表
bin/kafka-topics.sh --list --zookeeper localhost:2181

# 创建一个topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic test

# 控制台生产者
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

# 控制台消费者,每个控制台都是一个消费组
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning

# 进入node-zk目录
cd ~/Github/_oss/node-zk-browser
node app.js

# 模拟同一个消费组,多个消费者
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning --consumer.config config/consumer.properties
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning --consumer.config config/consumer.properties


