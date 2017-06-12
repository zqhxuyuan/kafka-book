

## HOW TO RUN

start zk and kafka

```
$ cd kafka_2.10-0.10.0.0
$ bin/zookeeper-server-start.sh config/zookeeper.properties
$ bin/kafka-server-start.sh config/server.properties

$ bin/kafka-topics.sh --create --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 3 --topic topic
$ bin/kafka-topics.sh --list --zookeeper localhost:2181
```

start program

```
➜  git clone https://github.com/zqhxuyuan/kafka-book.git
➜  cd kafka-book
➜  mvn clean compile exec:java -Dexec.mainClass="KafkaProducerConsumerDemo"
```

## NIO

http://blog.csdn.net/jjzhk/article/details/39553613
http://www.iclojure.com/blog/articles/2015/12/05/Scalable-IO-in-Java

## Consumer消息发送语义

https://dzone.com/articles/kafka-clients-at-most-once-at-least-once-exactly-o