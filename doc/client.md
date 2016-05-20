## Disruptor

disruptor-kafka-consumer: <http://github.com/>

Demonstration of Using LMAX Disruptor with Kafka 0.9 Consumer

Benefits -> Use the Sequence Barriers to commit messages once they are completely processed by the previous consumer
Imagination is the limit
If Ring Buffers can fit in L3 Cache, (depending on the payload of the message) processing becomes even faster

Want to have more consumers? Create more KafkaDisruptor Objects and run them. The KafkaDisruptor object makes sure only 1 Kafka Consumer instance
runs with 1 Disruptor Instance.   1 Producer per ring buffer giving performance benefits.

