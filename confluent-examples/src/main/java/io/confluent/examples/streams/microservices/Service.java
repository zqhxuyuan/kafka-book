package io.confluent.examples.streams.microservices;

public interface Service {

  void start(String bootstrapServers);

  void stop();
}