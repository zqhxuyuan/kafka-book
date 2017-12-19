package io.confluent.examples.streams.microservices.util;

public class Paths {

  private String base;

  public Paths(String host, int port) {
    base = "http://" + host + ":" + port;
  }

  public String urlGet(int id) {
    return base + "/v1/orders/" + id;
  }

  public String urlGet(String id) {
    return base + "/v1/orders/" + id;
  }

  public String urlGetValidated(int id) {
    return base + "/v1/orders/" + id + "/validated";
  }

  public String urlGetValidated(String id) {
    return base + "/v1/orders/" + id + "/validated";
  }

  public String urlPost() {
    return base + "/v1/orders/";
  }
}