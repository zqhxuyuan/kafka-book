package kafka.streams.queryable;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import spark.Spark;

/**
 * @author ysaeki
 */
public class QueryableWordCountService {

  private KafkaStreams streams;
  private String storeName;
  private String applicationHost;
  private int applicationPort;
  private String html =
          "<!DOCTYPE html>\n"
                  + "<html><head><meta charset=\"UTF-8\"><title>Counter</title></head><body>"
                  + "<div style=\"margin:10px;\">"
                  + "<table style=\"border-width:medium;border-style:solid;\">"
                  + "<thead><tr><th>Word</th><th>Count</th></tr></thead>"
                  + "<tbody><tr><th>%s</th><th>%d</th></tr></tbody></table>"
                  + "</div></body></html>";

  public QueryableWordCountService(final KafkaStreams s,
                                   final String sName,
                                   final String appHost,
                                   final int appPort) {
    streams = s;
    storeName = sName;
    applicationHost = appHost;
    applicationPort = appPort;
  }

  public void start() {
    Spark.ipAddress(applicationHost);
    Spark.port(applicationPort);
    Spark.get("/results/:key", (req, resp) -> {
      String key = req.params(":key");
      ReadOnlyKeyValueStore<String, Long> store =
              streams.store(storeName,
                      QueryableStoreTypes.<String, Long>keyValueStore());
      Long value = store.get(key);
      resp.type("text/html");
      return String.format(html, key, value);
    });
  }
}
