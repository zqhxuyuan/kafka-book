package io.confluent.examples.streams.microservices;

import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.CUSTOMERS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.ORDERS;
import static io.confluent.examples.streams.microservices.domain.Schemas.Topics.PAYMENTS;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.MIN;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.addShutdownHookAndBlock;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.baseStreamsConfig;
import static io.confluent.examples.streams.microservices.util.MicroserviceUtils.parseArgsAndConfigure;

import io.confluent.examples.streams.avro.microservices.Customer;
import io.confluent.examples.streams.avro.microservices.Order;
import io.confluent.examples.streams.avro.microservices.Payment;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A very simple service which sends emails. Order and Payment streams are joined
 * using a window. The result is then joined to a lookup table of Customers.
 * Finally an email is sent for each resulting tuple.
 */
public class EmailService implements Service {

  private static final Logger log = LoggerFactory.getLogger(EmailService.class);
  private static final String APP_ID = "email-service";
  private KafkaStreams streams;
  private Emailer emailer;

  public EmailService(Emailer emailer) {
    this.emailer = emailer;
  }

  @Override
  public void start(String bootstrapServers) {
    streams = processStreams(bootstrapServers, "/tmp/kafka-streams");
    streams.cleanUp(); //don't do this in prod as it clears your state stores
    streams.start();
    log.info("Started Service " + APP_ID);
  }

  private KafkaStreams processStreams(final String bootstrapServers, final String stateDir) {

    KStreamBuilder builder = new KStreamBuilder();

    //Create the streams/tables for the join
    KStream<String, Order> orders = builder.stream(ORDERS.keySerde(), ORDERS.valueSerde(), ORDERS.name());
    KStream<String, Payment> payments = builder.stream(PAYMENTS.keySerde(), PAYMENTS.valueSerde(), PAYMENTS.name());
    GlobalKTable<Long, Customer> customers = builder.globalTable(CUSTOMERS.keySerde(), CUSTOMERS.valueSerde(), CUSTOMERS.name());

    //Rekey payments to be by OrderId for the windowed join
    payments = payments.selectKey((s, payment) -> payment.getOrderId());

    //Join the two streams and the table then send an email for each
    orders.join(payments, EmailTuple::new,
        //Join Orders and Payments streams with a 30s window
        JoinWindows.of(1 * MIN), ORDERS.keySerde(), ORDERS.valueSerde(), PAYMENTS.valueSerde())
        //Next join to the GKTable of Customers
        .join(customers,
            (key, tuple) -> tuple.order.getCustomerId(),
            // note how, because we use a GKtable, we can join on any attribute of the Customer.
            (tuple, customer) -> tuple.setCustomer(customer))
        //Now for each tuple send an email.
        .peek((key, emailTuple)
            -> emailer.sendEmail(emailTuple)
        );

    return new KafkaStreams(builder, baseStreamsConfig(bootstrapServers, stateDir, APP_ID));
  }

  public static void main(String[] args) throws Exception {
    EmailService service = new EmailService(new LoggingEmailer());
    service.start(parseArgsAndConfigure(args));
    addShutdownHookAndBlock(service);
  }

  private static class LoggingEmailer implements Emailer {

    @Override
    public void sendEmail(EmailTuple details) {
      //In a real implementation we would do something a little more useful
      log.warn("Sending an email to: \nCustomer:%s\nOrder:%s\nPayment%s", details.customer,
          details.order, details.payment);
    }
  }

  @Override
  public void stop() {
    if (streams != null) {
      streams.close();
    }
  }

  interface Emailer {

    void sendEmail(EmailTuple details);
  }

  public class EmailTuple {
    public Order order;
    public Payment payment;
    public Customer customer;

    public EmailTuple(Order order, Payment payment) {
      this.order = order;
      this.payment = payment;
    }

    EmailTuple setCustomer(Customer customer) {
      this.customer = customer;
      return this;
    }
  }
}
