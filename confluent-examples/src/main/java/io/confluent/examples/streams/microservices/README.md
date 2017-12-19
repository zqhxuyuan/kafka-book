# Kafka Streams Microservice Examples

Here is a small microservice ecosystem built with Kafka Streams. There is a related [blog post](https://www.confluent.io/blog/building-a-microservices-ecosystem-with-kafka-streams-and-ksql/) that outlines the approach used.  

The example centers around an Orders Service which provides a REST interface to POST and GET Orders. Posting an Order creates an event in Kafka. This is picked up by three different validation engines (Fraud Service, Inventory Service, Order Details Service) which validate the order in parallel, emitting a PASS or FAIL based on whether each validation succeeds. The result of each validation is pushed through a separate topic, Order Validations, so that we retain the ‘single writer’ status of the Orders Service —> Orders Topic. The results of the various validation checks are aggregated back in the Order Service (Validation Aggregator) which then moves the order to a Validated or Failed state, based on the combined result. 

To allow users to GET any order, the Orders Service creates a queryable materialized view (embedded inside the Orders Service), using a state store in each instance of the service, so any Order can be requested historically. Note also that the Orders Service can be scaled out over a number of nodes, so GET requests must be routed to the correct node to get a certain key. This is handled automatically using the Interactive Queries functionality in Kafka Streams. 

The Orders Service also includes a blocking HTTP GET so that clients can read their own writes. In this way we bridge the synchronous, blocking paradigm of a Restful interface with the asynchronous, non-blocking processing performed server-side.

Finally there is a very simple email service, which is probably the best place to get started.

NB - this is demo code, not a production system and certain elements are left for further work, but there is sufficient code here to exemplify the approach in a running system. 

![alt text](https://www.confluent.io/wp-content/uploads/Screenshot-2017-11-09-12.34.26.png "System Diagram")

# Getting Started:
To play with this ecosystem the simplest way is to run the tests and fiddle with the code. Each test boots a self-contained Kafka cluster so it's easy to play with different queries and configurations. 
The best place to start is [EndToEndTest.java](https://github.com/confluentinc/kafka-streams-examples/blob/3.3.1-post/src/test/java/io/confluent/examples/streams/microservices/EndToEndTest.java)


# Running the Examples:
* Requires Java 1.8
* mvn install -Dmaven.test.skip=true

# Outstanding Work
- Currently bare bones testing only. Should add tests using KStreamTestDriver to demonstrate how to build tests quickly. 
- Test framework needs to implement multiple Kafka instances to ensure accuracy in partitioned mode. 
- The implementation of the Order Details Service using a producer and consumer probably isn't that useful. Lets port this to KStreams
- Demo embedded KSQL around the input of Inventory (which can be done without Avro support)
