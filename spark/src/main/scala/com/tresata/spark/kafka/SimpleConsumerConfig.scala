package com.tresata.spark.kafka

import java.util.Properties
import kafka.utils.VerifiableProperties

object SimpleConsumerConfig {
  def apply(originalProps: Properties): SimpleConsumerConfig = new SimpleConsumerConfig(new VerifiableProperties(originalProps))

  val SocketTimeout = 30 * 1000
  val SocketBufferSize = 64 * 1024
  val FetchSize = 1024 * 1024
  val ClientId = ""
  val MetadataBrokerList = "localhost:9092"
  val RefreshLeaderBackoffMs = 200
  val RefreshLeaderMaxRetries = 4
}

class SimpleConsumerConfig private (val props: VerifiableProperties) {
  import SimpleConsumerConfig._

  /** the socket timeout for network requests. Its value should be at least fetch.wait.max.ms. */
  val socketTimeoutMs = props.getInt("socket.timeout.ms", SocketTimeout)

  /** the socket receive buffer for network requests */
  val socketReceiveBufferBytes = props.getInt("socket.receive.buffer.bytes", SocketBufferSize)

  /** the number of byes of messages to attempt to fetch */
  val fetchMessageMaxBytes = props.getInt("fetch.message.max.bytes", FetchSize)

  /** specified by the kafka consumer client, used to distinguish different clients */
  val clientId = props.getString("client.id", ClientId)

  /** this is for bootstrapping and the consumer will only use it for getting metadata */
  val metadataBrokerList = props.getString("metadata.broker.list", MetadataBrokerList)

  /** backoff time to wait before trying to determine the leader of a partition that has just lost its leader */
  val refreshLeaderBackoffMs = props.getInt("refresh.leader.backoff.ms", RefreshLeaderBackoffMs)

  /** maximum attempts to determine leader for all partition before giving up */
  val refreshLeaderMaxRetries = props.getInt("refresh.leader.max.retries", RefreshLeaderMaxRetries)

  props.verify()
}