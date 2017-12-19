/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams.algebird

import java.util

import com.twitter.algebird.TopCMS
import com.twitter.chill.ScalaKryoInstantiator
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization._

class TopCMSSerializer[T] extends Serializer[TopCMS[T]] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    // nothing to do
  }

  override def serialize(topic: String, cms: TopCMS[T]): Array[Byte] =
    if (cms == null) null
    else ScalaKryoInstantiator.defaultPool.toBytesWithClass(cms)

  override def close(): Unit = {
    // nothing to do
  }

}

class TopCMSDeserializer[T] extends Deserializer[TopCMS[T]] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    // nothing to do
  }

  override def deserialize(topic: String, bytes: Array[Byte]): TopCMS[T] =
    if (bytes == null) null
    else if (bytes.isEmpty) throw new SerializationException("byte array must not be empty")
    else ScalaKryoInstantiator.defaultPool.fromBytes(bytes).asInstanceOf[TopCMS[T]]

  override def close(): Unit = {
    // nothing to do
  }

}

/**
  * A [[Serde]] for [[TopCMS]].
  *
  * =Usage=
  *
  * {{{
  * val anyTopic = "any-topic"
  * val cms: TopCMS[String] = ???
  * val serde: Serde[TopCMS[String]] = TopCMSSerde[String]
  *
  * val bytes: Array[Byte] = serde.serializer().serialize(anyTopic, cms)
  * val restoredCms: TopCMS[String] = serde.deserializer().deserialize(anyTopic, bytes)
  * }}}
  *
  * =Future Work=
  *
  * We could perhaps be more efficient if we serialized not the full [[TopCMS]] instance but only
  * its relevant fields.
  */
object TopCMSSerde {

  def apply[T]: Serde[TopCMS[T]] = Serdes.serdeFrom(new TopCMSSerializer[T], new TopCMSDeserializer[T])

}