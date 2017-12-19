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

import com.twitter.algebird.{TopCMS, TopPctCMS, TopPctCMSMonoid}
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serde
import org.assertj.core.api.Assertions.assertThat
import org.junit._
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mock.MockitoSugar

class TopCMSSerdeTest extends AssertionsForJUnit with MockitoSugar {

  private val anyTopic = "any-topic"

  @Test
  def shouldRoundtrip(): Unit = {
    // Given
    val cmsMonoid: TopPctCMSMonoid[String] = {
      val delta: Double = 1E-10
      val eps: Double = 0.001
      val seed: Int = 1
      val heavyHittersPct: Double = 0.01
      TopPctCMS.monoid[String](eps, delta, seed, heavyHittersPct)
    }
    val itemToBeCounted = "count-me"
    val cms: TopCMS[String] = cmsMonoid.create(itemToBeCounted)
    val serde: Serde[TopCMS[String]] = TopCMSSerde[String]

    // When
    val bytes: Array[Byte] = serde.serializer().serialize(anyTopic, cms)
    val restoredCms = serde.deserializer().deserialize(anyTopic, bytes)

    // Then
    assertThat(restoredCms).isEqualTo(cms)
    assertThat(restoredCms.frequency(itemToBeCounted).estimate).isEqualTo(cms.frequency(itemToBeCounted).estimate)
  }

  @Test
  def shouldSerializeNullToNull() {
    val serde: Serde[TopCMS[String]] = TopCMSSerde[String]
    assertThat(serde.serializer.serialize(anyTopic, null)).isNull()
    serde.close()
  }

  @Test
  def shouldDeserializeNullToNull() {
    val serde: Serde[TopCMS[String]] = TopCMSSerde[String]
    assertThat(serde.deserializer.deserialize(anyTopic, null)).isNull()
    serde.close()
  }

  @Test
  def shouldThrowSerializationExceptionWhenDeserializingZeroBytes() {
    val serde: Serde[TopCMS[String]] = TopCMSSerde[String]
    try {
      serde.deserializer.deserialize(anyTopic, new Array[Byte](0))
      fail("Should have thrown a SerializationException because of zero input bytes");
    }
    catch {
      case _: SerializationException => // Ignore (there's no contract on the details of the exception)
    }
    serde.close()
  }

}