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

import com.twitter.algebird.CMSHasher
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.state.StoreBuilder

/**
  * A factory for Kafka Streams to instantiate a [[CMSStore]].
  *
  * =Usage=
  *
  * The [[CMSStore]]'s changelog will typically have rather few and small records per partition.
  * To improve efficiency we thus set a smaller log segment size (`segment.bytes`) than Kafka's
  * default of 1GB.
  *
  * {{{
  * val changeloggingEnabled = true
  * val changelogConfig = {
  *   val cfg = new java.util.HashMap[String, String]
  *   val segmentSizeBytes = (20 * 1024 * 1024).toString
  *   cfg.put("segment.bytes", segmentSizeBytes)
  *   cfg
  * }
  * new CMSStoreSupplier[String](cmsStoreName, Serdes.String(), changeloggingEnabled, changelogConfig)
  * }}}
  */
class CMSStoreBuilder[T: CMSHasher](val name: String,
                                    val serde: Serde[T])
    extends StoreBuilder[CMSStore[T]] {

  var loggingEnabled = false
  var logConfig : util.Map[String, String] = new util.HashMap[String, String]()


  override def build(): CMSStore[T] = new CMSStore[T](name, loggingEnabled)

  override def withCachingEnabled() = throw new UnsupportedOperationException("caching not supported")

  override def withLoggingEnabled(config: util.Map[String, String]): CMSStoreBuilder[T] = {
    loggingEnabled = true
    logConfig.clear()
    logConfig.putAll(config)
    this
  }

  override def withLoggingDisabled(): CMSStoreBuilder[T] = {
    loggingEnabled = false
    logConfig.clear()
    this
  }
}
