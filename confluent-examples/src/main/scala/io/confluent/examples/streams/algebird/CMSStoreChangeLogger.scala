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

import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.internals.{ProcessorStateManager, RecordCollector}
import org.apache.kafka.streams.state.StateSerdes

/**
  * Copied from Kafka's [[org.apache.kafka.streams.state.internals.StoreChangeLogger]].
  *
  * If StoreChangeLogger had been public, we would have used it as-is.
  *
  * Note that the use of array-typed keys is discouraged because they result in incorrect caching
  * behavior.  If you intend to work on byte arrays as key, for example, you may want to wrap them
  * with the [[org.apache.kafka.common.utils.Bytes]] class.
  */
class CMSStoreChangeLogger[K, V](val storeName: String,
                                 val context: ProcessorContext,
                                 val partition: Int,
                                 val serialization: StateSerdes[K, V]) {

  private val topic = ProcessorStateManager.storeChangelogTopic(context.applicationId, storeName)
  private val collector = context.asInstanceOf[RecordCollector.Supplier].recordCollector

  def this(storeName: String, context: ProcessorContext, serialization: StateSerdes[K, V]) {
    this(storeName, context, context.taskId.partition, serialization)
  }

  def logChange(key: K, value: V, timestamp: Long) {
    if (collector != null) {
      val keySerializer = serialization.keySerializer
      val valueSerializer = serialization.valueSerializer
      collector.send(this.topic, key, value, this.partition, timestamp, keySerializer, valueSerializer)
    }
  }

}
