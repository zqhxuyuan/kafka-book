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

import java.lang.Long

import com.twitter.algebird.TopCMS
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.serialization.{Serdes, Serializer}
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics
import org.apache.kafka.streams.state.KeyValueStoreTestDriver
import org.apache.kafka.streams.state.internals.ThreadCache
import org.apache.kafka.test.{MockProcessorContext, NoOpRecordCollector, TestUtils}
import org.assertj.core.api.Assertions.assertThat
import org.junit._
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mock.MockitoSugar

class CMSStoreTest extends AssertionsForJUnit with MockitoSugar {

  private val anyStoreName = "cms-store"

  private def createTestContext[T](driver: KeyValueStoreTestDriver[Integer, TopCMS[T]] = createTestDriver[T](),
                                   changelogRecords: Option[Seq[(Int, TopCMS[T])]] = None
                                  ): MockProcessorContext = {
    // Write the records to the store's changelog
    changelogRecords.getOrElse(Seq.empty).foreach { case (key, cms) => driver.addEntryToRestoreLog(key, cms) }

    // The processor context is what makes the restore data available to a store during
    // the store's initialization, hence this is what we must return back.
    val processorContext: MockProcessorContext = {
      val pc = driver.context.asInstanceOf[MockProcessorContext]
      pc.setTime(1)
      pc
    }
    processorContext
  }

  private def createTestDriver[T](): KeyValueStoreTestDriver[Integer, TopCMS[T]] = {
    val keySerde = Serdes.Integer()
    val cmsSerde = TopCMSSerde[T]
    KeyValueStoreTestDriver.create[Integer, TopCMS[T]](
      keySerde.serializer(),
      keySerde.deserializer(),
      cmsSerde.serializer(),
      cmsSerde.deserializer())
  }

  @Test
  def shouldBeChangeloggingByDefault(): Unit = {
    val store = new CMSStore[String](anyStoreName)
    assertThat(store.loggingEnabled).isTrue
  }

  @Test
  def shouldBeNonPersistentStore(): Unit = {
    val store = new CMSStore[String](anyStoreName)
    assertThat(store.persistent).isFalse
  }

  @Test
  def shouldBeClosedBeforeInit(): Unit = {
    val store = new CMSStore[String](anyStoreName)
    assertThat(store.isOpen).isFalse
  }

  @Test
  def shouldBeOpenAfterInit(): Unit = {
    // Given
    val store: CMSStore[String] = new CMSStore[String](anyStoreName)
    val processorContext = createTestContext[String]()

    // When
    store.init(processorContext, store)

    // Then
    assertThat(store.isOpen).isTrue
  }

  @Test
  def shouldBeClosedAfterClosing(): Unit = {
    // Given
    val store: CMSStore[String] = new CMSStore[String](anyStoreName)

    // When
    store.close()

    // Then
    assertThat(store.isOpen).isFalse
  }


  @Test
  def shouldExactlyCountSmallNumbersOfItems(): Unit = {
    // Given
    val store: CMSStore[String] = new CMSStore[String](anyStoreName)
    val items = Seq(
      ("foo", System.currentTimeMillis()),
      ("bar", System.currentTimeMillis()),
      ("foo", System.currentTimeMillis()),
      ("foo", System.currentTimeMillis()),
      ("quux", System.currentTimeMillis()),
      ("bar", System.currentTimeMillis()),
      ("foor", System.currentTimeMillis()))
    val processorContext = createTestContext()
    store.init(processorContext, store)

    // When
    items.foreach(x => store.put(x._1, x._2))

    // Note: We intentionally do not flush the store in this test.

    // Then
    assertThat(store.totalCount).isEqualTo(items.size)
    assertThat(store.heavyHitters).isEqualTo(items.map(x => x._1).toSet)
    val expWordCounts: Map[String, Int] = items.map(x => x._1).groupBy(identity).mapValues(_.length)
    expWordCounts.foreach { case (word, count) => assertThat(store.get(word)).isEqualTo(count) }
  }

  @Test
  def shouldBackupToChangelogIfLoggingIsEnabled(): Unit = {
    // Given
    val driver: KeyValueStoreTestDriver[Integer, TopCMS[String]] = createTestDriver[String]()
    val processorContext = createTestContext(driver)
    val store: CMSStore[String] = new CMSStore[String](anyStoreName, loggingEnabled = true)
    store.init(processorContext, store)

    // When
    val items = Seq(
      ("one", System.currentTimeMillis()),
      ("two", System.currentTimeMillis()),
      ("three", System.currentTimeMillis()))
    items.foreach(x => store.put(x._1, x._2))
    store.flush()

    // Then
    val cms: TopCMS[String] = store.cmsFrom(items.map(x => x._1))
    assertThat(driver.flushedEntryStored(store.changelogKey)).isEqualTo(cms)
    assertThat(driver.flushedEntryRemoved(store.changelogKey)).isFalse
  }

  @Test
  def shouldBackupToChangelogOnlyOnFlush(): Unit = {
    // Given
    val store: CMSStore[String] = new CMSStore[String](anyStoreName, loggingEnabled = true)
    val observedChangelogRecords = new java.util.ArrayList[ProducerRecord[_, _]]
    val processorContext = {
      // We must use a "spying" RecordCollector because, unfortunately, Kafka's
      // KeyValueStoreTestDriver is not providing any such facilities.
      val observingCollector = new NoOpRecordCollector() {
        override def send[K, V](topic: String,
                                key: K,
                                value: V,
                                partition: Integer,
                                timestamp: Long,
                                keySerializer: Serializer[K],
                                valueSerializer: Serializer[V]) {
          observedChangelogRecords.add(new ProducerRecord[K, V](topic, partition, timestamp, key, value))
        }
      }
      val cache: ThreadCache = new ThreadCache(new LogContext("test"), 1024, new MockStreamsMetrics(new Metrics))
      val context = new MockProcessorContext(TestUtils.tempDirectory, Serdes.Integer(), TopCMSSerde[String], observingCollector, cache)
      context.setTime(1)
      context
    }
    store.init(processorContext, store)
    val items = Seq(
      ("one", System.currentTimeMillis()),
      ("two", System.currentTimeMillis()),
      ("three", System.currentTimeMillis()),
      ("four", System.currentTimeMillis()),
      ("firve", System.currentTimeMillis()))

    // When
    items.foreach(x => store.put(x._1, x._2))
    // Then
    assertThat(observedChangelogRecords.size()).isEqualTo(0)

    // When
    store.flush()
    // Then
    assertThat(observedChangelogRecords.size()).isEqualTo(1)
  }

  @Test
  def shouldNotBackupToChangelogIfLoggingIsDisabled(): Unit = {
    // Given
    val driver: KeyValueStoreTestDriver[Integer, TopCMS[String]] = createTestDriver[String]()
    val processorContext = createTestContext(driver)
    val store: CMSStore[String] = new CMSStore[String](anyStoreName, loggingEnabled = false)
    store.init(processorContext, store)

    // When
    val items = Seq(
      ("one", System.currentTimeMillis()),
      ("two", System.currentTimeMillis()),
      ("three", System.currentTimeMillis()))
    items.foreach(x => store.put(x._1, x._2))
    store.flush()

    // Then
    assertThat(driver.flushedEntryStored(store.changelogKey)).isNull()
    assertThat(driver.flushedEntryRemoved(store.changelogKey)).isFalse
  }

  @Test
  def shouldRestoreFromEmptyChangelog(): Unit = {
    // Given
    val driver: KeyValueStoreTestDriver[Integer, TopCMS[String]] = createTestDriver[String]()
    val store: CMSStore[String] = new CMSStore[String](anyStoreName, loggingEnabled = true)
    val processorContext = createTestContext[String](driver)

    // When
    store.init(processorContext, store)
    processorContext.restore(store.name, driver.restoredEntries())

    // Then
    assertThat(store.totalCount).isZero
    assertThat(store.heavyHitters.size).isZero
  }

  @Test
  def shouldRestoreFromNonEmptyChangelog(): Unit = {
    // Given
    val driver: KeyValueStoreTestDriver[Integer, TopCMS[String]] = createTestDriver[String]()
    val store: CMSStore[String] = new CMSStore[String](anyStoreName, loggingEnabled = true)
    val items: Seq[String] = Seq("foo", "bar", "foo", "foo", "quux", "bar", "foo")
    val processorContext: MockProcessorContext = {
      val changelogKeyDoesNotMatter = 123
      val cms: TopCMS[String] = store.cmsFrom(items)
      val changelogRecords = Seq((changelogKeyDoesNotMatter, cms))
      createTestContext(driver, changelogRecords = Some(changelogRecords))
    }

    // When
    store.init(processorContext, store)
    processorContext.restore(store.name, driver.restoredEntries())

    // Then
    val expWordCounts: Map[String, Int] = items.groupBy(identity).mapValues(_.length)
    expWordCounts.foreach { case (word, count) => assertThat(store.get(word)).isEqualTo(count) }
  }

  @Test
  def shouldRestoreFromChangelogTombstone(): Unit = {
    // Given
    val driver: KeyValueStoreTestDriver[Integer, TopCMS[String]] = createTestDriver[String]()
    val store: CMSStore[String] = new CMSStore[String](anyStoreName, loggingEnabled = true)
    val processorContext: MockProcessorContext = {
      val changelogKeyDoesNotMatter = 123
      val tombstone: TopCMS[String] = null
      val changelogRecords = Seq((changelogKeyDoesNotMatter, tombstone))
      createTestContext(driver, changelogRecords = Some(changelogRecords))
    }

    // When
    store.init(processorContext, store)
    processorContext.restore(store.name, driver.restoredEntries())

    // Then
    assertThat(store.totalCount).isZero
    assertThat(store.heavyHitters.size).isZero
  }

  @Test
  def shouldRestoreFromLatestChangelogRecordOnly(): Unit = {
    // Given
    val driver: KeyValueStoreTestDriver[Integer, TopCMS[String]] = createTestDriver[String]()
    val store: CMSStore[String] = new CMSStore[String](anyStoreName, loggingEnabled = true)
    val expectedItems = Seq("foo", "bar", "foo", "foo", "quux", "bar", "foo")
    val unexpectedItems1 = Seq("something", "entirely", "different")
    val unexpectedItems2 = Seq("even", "more", "different")
    val processorContext: MockProcessorContext = {
      val cms = store.cmsFrom(expectedItems)
      val differentCms1 = store.cmsFrom(unexpectedItems1)
      val differentCms2 = store.cmsFrom(unexpectedItems2)
      val sameKey = 123
      val differentKey = 456
      val changelogRecords = Seq(
        (sameKey, differentCms1),
        (differentKey, differentCms2),
        (sameKey, cms) /* latest entry in the changelog should "win" */
      )
      createTestContext(driver, changelogRecords = Some(changelogRecords))
    }

    // When
    store.init(processorContext, store)
    processorContext.restore(store.name, driver.restoredEntries())

    // Then
    val expWordCounts: Map[String, Int] = expectedItems.groupBy(identity).mapValues(_.length)
    expWordCounts.foreach { case (word, count) => assertThat(store.get(word)).isEqualTo(count) }
    // Note: The asserts below work only because, given the test setup, we are sure not to run into
    // CMS hash collisions that would lead to non-zero counts even for un-counted items.
    unexpectedItems1.toSet[String].foreach(item => assertThat(store.get(item)).isEqualTo(0))
    unexpectedItems2.toSet[String].foreach(item => assertThat(store.get(item)).isEqualTo(0))
  }

  @Test
  def shouldNotRestoreFromChangelogIfLoggingIsDisabled(): Unit = {
    // Given
    val store: CMSStore[String] = new CMSStore[String](anyStoreName, loggingEnabled = false)
    val items: Seq[String] = Seq("foo", "bar", "foo", "foo", "quux", "bar", "foo")
    val processorContext: MockProcessorContext = {
      val changelogKeyDoesNotMatter = 123
      val cms: TopCMS[String] = store.cmsFrom(items)
      val changelogRecords = Seq((changelogKeyDoesNotMatter, cms))
      createTestContext(changelogRecords = Some(changelogRecords))
    }

    // When
    store.init(processorContext, store)

    // Then
    assertThat(store.totalCount).isZero
    assertThat(store.heavyHitters.size).isZero
  }

}