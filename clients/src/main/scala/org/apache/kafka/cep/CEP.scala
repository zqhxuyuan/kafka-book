package org.apache.kafka.cep

import java.util.Properties

import grizzled.slf4j.Logger
import kafka.consumer.{Consumer, ConsumerConfig}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.cep.utils.{Config, Observed}

import scala.collection.JavaConversions._

trait DistributedMode {}

abstract class CEP(val config: Config) extends Observed {
  implicit val system: CEP = this
  val inDistributedMode: Boolean = this.isInstanceOf[DistributedMode]
  var detectors: Set[Detector] = Set[Detector]()
  val shell = new CEPShell(CEP.this)
  val log = Logger(classOf[CEP])

  //
  def register[T <: Detector](detector: T) = detectors += detector

  //def register(kafkaStream: KafkaStream) = messageTypes += messageStream

  def resetZooKeeperOffsets: CEP = {
    val zk: ZkClient = new ZkClient(config.getProperty("kafka.zk.connect"), 10000, 10000, new ZkSerializer {
      override def deserialize(data: Array[Byte]): Object = if (data == null) null else new String(data)

      def serialize(data: Object): Array[Byte] = data.toString.getBytes
    })
    if (zk.exists("/consumers/" + config.getProperty("consumer.id"))) {
      log.info("Resetting consumed offsets.." + "/consumers/" + config.getProperty("consumer.id"))
      zk.deleteRecursive("/consumers/" + config.getProperty("consumer.id"))
    }
    zk.close
    this
  }

  def startWithShell {
    start
    shell.init
  }

  def start {
    MessageImpulseGenerator.start
  }

  def stop: Unit = {
    MessageImpulseGenerator.stop
  }

  def processImpulse(impulse: Any) {
    detectors.foreach(d ⇒ {
      d.handle(this, impulse)
    })
  }

  def create[T <: Detector](clz: Class[T], args: AnyRef*): T = {
    val constructors = clz.getConstructors()
      .filter(c ⇒ {
      c.getParameterTypes().size.equals(args.size)
      //TODO match Seq[_] with consecutive args and then foldLeft to Boolean
    })
    val c = constructors(0)
    println(c.getParameterTypes()(0))
    println(args(0).getClass())
    c.newInstance(args: _*).asInstanceOf[T]
  }

  object MessageImpulseGenerator {
    val kafka08Config = new Properties()

    config.filter(_._1.startsWith("kafka.")).foreach { case (key, value) => {
      kafka08Config.put(key.substring("kafka.".length), value)
    }}

    if (!kafka08Config.containsKey("group.id")) {
      throw new IllegalArgumentException("configuration must contain kafka.group.id")
    }
    if (!kafka08Config.containsKey("zookeeper.connect")) {
      throw new IllegalArgumentException("configuration must contain kafka.zookeeper.connect")
    }


    val consumer = Consumer.create(new ConsumerConfig(kafka08Config))

    def start = {
      //consumer.
    }
    def stop = consumer.shutdown

//    override def build: VDNADataStreamProcessor[VDNAMessage] = {
//      new VDNADataStreamProcessor[VDNAMessage] {
//        override def process(message: VDNAMessage) = {
//          if (message != null) {
//            CEP.this.processImpulse(message);
//          }
//        }
//
//        override def onError[P <: VDNAMessage](vdnaDataStream: VDNADataStream[P], throwable: Throwable): Unit = {
//          throwable.printStackTrace();
//        }
//      }
//    }
  }

}
