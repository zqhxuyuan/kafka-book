package clients.actor

import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import kafka.serializer._

/**
  * Created by ctao on 16-1-26.
  * Long型的序列化，是kafka序列化的子类
  */
class LongSerializer extends Serializer[Long] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def serialize(topic: String, data: Long): Array[Byte] = BigInt(data).toByteArray

  override def close(): Unit = ()
}

class LongDeserializer extends Deserializer[Long]{
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()

  override def deserialize(topic: String, data: Array[Byte]): Long = BigInt(data).toLong
}

class LongDecoder extends Decoder[Long] {
  override def fromBytes(bytes: Array[Byte]): Long = BigInt(bytes).toLong
}

class LongEncoder extends Encoder[Long] {
  override def toBytes(t: Long): Array[Byte] = BigInt(t).toByteArray
}