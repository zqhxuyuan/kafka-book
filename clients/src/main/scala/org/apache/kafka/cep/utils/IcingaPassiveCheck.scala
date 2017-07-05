package org.apache.kafka.cep.utils

import com.googlecode.jsendnsca.builders.{MessagePayloadBuilder, NagiosSettingsBuilder}
import com.googlecode.jsendnsca.encryption.Encryption
import com.googlecode.jsendnsca.{Level, NagiosPassiveCheckSender}

class IcingaPassiveCheck(config: Config) {

  val enabled: Boolean = config.getProperty("icinga.enabled", "true").toBoolean
  val host: String = config.getProperty("icinga.host")
  val port: Int = config.getProperty("icinga.port", "5667").toInt
  val alertHost: String = config.getProperty("icinga.alert.host")

  private val password: String = config.getProperty("icinga.password")
  private val settings = new NagiosSettingsBuilder()
    .withNagiosHost(host)
    .withPort(port)
    .withEncryption(Encryption.XOR)
    .withPassword(password)
    .create

  private val sender = new NagiosPassiveCheckSender(settings)

  def updateStatus(service: String, level: Level, info: String): Boolean = {
    try {
      if (enabled) {
        val payload = new MessagePayloadBuilder()
          .withHostname(alertHost)
          .withServiceName(service)
          .withLevel(level)
          .withMessage(info)
          .create
        sender.send(payload)
        true
      } else {
        false
      }
    } catch {
      case e: Throwable => false
    }
  }

}