package org.apache.kafka.cep.utils

import java.io.{InputStream, File, FileInputStream}
import java.util.Properties

import scala.collection.JavaConverters._

object Config {
  def fromResource(resourcePath: String): Config = fromInputStream(getClass getResourceAsStream resourcePath)

  def fromFile(configFilePath: String): Config = fromInputStream(new FileInputStream(new File(configFilePath)))

  def fromInputStream(is: InputStream): Config = {
    val config = new Config
    config.putAll(new Properties() {
      load(is)
    })
    config
  }
}

class Config extends Properties {


  val config = this.asScala

  def apply(propertyName: String) = config(propertyName)

  def apply(propertyName: String, defaultValue: String) = {
    if (config contains propertyName) {
      config(propertyName)
    } else {
      defaultValue
    }
  }

}