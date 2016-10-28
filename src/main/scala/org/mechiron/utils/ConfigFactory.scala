package org.mechiron.utils

import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * A singleton object to handle all configuration properties in the resources/config.properties
  *
  * @author Eran Levy
  */
object ConfigFactory {
  val props = new Properties()
  var configProps = mutable.Map[String,String]()

  /**
    * Return a configuration value for the given configuration property key
    * @param configKey a configuration property in config.properties
    * @return  a configuration value for the given configuration property key
    */
  def getProperty(configKey: String):Option[String] = {
    if(props.size==0) {
      val inputStream = ConfigFactory.getClass.getClassLoader.getResourceAsStream("config.properties")
      if(inputStream == null) {
        println("couldnt load config.properties resource")

      }
      props.load(inputStream)
      configProps= propertiesAsScalaMap(props)
    }
    configProps.get(configKey)
  }
}
