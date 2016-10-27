package org.mechiron.utils

import java.util.Properties

import scala.collection.JavaConversions._
import scala.collection.mutable
/**
  * Created by eran on 26/10/16.
  */
object ConfigFactory {
  val props = new Properties()
  var configProps = mutable.Map[String,String]()
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
