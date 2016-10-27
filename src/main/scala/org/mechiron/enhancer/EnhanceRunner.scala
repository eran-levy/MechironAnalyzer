package org.mechiron.enhancer

import org.mechiron.utils.SparkConnector

/**
  * Created by eran on 15/10/16.
  */
object EnhanceRunner {
  def main(args: Array[String]) {
    val sc = new SparkConnector().initSparkContext()
    new ItemsEnhancer(sc).sparkFuncEnhance("item_name")
    new ItemsEnhancer(sc).sparkFuncEnhance("manufacture_name")
    new ItemsEnhancer(sc).sparkFuncEnhance("manufacture_country")
  }
}
