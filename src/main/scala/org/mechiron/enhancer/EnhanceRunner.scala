package org.mechiron.enhancer

import org.mechiron.utils.SparkConnector

/**
  * Created by eran on 15/10/16.
  */
object EnhanceRunner {
  def main(args: Array[String]) {
    val sc = new SparkConnector().initSparkContext()
    val enhancer = new ItemsEnhancer(sc)
    enhancer.sparkFuncEnhance("item_name")
    enhancer.sparkFuncEnhance("manufacture_name")
    enhancer.sparkFuncEnhance("manufacture_country")
  }
}
