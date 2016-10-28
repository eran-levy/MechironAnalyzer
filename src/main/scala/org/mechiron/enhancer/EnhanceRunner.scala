package org.mechiron.enhancer

import org.mechiron.utils.SparkConnector

/**
  * Items Enhancer unit - each retail name its item names/manufacture names/manufacture country names/etc different.
  * Its main role is to unify the names (currently) simply based on the most frequent word
  * Here we just run the same function for different columns in the "items" MySQL table
  *
  * @author Eran Levy
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
