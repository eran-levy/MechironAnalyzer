package org.mechiron.importer

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.mechiron.utils.{ConfigFactory, SparkConnector}

/**
  * Created by eran on 20/09/16.
  */
object MechironImporter {
  val folderName = ConfigFactory.getProperty("mechiron_downloaded_data_location")
  val logger = LogManager.getLogger(getClass)

  def runImporters(sc: SparkContext): Unit = {
    val sqlContext = new SQLContext(sc)
    logger.debug("created sqlContext using the given sparkContext and running importer")
    new ItemsAndPricesImporter().executeImport(sqlContext, folderName.get)
    new StoresImporter().executeImport(sqlContext, folderName.get)
  }



  def main(args: Array[String]) {
    logger.debug("starting mechiron importer...")
    val theimporter = MechironImporter
    val sc = new SparkConnector().initSparkContext()
    theimporter.runImporters(sc)
  }

}
