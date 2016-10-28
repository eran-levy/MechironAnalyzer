package org.mechiron.importer

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.mechiron.utils.{ConfigFactory, SparkConnector}

/**
  * Main unit to perform ETL on the downloaded csv files from url.publishedprices.co.il
  * @see the Mechiron GitHub repository (https://github.com/eran-levy/Mechiron) for the Python project
  *      that downloads the prices CSV files
  *
  * Import new items and updated prices into the "items" and "prices" mysql tables
  *
  * @see ItemsAndPricesImporter
  * @see StoresImporter
  *
  * @author Eran Levy
  */
object MechironImporter {
  val folderName = ConfigFactory.getProperty("mechiron_downloaded_data_location")
  val logger = LogManager.getLogger(getClass)

  /**
    * Run the ETL processes on the downloaded CSV files
    * @param sc the SparkContext
    */
  def runImporters(sc: SparkContext): Unit = {
    val sqlContext = new SQLContext(sc)
    logger.debug("created sqlContext using the given sparkContext and running importer")
    new ItemsAndPricesImporter().executeImport(sqlContext, folderName.get)
    new StoresImporter().executeImport(sqlContext, folderName.get)
  }

  /**
    * Run the experimental unit
    * @param args doesnt accept additional arguments
    */
  def main(args: Array[String]) {
    logger.debug("starting mechiron importer...")
    val sc = new SparkConnector().initSparkContext()
    runImporters(sc)
  }

}
