package org.mechiron.importer
import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.types._
import org.mechiron.utils.MySQLBridge

/**
  * Created by eran on 26/10/16.
  */
class StoresImporter extends Importer{
  val logger = LogManager.getLogger(getClass)
  val mysqlBridge = new MySQLBridge()
  def executeImport(sqlContext: SQLContext, filesLocationForProcess: String): Unit = {
    val customSchema = StructType(Array(StructField("chain_id", StringType, false),
      StructField("chain_name", StringType, true),
      StructField("store_id", StringType, false),
      StructField("store_name", StringType, true),
      StructField("store_address", StringType, true),
      StructField("store_city", StringType, true),
      StructField("store_zipcode", StringType, true)))
    val directoriesToIterate = new File(filesLocationForProcess).listFiles().filter(listedFile => listedFile.isDirectory).map(listedFile => new Path(listedFile.getAbsolutePath))
    if (directoriesToIterate.length > 0) {
      for (dir <- directoriesToIterate) {
        logger.debug("starting to iterate files in: " + dir)
        val outputFolder = new File(dir.toString + File.separatorChar + "output").listFiles().filter(listedFile => listedFile.isFile && listedFile.getName.indexOf("store") == 0).map(listedFile => new Path(listedFile.getAbsolutePath))
        if(outputFolder.length>0) {
          //found a store csv file to extract
          //we wont have more then 1 stores csv file so we can easily take index 0
          val storeFile = outputFolder(0)
          val storesDfMysql = mysqlBridge.connect(sqlContext,"store")
          val orderedStoresDf = storesDfMysql.select(storesDfMysql("chain_id"), storesDfMysql("store_id")).orderBy(storesDfMysql("chain_id"), storesDfMysql("store_id"))
          val csvDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode","DROPMALFORMED").schema(customSchema).option("delimiter", "|").load(storeFile.toString)
          logger.debug("starting to load new stores into the stores table from file: " + storeFile.toString)
          val sortedCsvDf = csvDf.filter(csvDf("chain_id").isNotNull && csvDf("store_id").isNotNull)
          val csvStoreChainId = sortedCsvDf.select(sortedCsvDf("chain_id"), sortedCsvDf("chain_name"), sortedCsvDf("store_id"),  sortedCsvDf("store_name"),  sortedCsvDf("store_address"),
            sortedCsvDf("store_city"),  sortedCsvDf("store_zipcode"))
            .orderBy(sortedCsvDf("chain_id"),sortedCsvDf("store_id"))
          val joinedStreams = orderedStoresDf.join(csvStoreChainId, orderedStoresDf("chain_id")===csvStoreChainId("chain_id") && orderedStoresDf("store_id")===csvStoreChainId("store_id"), "right_outer").filter(orderedStoresDf("store_id").isNull)
            .select(csvStoreChainId("chain_id"), csvStoreChainId("chain_name"), csvStoreChainId("store_id"),  csvStoreChainId("store_name"),  csvStoreChainId("store_address"), csvStoreChainId("store_city"),  csvStoreChainId("store_zipcode"))
          val rowsToAdd = joinedStreams.count()
          logger.debug("successfully joined csv and store db streams, count of new stores to be added: " + rowsToAdd)
          if(rowsToAdd>0) {
            mysqlBridge.writeToMysqlJdbc(joinedStreams,SaveMode.Append,"store")
          }
          logger.debug("finished loading new stores into the store table")

        }
      }
    }
  }
}
