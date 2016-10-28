package org.mechiron.importer

import java.io.File

import org.apache.hadoop.fs.Path
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.types._
import org.mechiron.utils.{ConfigFactory, MySQLBridge}


/**
  * Run an ETL process to import new items and updated prices into the "items" and "prices" mysql tables
  *
  * Iterate the the mechiron_downloaded_data_location folders that are in the following structure:
  * YYYY-MM-dd_HH_mm_ss/output/retail_name_YYYY-MM-dd_HH_mm_ss.csv
  * Each "output" folder contains the aggregated CSV files downloaded per each retail
  *
  *  @see the Mechiron GitHub repository (https://github.com/eran-levy/Mechiron) for the Python project
  *      that downloads the prices CSV files
  *
  * @author Eran Levy
  */
class ItemsAndPricesImporter extends Importer{

  val mysqlBridge = new MySQLBridge()
  val logger = LogManager.getLogger(getClass)

  /**
    * Iterate the filesLocationForProcess folders and execute its ETL processes
    *
    * @param sqlContext the Spark SQLContext initiated with the existing SparkContext
    * @param filesLocationForProcess the folders to iterate mechiron_downloaded_data_location property
    */
  def executeImport(sqlContext: SQLContext, filesLocationForProcess: String): Unit = {
    //define csv columns used by the databricks sprak CSV parser
    val customSchema = StructType(Array(StructField("chain_id", StringType, nullable = false),
      StructField("sub_chain_id", StringType, nullable = true),
      StructField("store_id", StringType, nullable = false),
      StructField("item_id", StringType, nullable = true),
      StructField("item_price", DoubleType, nullable = true),
      StructField("qty", DoubleType, nullable = true),
      StructField("manufacture_name", StringType, nullable = true),
      StructField("manufacture_country", StringType, nullable = true),
      StructField("manufacture_item_desc", StringType, nullable = true),
      StructField("item_name", StringType, nullable = true),
      StructField("item_code", StringType, nullable = false),
      StructField("price_update_date", TimestampType, nullable = true)))

    //read dataframes from mysql
    val itemsDfMysql = mysqlBridge.connect(sqlContext,"items")
    val pricesDfMysql = mysqlBridge.connect(sqlContext,"prices")

    logger.debug("write prices table results to parquet")
    //EXPERIMENTAL - for performance purposes
    //write Dataframe to parquet - since its EXPERIMENATAL just overwrite
    val hdfsServer = ConfigFactory.getProperty("hdfs_server")
    val hdfsDefaultLocation = ConfigFactory.getProperty("hdfs_default_location")
    val parquetFileLocation =  hdfsServer.get + hdfsDefaultLocation.get
    pricesDfMysql.write.mode(SaveMode.Overwrite).parquet(parquetFileLocation+"/pricestest.parquet")
    val pricesParquetDf = sqlContext.read.parquet(parquetFileLocation+"/pricestest.parquet")

    logger.debug("arrange items and prices dataframes")
    val orderedItemsDf = itemsDfMysql.select(itemsDfMysql("item_code")).orderBy(itemsDfMysql("item_code"))
    val orderedPricesDf = pricesParquetDf.orderBy(pricesParquetDf("chain_id"), pricesParquetDf("store_id"), pricesParquetDf("item_code"), pricesParquetDf("price_update_date"))

    //for hdfs file access: hdfs://localhost:9000/user/mechiron/testitems.csv
    //local - "/home/eran/mechiron-downloads/2016-07-09_06_53_03/output/RamiLevi.csv"
    val directoriesToIterate = new File(filesLocationForProcess).listFiles().filter(listedFile => listedFile.isDirectory).map(listedFile => new Path(listedFile.getAbsolutePath))
    //iterate the mechiron_downloaded_data_location folders
    if (directoriesToIterate.length > 0) {
      for (dir <- directoriesToIterate) { //found output folder
        logger.debug("starting to iterate files in: " + dir)
        //for each date folder get its output folder where the files reside and exclude the stores CSV file which we have a dedicated ETL process
        val outputFolder = new File(dir.toString + File.separatorChar + "output").listFiles().filter(listedFile => listedFile.isFile && listedFile.getName.indexOf("store") == -1).map(listedFile => new Path(listedFile.getAbsolutePath))
        if(outputFolder.length>0) {
          for (outputFile <- outputFolder) {
            logger.debug("starting to process output file: " + outputFile)
            //for each output folder
            val csvDf = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("mode","DROPMALFORMED").schema(customSchema).option("delimiter", "|").load(outputFile.toString)
            //load new items
            loadNewItemCodes(sqlContext, orderedItemsDf, csvDf)
            //load new prices
            loadUpdatedOrNewPricesForItems(orderedPricesDf,csvDf)
            logger.debug("finished processing output file: " + outputFile)
          }
          logger.debug("finished handling folder: " + dir)
        }
      }
    }

  }

  /**
    * ETL process to add new items item_code into the "items" mysql table
    * Select the MySQL "items" table into a dedicated dataframe
    * Right-join the selected mysql table with the given CSV dataframe
    * Append the new items into the "items" table with the new item_code only - the rest with value: "None" -
    * the enhancer will be in charge of providing the most relevant names: items_name, manufacture_name, etc
    * @see ItemsEnhancer for more information
    *
    * @param sqlContext the Spark SQLContext initiated with the existing SparkContext
    * @param itemsDf the selected Mysql items dataframe
    * @param csvDf the parsed CSV dataframe
    */
  def loadNewItemCodes(sqlContext: SQLContext, itemsDf: DataFrame, csvDf: DataFrame): Unit = {
    logger.debug("starting to load new item codes into the items table")
    val sortedCsvDf = csvDf.filter(csvDf("item_code").isNotNull)
    val csvItemCode = sortedCsvDf.select(sortedCsvDf("item_code")).distinct.orderBy(sortedCsvDf("item_code"))
    val joinedStreams = itemsDf.join(csvItemCode, itemsDf("item_code")===csvItemCode("item_code"), "right_outer").filter(itemsDf("item_code").isNull).select(csvItemCode("item_code"))
    val rowsToAdd = joinedStreams.count()
    logger.debug("successfully joined csv and items db streams, count of new item codes to be added: " + rowsToAdd)
    if(rowsToAdd>0) {
      joinedStreams.registerTempTable("joinedtabletest")
      val dfToWrite = sqlContext.sql("select item_code, 'None' as item_name, 'None' as manufacture_name, 'None' as manufacture_country, 'None' as manufacture_item_desc from joinedtabletest")

      mysqlBridge.writeToMysqlJdbc(dfToWrite, SaveMode.Append,"items")
    }
    logger.debug("finished loading new item codes into the items table")
  }

  /**
    * ETL process to add new and updated prices into the "prices" mysql table
    * EXPERIMENTAL - for performance purposes saved the selected mysql dataframe into Parquet
    * Right-join the selected mysql table with the given CSV dataframe
    * Append the new prices into the "prices" mysql fact table
    *
    * @param pricesDf the selected mysql prices dataframe - as mentioned previously created the dataframe based on
    *                 Parquet
    * @param csvDf the parsed CSV dataframe
    */
  def loadUpdatedOrNewPricesForItems(pricesDf: DataFrame, csvDf: DataFrame): Unit = {
    logger.debug("starting to load new prices into the prices table")
    val sortedCsvDf = csvDf.filter(csvDf("item_code").isNotNull)
    val csvPricesDf = sortedCsvDf.select(sortedCsvDf("chain_id"),sortedCsvDf("sub_chain_id"),sortedCsvDf("store_id"),sortedCsvDf("item_id"),sortedCsvDf("price_update_date"),sortedCsvDf("qty"),sortedCsvDf("item_code"),sortedCsvDf("item_price")).orderBy(sortedCsvDf("chain_id"),sortedCsvDf("store_id"),sortedCsvDf("item_code"),sortedCsvDf("price_update_date"))
    val joinedStreams = pricesDf.join(csvPricesDf, pricesDf("chain_id")===csvPricesDf("chain_id") &&
      pricesDf("store_id")===csvPricesDf("store_id") && pricesDf("item_code")===csvPricesDf("item_code") && pricesDf("price_update_date")===csvPricesDf("price_update_date"), "right_outer").filter(pricesDf("item_code").isNull).select(csvPricesDf("chain_id"),csvPricesDf("sub_chain_id"),csvPricesDf("store_id"),csvPricesDf("item_id"),csvPricesDf("item_price"),csvPricesDf("qty"),csvPricesDf("item_code"),csvPricesDf("price_update_date"))
    val rowsToAdd = joinedStreams.count()

    logger.debug("successfully joined csv and prices db streams, count of new prices to be added: " + rowsToAdd)
    //we dont need to register a temp table here since the df here got to enter as-is without enhancement
    if(rowsToAdd>0) {
      mysqlBridge.writeToMysqlJdbc(joinedStreams,SaveMode.Append,"prices")
    }
    logger.debug("finished loading new prices into the prices table")
  }
}
