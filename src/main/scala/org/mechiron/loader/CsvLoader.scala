package org.mechiron.loader

import java.io.File

import org.apache.hadoop.fs._
import org.apache.logging.log4j.LogManager
import org.mechiron.utils.{ConfigFactory, HdfsConnector, HiveConnector}


/**
  * Load downloaded CSV files into the "dwdata" Hive table
  * This is a big fact table meant to grow.
  * Currently used by the enhancer classes but later on can be used for additional purposes (ETL processes,
  * analysis, etc)
  * Iterate the mechiron_downloaded_data_location folders and for per each CSV:
  * 1. copy to HDFS location: mechiron_hdfs_data_folder
  * 2. load into the Hive table
  *
  * Files are encoded in UTF-8 so there shouldnt be any encoding issues with the Hebrew chars
  *
  * @author Eran Levy
  */
object CsvLoader {

  val logger = LogManager.getLogger(getClass)
  val hdfs =  new HdfsConnector().connect()
  val hiveConnection = new HiveConnector().connect()

  val folderName = ConfigFactory.getProperty("mechiron_downloaded_data_location")
  val defaultHdfsDataFolder = ConfigFactory.getProperty("mechiron_hdfs_data_folder")

  /**
    * Load the given local folder into HDFS
    * @param folderName - the folder to load
    */
  def loadFoldersToHdfs(folderName: String): Unit ={
    val folder = new File(folderName)
    logger.debug("starting to process download folder: " + folderName)
    //get all folder names to iterate and load into HDFS
    folder.listFiles().filter(_.isDirectory).foreach(moveLocalFolderToHdfs(_))
  }

  /**
    * Load CSV files into HIVE
    */
  def loadCsvFilesIntoHive(hdfsFileLocation: String): Unit ={
    logger.debug("Start loading file: " + hdfsFileLocation + " into Hive datastore")
    val stmt = hiveConnection.createStatement()
//      val query = "load data inpath " +  "'/user/eran/mechiron-data/2016-09-08_05_41_18/output/Keshet.csv'" +" into table dwdata"
      val query = "load data inpath " +  "'" + hdfsFileLocation +"'" +" into table dwdata"
      stmt.execute(query)
    logger.debug("Successfully finished loading file: " + hdfsFileLocation)
  }

  /**
    * Transfer a given file into HDFS
    * @param folder - the folder to move
    */
  def moveLocalFolderToHdfs(folder: File): Unit = {
    val folderName = folder.getName
    val outputFolder = new File(folder.getAbsolutePath + File.separatorChar + "output")
    val localArrPath = outputFolder.listFiles().filter(_.getName.indexOf("store") == -1).map(x => new Path(x.getAbsolutePath))
    logger.debug("starting to process files for output folder: " + outputFolder)
    //if local folder exists
    if(outputFolder.exists) {
      val hdfsOutputDataFolder = new Path(defaultHdfsDataFolder.get + File.separatorChar + folderName + File.separatorChar + "output")
      logger.debug("creating folder for move in hdfs: " + hdfsOutputDataFolder)
      hdfs.mkdirs(hdfsOutputDataFolder)
      logger.debug("Files to copy: ")
      localArrPath.foreach(path=>logger.debug(path.toString))
      hdfs.copyFromLocalFile(false,true, localArrPath, hdfsOutputDataFolder)
      logger.debug("Files transferred successfully for: " + outputFolder)

      //if files transferred successfully - load them into hive
      for(localFile <- localArrPath) {
        val hdfsFileLocation = hdfsOutputDataFolder.toString + File.separatorChar + localFile.getName
        loadCsvFilesIntoHive(hdfsFileLocation)
      }

    }
  }

  def main(args: Array[String]) {
    loadFoldersToHdfs(folderName.get)
  }
}
