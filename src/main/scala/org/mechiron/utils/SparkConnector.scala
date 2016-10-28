package org.mechiron.utils

import org.apache.logging.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Initiate a SparkConext
  * EXPERIMENTAL - not tested using spark-submit
  *
  * @author Eran Levy
  */
class SparkConnector {
  val logger = LogManager.getLogger(getClass)

  def initSparkContext(): SparkContext = {
    logger.debug("trying to init spark context")
    //only for standalone mode - its possible to submit with --files
    val hiveMetaStoreUrl = ConfigFactory.getProperty("hive_metastore_url")
    val mysqlUser = ConfigFactory.getProperty("mysql_user")
    val mysqlPass = ConfigFactory.getProperty("mysql_pass")
    val sparkUrl = ConfigFactory.getProperty("spark_master")
    val hiveHome = ConfigFactory.getProperty("hive_home")
    val sparkHome = ConfigFactory.getProperty("spark_home")
    System.setProperty("javax.jdo.option.ConnectionURL",hiveMetaStoreUrl.get+"?createDatabaseIfNotExist=true")
    System.setProperty("javax.jdo.option.ConnectionDriverName","com.mysql.jdbc.Driver")
    System.setProperty("javax.jdo.option.ConnectionUserName",mysqlUser.get)
    System.setProperty("javax.jdo.option.ConnectionPassword",mysqlPass.get)
    //EXPERIMENTAL - not tested with spark-submit
    val sparkConf = new SparkConf().setAppName("MechironImporter").set("spark.executor.memory","1g")
      .set("spark.driver.memory","1g").setMaster(sparkUrl.get)
      .setJars(Seq(hiveHome.get+"/lib/hive-exec-1.2.1.jar",
        sparkHome.get+"/lib/mysql-connector-java-5.1.39-bin.jar",
        sparkHome.get+"/lib/com.databricks_spark-csv_2.10-1.4.0.jar",
        sparkHome.get+"/lib/org.apache.commons_commons-csv-1.1.jar",
        sparkHome.get+"/lib/com.univocity_univocity-parsers-1.5.1.jar"))
    logger.debug("successfully initiated spark context")
    new SparkContext(sparkConf)
  }
}
