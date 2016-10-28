package org.mechiron.utils

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Connect to MySQL and several utility functions
  *
  * @author Eran Levy
  */
class MySQLBridge {
  val logger = LogManager.getLogger(getClass)
  val mysqlUrl = ConfigFactory.getProperty("mysql_jdbc_url")
  val mysqlUser = ConfigFactory.getProperty("mysql_user")
  val mysqlPass = ConfigFactory.getProperty("mysql_pass")

  def connectJdbc(): Connection = {
    DriverManager.getConnection(mysqlUrl.get+"?useUnicode=true&characterEncoding=UTF-8",mysqlUser.get,mysqlPass.get)
  }

  def connect(sqlContext: SQLContext, tableName: String): DataFrame = {
    logger.debug("connecting to MySQL, table: " + tableName)
    val connProps = new Properties()
    connProps.setProperty("user", mysqlUser.get)
    connProps.setProperty("password", mysqlPass.get)
    sqlContext.read.jdbc(mysqlUrl.get,tableName,connProps)
  }

  def writeToMysqlJdbc(resultsetDf: DataFrame, saveMode: SaveMode, tableName: String): Boolean = {
    val connProps = new Properties()
    connProps.setProperty("user", mysqlUser.get)
    connProps.setProperty("password", mysqlPass.get)
    resultsetDf.write.mode(saveMode).jdbc(mysqlUrl.get, tableName, connProps)
    logger.debug("successfully persisted results to mysql table: " + tableName)
    true
  }

}
