package org.mechiron.utils

import java.sql.{Connection, DriverManager}

/**
  * Created by eran on 10/09/16.
  */
class HiveConnector {

  def connect(): Connection = {
    val hiveUrl = ConfigFactory.getProperty("hive_jdbc_url")
    val hiveUser = ConfigFactory.getProperty("hive_user")
    val hivePass = ConfigFactory.getProperty("hive_pass")
    val connection = DriverManager.getConnection(hiveUrl.get, hiveUser.get, hivePass.get)
    println("Connected to Hive: " + connection.toString)
    connection
  }

}
