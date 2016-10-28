package org.mechiron.enhancer

import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.mechiron.utils.MySQLBridge

/**
  * Created by eran on 20/09/16.
  */
class ItemsEnhancer(sc: SparkContext) {
  val logger = LogManager.getLogger(getClass)

  def sparkFuncEnhance(tableName: String): Unit = {
    logger.debug("starting values enhancer... for tableName: " +tableName)
    val mysqlConnection = new MySQLBridge().connectJdbc()
    //auto commit to false so we will be able to perform btach updates efficiently
    mysqlConnection.setAutoCommit(false)

    val hiveContext = new HiveContext(sc)
    val df = hiveContext.sql("select item_code, " + tableName + " from (select item_code," + tableName + ", rank() over " +
      "(partition by item_code order by thenum desc) as therank from (select item_code, " + tableName + ", count(*) as thenum " +
      "from dwdata group by " + tableName + ", item_code) as b) as y where y.therank=1")

    val ps = mysqlConnection.prepareStatement("UPDATE items SET "+ tableName +"=? WHERE item_code=?")
    var index = 0
    val rowsArr = df.collect()
    logger.debug("successfully collected results from dwdata Hive store...")
    for(row <- rowsArr) {
      val itemCode = row.getString(0)
      val columnToEnhance = row.getString(1)
      ps.setString(1,columnToEnhance)
      ps.setString(2,itemCode)
      ps.addBatch()
      index+=1
      //execute 1000
      if(index%1000 == 0) {
        logger.debug("writing batch to mysql")
        ps.executeBatch()
        logger.debug("successfully wrote batch to mysql")
        index=0
      }
    }
    //execute we still have some left to execute and commit
    if(index>0) {
      logger.debug("we still got updates to perform - writing batch to mysql")
      ps.executeBatch()
      logger.debug("successfully wrote batch to mysql")
    }
    mysqlConnection.commit()
    logger.debug("done! committed batch results to mysql")
  }

}
